# WarehouseTaskBridge

WarehouseTaskBridge is the integration and middleware layer between a customer
Warehouse Management System (WMS) and an internal robotic fulfillment system. It
accepts work orders over NATS JetStream and REST/JSON, validates and translates
the external schema into a typed internal task model, deduplicates by
idempotency key, quarantines anything it cannot safely map, sequences work onto
a fleet of bots with a deterministic priority/zone/travel-cost scheduler, and
publishes task lifecycle transitions back to the WMS with ordered
at-least-once delivery. It is built for the messy parts of integration:
unknown messages, duplicate and conflicting orders, redelivery, schema drift,
and a peer that stops answering mid-exchange.

## Architecture

```
 Customer WMS ──POST /v1/orders──────────────┐
      │  publish wms.orders.>                │
      │  subscribe wms.status.>              v
      │                              ┌──────────────────┐
      │                              │ ingest           │
      │                              │ validate/translate│
      │                              │ idempotency      │
      │                              └────────┬─────────┘
      │                                       │
      │                        ┌──────────────v──────────────┐
      │                        │ PostgreSQL                  │
      │                        │ tasks · task_events(outbox) │
      │                        │ quarantine · peer_heartbeats│
      │                        │ bots                        │
      │                        └───────┬──────────────┬──────┘
      │                                │ PENDING      │ unpublished
      │                                v              v
      │                        ┌──────────────┐  ┌──────────────────┐
      │                        │ scheduler    │  │ feedback         │
      │                        │ cost + ties  │  │ ordered outbox   │
      │                        └──────┬───────┘  │ drainer          │
      │                               v          └────────┬─────────┘
      │                        ┌──────────────┐           │
      │                        │ simulated    │           │ wms.status.<order_ref>
      │                        │ bot fleet    │           v
      │                        │ state machine│    NATS JetStream (WMS_OUT)
      └────────────────────────┴──────────────┴───────────┘
```

The full diagram, including quarantine and heartbeat side paths, is in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

**Components**

- `internal/translate` — external-to-internal mapping, `A-12-3` slot parsing
  (aisle/bay/level plus derived zone), state mapping back to the WMS, and the
  `ReconciliationReport` that aggregates mapping warnings by code.
- `internal/ingest` — one shared acceptance path for JetStream and REST:
  schema-version gate, `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`,
  payload-hash conflict detection, and quarantine.
- `internal/store` — pgx pool, idempotent SQL migrations, and the transactional
  `task_events` outbox written in the same transaction as every state change.
- `internal/scheduler` — greedy assignment by `priority*1.0 +
  travel*0.5 + zonePenalty`, with documented deterministic tie-breaking
  (cost, then `created_at`, then task id; bot id for bot ties) and an explicit
  legal-transition table.
- `internal/feedback` — drains unpublished events in `seq` order, publishes to
  `wms.status.<order_ref>` with a JetStream publish ack, marks published only
  after the ack, and retries with exponential backoff.
- `internal/peer` — heartbeat exchange, silence detection after
  `PEER_SILENCE_TIMEOUT`, and ordered buffering while the peer is silent.
- `internal/sim` — simulated bots that derive handling time from the cost model
  and drive `ASSIGNED → RUNNING → DONE|FAILED`, requeueing failures.
- `cmd/fakewms` — a customer-side harness that generates reproducible ugly
  traffic and measures the result end to end.
- `internal/app`, `cmd/bridge` — dependency wiring and the service entrypoint.

### Schema mismatch: where the two models disagree

The external schema is intentionally flatter than the internal task. The
reconciliation report makes each gap explicit instead of losing it silently:

| External field | Internal behavior | Warning |
| --- | --- | --- |
| `client_meta` | No internal home; dropped | `DROPPED_CLIENT_META` |
| `due_by` present | No internal home; not persisted; scheduling uses priority/zone/travel | `DROPPED_DUE_BY` |
| `due_by` absent | Task still schedulable | `MISSING_DUE_BY` |
| unknown `kind` | Accepted as `TASK_TYPE_UNKNOWN`; never dropped while the slot is valid | `UNMAPPED_KIND` |
| unknown `priority_band` | Defaults to `STANDARD` (50) | `UNKNOWN_PRIORITY_BAND` |
| unsupported `schema_version` | Rejected at ingest and quarantined; direct translator use warns | `UNKNOWN_SCHEMA_VERSION` |
| unparseable location | Hard error; the order is quarantined, not routed | `UNPARSEABLE_SLOT` |
| `status` vocabulary | `ASSIGNED` and `RUNNING` both collapse to `IN_PROGRESS` | `LOSSY_STATE_COLLAPSE` (emitted by `ToExternal`, counted as `lossy_state_collapses`) |

`GET /v1/reconciliation` returns the aggregate plus a sample per code;
`GET /v1/reconciliation?format=markdown` renders the table.

## How to run

Prerequisites: Go 1.22+ language baseline (the checked-in `go.mod` and current
dependencies require Go 1.26+; this run used Go 1.27.1), Docker with Compose,
and optionally `protoc` + `protoc-gen-go` for `make proto`.

```bash
make up          # NATS JetStream (:4222, monitor :8222) and PostgreSQL (:55432)
make build       # bin/bridge and bin/fakewms
```

Start the bridge (migrations run automatically from `migrations/`):

```bash
export POSTGRES_DSN='postgres://bridge:bridge@127.0.0.1:55432/bridge?sslmode=disable'
export NATS_URL='nats://127.0.0.1:4222'
./bin/bridge
```

Defaults: `BRIDGE_HTTP_ADDR=:8080`, `HEARTBEAT_INTERVAL=2s`,
`PEER_SILENCE_TIMEOUT=6s`, `BOT_COUNT=6`, `SIM_SPEED_FACTOR=20`,
`SIM_FAILURE_RATE=0.05`, `SIM_SEED=42`. Invalid durations or out-of-range
values fail startup with a clear config error.

REST examples:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "order_ref": "ORD-9001",
    "kind": "pick",
    "priority_band": "EXPEDITE",
    "source_location": "A-12-3",
    "dest_location": "D-4-1",
    "qty": 3,
    "due_by": "2026-12-01T10:00:00Z",
    "client_meta": {"purchase_order": "PO-88121"},
    "schema_version": "2025-01-15",
    "idempotency_key": "idem-ord-9001"
  }'

curl -sS 'http://127.0.0.1:8080/v1/tasks?state=PENDING'
curl -sS http://127.0.0.1:8080/v1/tasks/<task-id>
curl -sS http://127.0.0.1:8080/v1/reconciliation
curl -sS 'http://127.0.0.1:8080/v1/reconciliation?format=markdown'
curl -sS http://127.0.0.1:8080/metrics
curl -sS http://127.0.0.1:8080/healthz
```

NATS examples (install the CLI with
`go install github.com/nats-io/natscli/nats@latest`):

```bash
nats --server nats://127.0.0.1:4222 pub wms.orders.orders \
  '{"order_ref":"ORD-9002","kind":"putaway","priority_band":"STANDARD","source_location":"B-2-1","dest_location":"N-8-2","qty":5,"schema_version":"2025-01-15","idempotency_key":"idem-ord-9002"}'

nats --server nats://127.0.0.1:4222 sub 'wms.status.>'
nats --server nats://127.0.0.1:4222 sub wms.quarantine
```

Reproducible demo (fixed seed, mixed transports, untrusted traffic, a peer
silence window, real metrics):

```bash
make demo
# writes docs/demo_run.txt and docs/results.json
make down
```

`make test-race` runs the unit suite; the integration suite is
`go test -race -tags=integration ./...` (Testcontainers starts disposable NATS
and PostgreSQL containers and skips cleanly when Docker is unavailable).

## Testing strategy

- **Table-driven unit tests** cover every translation branch, warning code,
  scheduler cost/tie-break case, and state-machine edge.
- **Golden files** in `internal/translate/testdata` pin external order →
  internal task output; refresh with
  `go test ./internal/translate/ -run TestGoldenTranslations -update`.
- **Fuzzing** (`FuzzParseSlot`) proves slot parsing never panics and that every
  successful parse survives a canonical round-trip. CI runs it for 30 seconds.
- **Testcontainers integration tests** (`test/integration`, build tag
  `integration`) exercise the real stack: 200-order happy path, duplicate storm
  across both transports, malformed/unknown-schema quarantine, forced
  redelivery, peer silence buffering/recovery, and mid-run crash recovery of
  the outbox. No live customer system is required.
- Everything runs under `-race`; tests use channels, polling deadlines, and
  JetStream acknowledgements rather than sleeps as synchronization.

## Results

These are measured on this machine from the reproducible demo
(seed `424242`, raw output in [docs/demo_run.txt](docs/demo_run.txt),
[docs/results.json](docs/results.json), [docs/bench.txt](docs/bench.txt), and
[docs/coverage.txt](docs/coverage.txt)). The demo uses 12 bots,
`SIM_SPEED_FACTOR=400`, `SIM_FAILURE_RATE=0.03`, heartbeats every 500 ms, a 3 s
silence timeout, and goes silent for 12 seconds starting at t=8s.

| Measurement | Result |
| --- | ---: |
| Orders sent (all messages) | 1,200 |
| Unique orders | 1,000 |
| Duplicates sent / deduped | 150 / 150 |
| Unique tasks created | 1,000 |
| Malformed messages sent / quarantined | 50 / 50 |
| Unmapped-kind warnings | 85 |
| Status updates delivered | 3,116 |
| Out-of-order deliveries | 0 |
| Events lost (published vs received) | 0 |
| End-to-end latency p50 / p95 / max | 2,691 / 10,944 / 12,117 ms |
| Scheduler (`BenchmarkAssign`, 10k tasks × 50 bots) | 2,212,344 ns/op, 222,464 B/op, 2,659 allocs/op |
| Scheduler throughput (50 assignments per op) | ≈22,600 tasks/s |
| Total tests (top-level functions / executed cases) | 44 / 96 |
| Line coverage (unit profile) | 33.5% |
| Demo wall-clock run | 22.77 s |

All 1,000 orders finished externally in `COMPLETE`; the 150 duplicate messages
produced exactly 1,000 tasks, and the 50 malformed payloads produced 50
quarantine rows without stopping later work.

**Reconciliation warning breakdown** (1,419 total):

| Warning code | Count |
| --- | ---: |
| `DROPPED_DUE_BY` | 750 |
| `DROPPED_CLIENT_META` | 334 |
| `MISSING_DUE_BY` | 250 |
| `UNMAPPED_KIND` | 85 |

**Peer-silence recovery:** 1 silence event; 100 events buffered; 100 drained;
0 gaps. The 3,116 published events equals the 3,116 unique status updates the
harness received, so no event was lost and no delivery was out of order.

The coverage figure is the required unit profile
(`go test -race -coverprofile=coverage.out ./...`); the Testcontainers suite and
the command entrypoints are outside that profile.

## Failure modes

The full detection/response/persistence/test matrix is in
[docs/TOUCHPOINTS.md](docs/TOUCHPOINTS.md). The five most interesting:

- **Conflicting duplicate** — a repeated idempotency key with a different
  payload hash is quarantined as `CONFLICTING_DUPLICATE`; the original task is
  kept and its id returned. Proven by `TestAcceptOrderConflictingDuplicate`.
- **Peer silence** — missing heartbeats mark the WMS SILENT, increment a
  counter, and pause publishing while events stay in the durable outbox and an
  ordered in-memory buffer; the first heartbeat drains both. Proven by
  `TestPeerSilenceBuffersAndDrains`.
- **Publish-ack failure** — an event is marked published only after a JetStream
  ack, so a failed ack leaves it unpublished and it is replayed with the same
  `StateSeq`. Proven by `TestPublisherAtLeastOnceDuplicateCarriesSameStateSeq`.
- **Redelivery after a DB outage** — transient store errors NAK with delay;
  the redelivered message goes through the same idempotency path, creating no
  second task and no duplicate `StateSeq`. Proven by
  `TestRedeliveryIsIdempotent`.
- **Unparseable slot** — an order with an invalid location is quarantined as a
  translation error rather than being routed to a nonexistent slot. Proven by
  `TestAcceptOrderQuarantinesBadInput/unparseable_slot`.

## Limitations and next steps

- Single bridge instance; there is no leader election or multi-instance
  partitioning of the ingest consumer in this build.
- Bots are simulated. The scheduler and state machine are real, but no robot
  protocol, safety interlock, or fleet telemetry adapter is included.
- No multi-tenant authentication/authorization on REST or NATS; run it inside a
  trusted network or add mTLS/JWT at the edge.
- Reconciliation counters and the silence buffer are in memory; they reset on
  restart (the durable truth for work and events remains in PostgreSQL).
- `due_by` is reported as dropped rather than persisted or used for scheduling.
- JetStream streams use 24-hour retention and a single NATS server in the local
  compose file; production needs replication, TLS, and backup/restore.
- Quarantine is inspectable but there is no automatic reconcile-and-replay tool.
- Coverage of HTTP/app wiring is low because the measured profile is unit-only;
  the integration suite is the behavioral proof.

## License

MIT — see [LICENSE](LICENSE).
