# Architecture

WarehouseTaskBridge is the integration and orchestration layer between a
customer Warehouse Management System (WMS) and an internal robotic fulfillment
system. The external contract is flat and string-heavy; the internal model is
typed, validated, and persisted.

## End-to-end flow

```
                    external WMS
        +--------------------------------+
        |  POST /v1/orders               |
        |  publish wms.orders.<kind>     |
        |  subscribe wms.status.>        |
        |  publish wms.heartbeat         |
        +---+------------------------+---+
            |                        ^
   REST/JSON|                        | wms.status.<order_ref>
            |                        | (JetStream pub-ack)
            v                        |
  +---------+----------+   +---------+-------------------+
  | ingest/http.go     |   | feedback/publisher.go       |
  | POST /v1/orders    |   | drains unpublished          |
  | GET /v1/tasks      |   | task_events in seq order    |
  | GET /v1/reconcile  |   | retries + pauses on silence |
  | GET /healthz       |   +---------+-------------------+
  | GET /metrics       |             ^
  +---------+----------+             |
            |                        |
  +---------v----------+   +---------+-------------------+
  | NATS JetStream     |   | PostgreSQL                   |
  | WMS_IN             |   |  tasks (idempotency_key UQ)  |
  | durable pull       |-->|  task_events (outbox)        |
  | bridge-ingest      |   |  quarantine                  |
  | explicit ack / NAK |   |  bots                        |
  +---------+----------+   |  peer_heartbeats             |
            |              +---------+-------------------+
            v                        ^
  +---------+----------+             |
  | ingest.Acceptor    |             |
  | decode + schema    |             |
  | gate               |             |
  | translate.ToInternal|            |
  | payload hash       |             |
  | ON CONFLICT DO     |             |
  | NOTHING            |             |
  +----+----------+----+             |
       |          |                  |
       |          +---- conflict ---> quarantine
       |                             |
       | reject/unsupported ---------+
       |
       v
  +----+-------------------------------+
  | scheduler.Assign                   |
  | priority + travel + zone cost      |
  | deterministic tie-break            |
  +----+-------------------------------+
       | PENDING -> ASSIGNED (tx)
       v
  +----+-------------------------------+
  | sim.Engine                         |
  | ASSIGNED -> RUNNING -> DONE/FAILED |
  | FAILED -> PENDING (requeue)        |
  +----+-------------------------------+
       | every transition writes
       v
  task_events outbox  ---- busy peer? ----> peer.Buffer (ordered, in memory)
                                           while task_events stay unpublished
```

## Components

- **config** loads environment configuration with defaults and validation.
- **wms** is the external schema (WorkOrder, OrderStatusUpdate).
- **translate** maps external to internal, parses `A-12-3` slots into
  aisle/bay/level/zone, collapses internal states to the external vocabulary,
  and records every mapping gap as a `Warning`.
- **ingest** provides the REST API and the JetStream pull consumer. Both share
  one idempotent acceptance path with quarantine for malformed input and
  conflicting duplicates.
- **store** owns plain-SQL persistence, migrations, the transactional state
  transition + outbox insert, and the unpublished-event query.
- **scheduler** is a deterministic greedy assignment solver with an explicit
  legal-transition table.
- **feedback** drains the outbox to `wms.status.<order_ref>` with JetStream
  publish acks, monotonic `StateSeq`, retries, and silence-aware buffering.
- **peer** tracks heartbeat liveness and supplies the ordered recovery buffer.
- **sim** simulates bots taking assignments and driving state transitions.
- **app** wires everything into one runnable service; `cmd/bridge` is a thin
  entrypoint and `cmd/fakewms` is the customer-side harness.
- **metrics** backs the plain-text `/metrics` endpoint.

## Side paths

**Quarantine.** Unsupported schema versions, malformed JSON, unparseable slots,
and conflicting idempotency keys are inserted into the `quarantine` table and
mirrored to the `wms.quarantine` NATS subject with a reason header. NATS
messages are acknowledged after quarantine so they cannot poison the consumer.

**Heartbeat and peer silence.** The bridge publishes `bridge.heartbeat` and
records inbound `wms.heartbeat` in `peer_heartbeats`. A watcher marks the peer
SILENT after `PEER_SILENCE_TIMEOUT`. While silent, the feedback publisher
buffers unpublished events in seq order instead of dropping or publishing them.
The first heartbeat restores liveness and the publisher drains the buffer, then
continues from the durable outbox.
