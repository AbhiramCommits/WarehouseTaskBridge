#!/usr/bin/env bash
# Reproducible end-to-end demonstration of WarehouseTaskBridge.
#
# It brings up fresh NATS JetStream + PostgreSQL containers, starts the bridge,
# runs the fake-WMS harness with a fixed seed and a realistic ugly-traffic mix,
# and records the measured output in docs/.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p docs

export POSTGRES_DSN="${POSTGRES_DSN:-postgres://bridge:bridge@127.0.0.1:55432/bridge?sslmode=disable}"
export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
export BRIDGE_HTTP_ADDR="${BRIDGE_HTTP_ADDR:-:8080}"

# Demo-tuned bridge: fast simulated bots, a small real failure rate, and a
# peer-silence timeout short enough to exercise buffering during the run.
export BOT_COUNT=12
export SIM_SPEED_FACTOR=400
export SIM_FAILURE_RATE=0.03
export SIM_SEED=424242
export HEARTBEAT_INTERVAL=500ms
export PEER_SILENCE_TIMEOUT=3s

BRIDGE_PID=""
cleanup() {
  if [[ -n "$BRIDGE_PID" ]] && kill -0 "$BRIDGE_PID" 2>/dev/null; then
    kill "$BRIDGE_PID" 2>/dev/null || true
    wait "$BRIDGE_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

make build
docker compose down -v >/dev/null 2>&1 || true
make up

./bin/bridge > docs/bridge_run.log 2>&1 &
BRIDGE_PID=$!

for _ in $(seq 1 150); do
  if curl -sf "http://127.0.0.1:8080/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

if ! curl -sf "http://127.0.0.1:8080/healthz" >/dev/null; then
  echo "bridge did not become healthy; see docs/bridge_run.log" >&2
  exit 1
fi

# The bridge has applied migrations and created the simulated fleet. Clear any
# residual work so this run's counters are exact, keeping the bots.
docker compose exec -T postgres psql -U bridge -d bridge -q \
  -c 'TRUNCATE task_events, tasks, quarantine, peer_heartbeats RESTART IDENTITY CASCADE;'

{
  echo "# WarehouseTaskBridge reproducible demo"
  echo "# date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# seed: 424242, orders: 1000, dup-rate: 0.15, malformed-rate: 0.05, unmapped-kind-rate: 0.08"
  echo "# transport: mixed, go-silent-after: 8s, silent-for: 12s"
  echo
  ./bin/fakewms \
    -orders 1000 \
    -dup-rate 0.15 \
    -malformed-rate 0.05 \
    -unmapped-kind-rate 0.08 \
    -go-silent-after 8s \
    -silent-for 12s \
    -seed 424242 \
    -transport mixed \
    -send-interval 20ms \
    -timeout 90s \
    -results-out docs/results.json
} 2>&1 | tee docs/demo_run.txt

echo "demo complete: docs/demo_run.txt and docs/results.json"
