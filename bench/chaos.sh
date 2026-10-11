#!/usr/bin/env bash
# Chaos run: sends N (default 1,000,000) authorizations while repeatedly interrupting the broker and
# SIGKILLing the relay, then reconciles authorizations vs the consumer ledger -> results/chaos.json.
#
# Broker restarts: if BROKER_RESTART_CMD is set (e.g. "docker compose restart redpanda") it is run for
# each restart. Otherwise (no Docker available) the relay and consumer reach Redpanda through
# cmd/chaosproxy, which severs every connection and refuses new ones for OUTAGE_SECS ("proxy_outage").
# The mode used is recorded in the report as broker_restart_mode.
source "$(dirname "$0")/lib.sh"
N="${N:-1000000}"; INTERVAL="${INTERVAL:-20}"; OUTAGE_SECS="${OUTAGE_SECS:-6}"
export DATABASE_URL="${DATABASE_URL%%\?*}"
RUN="chaos$(date +%s)"; REPORT=results/chaos.json
build; fresh_db; go run ./bench/resetkafka

MODE="docker_restart"; DIAL=""
if [ -z "${BROKER_RESTART_CMD:-}" ]; then
  MODE="proxy_outage"
  bin/chaosproxy -listen 127.0.0.1:19092 -target "${KAFKA_BOOTSTRAP%%,*}" -control 127.0.0.1:19093 >"$LOGDIR/proxy.log" 2>&1 &
  PIDS+=($!); sleep 1; DIAL="127.0.0.1:19092"
fi
start api LISTEN_ADDR=:8080 SEED_ACCOUNTS=1000 REDIS_URL="$REDIS_URL" MODEL_PATH="${MODEL_PATH:-$ROOT/checkpoints/model.txt}"
wait_http http://127.0.0.1:8080/healthz
start_relay() { start relay LISTEN_ADDR=:8081 RUN_RELAY=1 KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" KAFKA_DIAL_ADDR="$DIAL"; }
start_relay
start consumer LISTEN_ADDR=:8082 RUN_CONSUMER=1 KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" KAFKA_DIAL_ADDR="$DIAL"

# Chaos loop (runs in this shell while the load generator runs in the background): every INTERVAL
# seconds alternate a broker restart, a relay SIGKILL, and a relay SIGKILL immediately followed by a
# broker restart.
restarts=0; kills=0
broker_restart() {
  if [ "$MODE" = proxy_outage ]; then
    curl -s -X POST 127.0.0.1:19093/down >/dev/null; sleep "$OUTAGE_SECS"; curl -s -X POST 127.0.0.1:19093/up >/dev/null
  else bash -c "$BROKER_RESTART_CMD"; fi
  restarts=$((restarts + 1))
}
relay_kill() { kill -9 "$(cat "$LOGDIR/relay.pid")" 2>/dev/null || true; kills=$((kills + 1)); sleep 1; start_relay; }

bin/bombard -n "$N" -workers 64 -accounts 1000 -run "$RUN" -out "$LOGDIR/bombard.json" &
BOMB=$!; PIDS+=($BOMB)
i=0
while kill -0 "$BOMB" 2>/dev/null; do
  for _ in $(seq "$INTERVAL"); do kill -0 "$BOMB" 2>/dev/null && sleep 1; done
  kill -0 "$BOMB" 2>/dev/null || break
  case $((i % 3)) in
    0) broker_restart ;;
    1) relay_kill ;;
    2) relay_kill; broker_restart ;;
  esac
  i=$((i + 1)); echo "chaos event $i: restarts=$restarts relay_kills=$kills"
done
wait "$BOMB"
[ "$MODE" = proxy_outage ] && curl -s -X POST 127.0.0.1:19093/up >/dev/null
python3 - "$MODE" "$restarts" "$kills" "$RUN" "$INTERVAL" "$OUTAGE_SECS" <<'PY'
import json, sys
mode, restarts, kills, run, interval, outage = sys.argv[1:]
b = json.load(open(".tools/logs/bombard.json"))
json.dump({"run_id": run, "broker_restart_mode": mode, "broker_restarts": int(restarts), "relay_kills": int(kills),
           "chaos_interval_secs": int(interval), "broker_outage_secs": int(outage),
           "client_sent": b["sent"], "client_ok": b["ok"], "client_failed": b["failed"],
           "client_seconds": round(b["seconds"], 1), "client_rps": round(b["rps"], 1)},
          open("results/chaos.json", "w"), indent=2)
PY
bin/reconcile -out "$REPORT" -prefix "$RUN-" -wait 10m
