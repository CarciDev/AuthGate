#!/usr/bin/env bash
# Drives vegeta at increasing RPS against a locally started AuthGate (API only, relay/consumer
# running as separate processes) and writes results/latency.md + results/latency.json.
# Env: RPS_LEVELS (default "200 500 1000 2000 3000"), DURATION (default 30s), DATABASE_URL, REDIS_URL, KAFKA_BOOTSTRAP.
source "$(dirname "$0")/lib.sh"
RPS_LEVELS="${RPS_LEVELS:-200 500 1000 2000 3000}"; DURATION="${DURATION:-30}"
export DATABASE_URL="${DATABASE_URL%%\?*}"
build; fresh_db; go run ./bench/resetkafka
RUN="load$(date +%s)"
start api LISTEN_ADDR=:8080 SEED_ACCOUNTS=2000 REDIS_URL="$REDIS_URL" MODEL_PATH="${MODEL_PATH:-$ROOT/checkpoints/model.txt}"
wait_http http://127.0.0.1:8080/healthz
start relay LISTEN_ADDR=:8081 RUN_RELAY=1 KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP"
start consumer LISTEN_ADDR=:8082 RUN_CONSUMER=1 KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP"
mkdir -p .tools/load
for rps in $RPS_LEVELS; do
  python3 - "$rps" "$DURATION" "$RUN" >".tools/load/targets-$rps.json" <<'PY'
import base64, json, sys
rps, dur, run = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
for i in range(rps * dur):
    body = json.dumps({"account_id": f"acc-{i % 2000}", "card_id": f"{run}-{rps}-card-{i}",
                       "amount_cents": 100 + i % 900, "merchant": "m-1"})
    print(json.dumps({"method": "POST", "url": "http://127.0.0.1:8080/authorize",
                      "header": {"Idempotency-Key": [f"{run}-{rps}-{i}"], "Content-Type": ["application/json"]},
                      "body": base64.b64encode(body.encode()).decode()}))
PY
  vegeta attack -format=json -rate="$rps" -duration="${DURATION}s" -max-workers=2000 \
    <".tools/load/targets-$rps.json" | vegeta report -type=json >"results/.vegeta-$rps.json"
  rm ".tools/load/targets-$rps.json"
done
python3 - "$RPS_LEVELS" "$DURATION" <<'PY'
import json, sys
levels, dur = sys.argv[1].split(), sys.argv[2]
rows = []
for r in levels:
    d = json.load(open(f"results/.vegeta-{r}.json"))
    l = d["latencies"]
    rows.append({"target_rps": int(r), "achieved_rps": round(d["throughput"], 1), "p50_ms": round(l["50th"] / 1e6, 2),
                 "p99_ms": round(l["99th"] / 1e6, 2), "max_ms": round(l["max"] / 1e6, 1),
                 "success_pct": round(d["success"] * 100, 2), "requests": d["requests"]})
json.dump(rows, open("results/latency.json", "w"), indent=2)
md = [f"# AuthGate latency (vegeta, {dur}s per level, unique Idempotency-Key per request)", "",
      "Single machine; load generator, AuthGate, Postgres and Redis share the host. Rules + in-process model enabled.", "",
      "| target RPS | p50 ms | p99 ms | achieved RPS | max ms | success % |", "|---|---|---|---|---|---|"]
md += [f"| {r['target_rps']} | {r['p50_ms']} | {r['p99_ms']} | {r['achieved_rps']} | {r['max_ms']} | {r['success_pct']} |" for r in rows]
open("results/latency.md", "w").write("\n".join(md) + "\n")
PY
rm -f results/.vegeta-*.json
cat results/latency.md
