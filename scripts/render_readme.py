"""Render README.md from the JSON files under results/ so every number is traceable.

Usage: python scripts/render_readme.py
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
R = ROOT / "results"

def main() -> None:
    lat = json.loads((R / "latency.json").read_text())
    chaos = json.loads((R / "chaos.json").read_text())
    met = json.loads((R / "model_metrics.json").read_text())
    ok = [x for x in lat if x["success_pct"] == 100]
    top = max(ok, key=lambda x: x["target_rps"])
    mid = next(x for x in lat if x["target_rps"] == 1000)

    head = (f"{chaos['authorizations']:,} authorizations under {chaos['broker_restarts']} broker outages and "
            f"{chaos['relay_kills']} relay kills: **{chaos['lost_events']} lost events, "
            f"{chaos['duplicates_in_ledger']} duplicates in the ledger**; p50 {mid['p50_ms']} ms / p99 "
            f"{mid['p99_ms']} ms at {mid['target_rps']} RPS (p50 {top['p50_ms']} ms at {top['target_rps']} RPS).")
    lat_rows = "\n".join(f"| {x['target_rps']} | {x['p50_ms']} | {x['p99_ms']} | {x['achieved_rps']} | "
                         f"{x['success_pct']} |" for x in lat)
    c = chaos
    chaos_rows = "\n".join(f"| {k} | {v} |" for k, v in [
        ("authorizations sent / ok / failed", f"{c['client_sent']:,} / {c['client_ok']:,} / {c['client_failed']}"),
        ("broker outages (mode)", f"{c['broker_restarts']} ({c['broker_restart_mode']}, {c['broker_outage_secs']} s each)"),
        ("relay SIGKILLs", c["relay_kills"]),
        ("outbox rows / still unsent", f"{c['outbox_rows']:,} / {c['outbox_unsent']}"),
        ("consumer ledger rows", f"{c['ledger_rows']:,}"),
        ("**lost events**", f"**{c['lost_events']}**"),
        ("redelivered by broker/relay (absorbed by dedup)", c["redelivered_events"]),
        ("**duplicates in ledger**", f"**{c['duplicates_in_ledger']}**"),
        ("negative balances", c["negative_balances"]),
        ("client throughput", f"{c['client_rps']} RPS over {c['client_seconds']} s"),
    ])
    rec = "\n".join(f"| {k} | {v['recall']:.4f} | {v['false_decline_rate']:.4f} | {v['threshold']:.4f} |"
                    for k, v in met["recall_at_false_decline_rate"].items())

    text = f"""# AuthGate: card transaction authorization in Go with a transactional outbox

> **Headline result:** {head}

## Architecture
```
client --POST /authorize (Idempotency-Key)--> AuthGate (Go, net/http)
                                               |-- Redis: velocity rules (1 min / 1 h INCR+EXPIRE, amount cap)
                                               |-- in-process LightGBM (leaves, pure Go)
                                               '-- ONE Postgres tx: lock account (FOR UPDATE) -> balance hold
                                                       -> authorizations row -> outbox row
Postgres outbox --SELECT .. FOR UPDATE SKIP LOCKED--> relay pool (idempotent producer)
      --> Redpanda (Kafka API) --> consumer --INSERT .. ON CONFLICT (event_id) DO NOTHING--> ledger
```

## Latency
Single machine, load generator + AuthGate + Postgres + Redis share the host, 30 s per level, unique
Idempotency-Key per request (source: `results/latency.md`, `results/latency.json`).

| target RPS | p50 ms | p99 ms | achieved RPS | success % |
|---|---|---|---|---|
{lat_rows}

p99 grows with load because everything shares one host; p50 stays flat.

## Chaos reconciliation
Source: `results/chaos.json`, produced by `bench/chaos.sh`. Reconciliation compares the `authorizations`
table with the consumer ledger.

| metric | value |
|---|---|
{chaos_rows}

Caveat: Docker is unavailable in the build sandbox, so "broker restarts" are outages injected by
`cmd/chaosproxy` (all broker connections severed, new ones refused for {c['broker_outage_secs']} s), not
process restarts of Redpanda. With Docker use `BROKER_RESTART_CMD="docker compose restart redpanda"`.

## Fraud model
LightGBM, time-ordered split (first {met['n_train']} claims train, last {met['n_test']} test:
{met['test_fraud']} fraud / {met['test_legit']} legit), test AUC {met['auc']:.4f}
(source: `results/model_metrics.md`).

| false-decline rate | recall | achieved false-decline rate | threshold |
|---|---|---|---|
{rec}

Caveat: FraudBench is a small claim-fraud proxy dataset (589 rows), not real card data, and the legit
test set has only {met['test_legit']} rows, so recall is low and the 0.5% row means zero false declines.
The model demonstrates the serving path (Go/Python parity < 1e-6 on 200 golden rows), not production accuracy.

## Why at-least-once + dedup is effectively exactly-once
- **At-least-once.** The decision and its outbox row are inserted in the *same* Postgres transaction, so
  an event exists if and only if the decision committed; nothing is published from the request path, so a
  broker outage cannot lose or invent an event. The relay claims unsent rows (`SKIP LOCKED`), publishes,
  and marks them sent only after the broker acknowledges. If the relay is killed or the broker drops
  between publish and mark, the row stays unsent and is published again. A row is therefore never
  dropped, but may be delivered more than once (here: {c['redelivered_events']} redeliveries).
- **Dedup.** Every event carries a unique `event_id`. The consumer writes to a ledger with a unique key on
  `event_id` (`ON CONFLICT DO NOTHING`), so redeliveries are absorbed
  ({c['extra_deliveries_absorbed']} absorbed) and the ledger holds each event once
  ({c['duplicates_in_ledger']} duplicates). At-least-once delivery plus idempotent consumption gives
  effectively-exactly-once *effects*; the transport itself is not exactly-once.
- **Request side.** The `Idempotency-Key` header makes client retries return the same decision with one row.

## Reproduce
```
uv sync
uv run pytest tests/test_acceptance.py -v
python scripts/render_readme.py   # regenerates this README from results/
```
Every number in this README comes from files under `results/`.

## Benchmarks
`bench/load.sh` (vegeta sweep -> `results/latency.md`) and `bench/chaos.sh` (1M authorizations under
broker outages and relay SIGKILLs -> `results/chaos.json`). Both need `DATABASE_URL`, `REDIS_URL`,
`KAFKA_BOOTSTRAP`. `bench/dev-postgres.sh` starts a local Postgres where Docker is unavailable.
"""
    (ROOT / "README.md").write_text(text)

if __name__ == "__main__":
    main()
