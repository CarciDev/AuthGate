# AuthGate: card transaction authorization in Go with a transactional outbox

> **Headline result:** 1,000,000 authorizations under 9 broker outages and 8 relay kills: **0 lost events, 0 duplicates in the ledger**; p50 2.01 ms / p99 38.81 ms at 1000 RPS (p50 2.84 ms at 3000 RPS).

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
| 200 | 2.4 | 6.53 | 200.0 | 100 |
| 500 | 2.2 | 64.34 | 500.0 | 100 |
| 1000 | 2.01 | 38.81 | 999.9 | 100 |
| 2000 | 2.39 | 87.87 | 1999.9 | 100 |
| 3000 | 2.84 | 153.86 | 2992.6 | 100 |

p99 grows with load because everything shares one host; p50 stays flat.

## Chaos reconciliation
Source: `results/chaos.json`, produced by `bench/chaos.sh`. Reconciliation compares the `authorizations`
table with the consumer ledger.

| metric | value |
|---|---|
| authorizations sent / ok / failed | 1,000,000 / 1,000,000 / 0 |
| broker outages (mode) | 9 (proxy_outage, 6 s each) |
| relay SIGKILLs | 8 |
| outbox rows / still unsent | 1,000,000 / 0 |
| consumer ledger rows | 1,000,000 |
| **lost events** | **0** |
| redelivered by broker/relay (absorbed by dedup) | 145 |
| **duplicates in ledger** | **0** |
| negative balances | 0 |
| client throughput | 3859.1 RPS over 259.1 s |

Caveat: Docker is unavailable in the build sandbox, so "broker restarts" are outages injected by
`cmd/chaosproxy` (all broker connections severed, new ones refused for 6 s), not
process restarts of Redpanda. With Docker use `BROKER_RESTART_CMD="docker compose restart redpanda"`.

## Fraud model
LightGBM, time-ordered split (first 353 claims train, last 236 test:
126 fraud / 110 legit), test AUC 0.7766
(source: `results/model_metrics.md`).

| false-decline rate | recall | achieved false-decline rate | threshold |
|---|---|---|---|
| 0.5% | 0.1270 | 0.0000 | 0.9722 |
| 1% | 0.1349 | 0.0091 | 0.9685 |
| 2% | 0.1746 | 0.0182 | 0.9619 |

Caveat: FraudBench is a small claim-fraud proxy dataset (589 rows), not real card data, and the legit
test set has only 110 rows, so recall is low and the 0.5% row means zero false declines.
The model demonstrates the serving path (Go/Python parity < 1e-6 on 200 golden rows), not production accuracy.

## Why at-least-once + dedup is effectively exactly-once
- **At-least-once.** The decision and its outbox row are inserted in the *same* Postgres transaction, so
  an event exists if and only if the decision committed; nothing is published from the request path, so a
  broker outage cannot lose or invent an event. The relay claims unsent rows (`SKIP LOCKED`), publishes,
  and marks them sent only after the broker acknowledges. If the relay is killed or the broker drops
  between publish and mark, the row stays unsent and is published again. A row is therefore never
  dropped, but may be delivered more than once (here: 145 redeliveries).
- **Dedup.** Every event carries a unique `event_id`. The consumer writes to a ledger with a unique key on
  `event_id` (`ON CONFLICT DO NOTHING`), so redeliveries are absorbed
  (145 absorbed) and the ledger holds each event once
  (0 duplicates). At-least-once delivery plus idempotent consumption gives
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
