# AuthGate: card transaction authorization in Go with a transactional outbox

> **Headline result:** _TBD (filled from results/ after milestone 5)_

## Architecture
_TBD (milestone 6): diagram of client -> AuthGate (Redis rules, in-process LightGBM, Postgres tx) -> outbox relay -> Redpanda -> deduplicating consumer._

## Latency
_TBD: table generated from `results/latency.md` (p50/p99 at 3+ RPS levels)._

## Chaos reconciliation
_TBD: table from `results/chaos.json` (1M authorizations, >=3 broker restarts, lost/duplicate counts)._

## Fraud model
_TBD: recall at 0.5/1/2% false-decline rate from `results/model_metrics.md` (time-ordered test split)._

## Why at-least-once + dedup is effectively exactly-once
_TBD (milestone 6)._

## Reproduce
```
uv sync
uv run pytest tests/test_acceptance.py -v
```
Every number in this README must come from files under `results/`.
