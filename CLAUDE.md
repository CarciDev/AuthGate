# AuthGate: Card Transaction Authorization Service in Go with a Transactional Outbox

AuthGate is a Go HTTP service that approves or declines card transactions. It uses Redis velocity rules and a LightGBM fraud model trained on FraudBench, scored in-process in pure Go. Each decision and its event are written in a single Postgres transaction, so a chaos test can show that Kafka receives every event despite broker restarts and kills of the relay: zero events lost, and duplicates are removed by event ID. Robinhood's backend work depends on the same three things: correctness, latency and idempotency.

## Goal and constraints
- Hardware: one NVIDIA RTX 4060 Ti (16 GB), 8 CPU cores, 24 GB RAM, inside Docker. No paid cloud.
- Total effort target: ~19 hours of work, split into the milestones below.
- Stack: Go (net/http, pgx, go-redis, franz-go), PostgreSQL, Redis, Redpanda (Kafka API, single node), LightGBM + scikit-learn (Python, offline training only), leaves (pure-Go LightGBM inference, so no cgo/ONNX), vegeta or k6 (load testing), Docker Compose, GitHub Actions (go test -race)
- Compute plan: Runs entirely on the local i5-12600K with 64 GB RAM under Docker Compose; no GPU is needed. LightGBM training on FraudBench takes a few minutes on CPU, the 1M-event chaos run takes about 10-20 minutes, and each load sweep takes about 5 minutes.

## Acceptance tests (the definition of done)
- `docker compose up` starts Postgres, Redis, Redpanda and AuthGate, and `curl POST /authorize` returns an approve or decline decision with a reason code
- `go test -race ./...` passes, including the concurrent double-spend test (no balance goes negative) and the idempotent-replay test (the same key returns the same decision, with one DB row)
- Go/Python score parity test passes on 200 golden rows (max abs diff < 1e-6)
- bench/chaos.sh completes 1M authorizations with at least 3 broker restarts and reports 0 lost events after reconciliation
- bench/load.sh writes results/latency.md with p50/p99 at three or more RPS levels
- results/model_metrics.md reports fraud recall at a fixed false-decline rate on the held-out, time-ordered test split

## Milestones
1. Fraud model offline (scope locked) (~3 h): train.py: loads FraudBench and makes a time-ordered train/test split. Trains LightGBM and saves model.txt plus a threshold table (recall at 0.5/1/2% false-decline rate) to results/model_metrics.md. Also exports 200 test rows with their Python scores as golden_scores.json for the Go parity test.
2. Go authorization API + Postgres transaction (~4 h): POST /authorize with an Idempotency-Key header. Schema: accounts, authorizations, outbox. A single pgx transaction checks and places the balance hold (SELECT ... FOR UPDATE), inserts the decision and inserts the outbox row. Integration tests run against a dockerized Postgres and cover idempotent replays and concurrent double-spend attempts.
3. Rules + in-process scoring (~3 h): Redis velocity rules: per-card INCR+EXPIRE counters over 1 min and 1 h windows, plus an amount cap. Pure-Go LightGBM scoring through leaves. A parity test checks that Go and Python scores on golden_scores.json differ by less than 1e-6.
4. Outbox relay + deduplicating consumer (~4 h): A relay goroutine pool claims rows with SELECT ... FOR UPDATE SKIP LOCKED, publishes to Redpanda with idempotent producer settings and marks rows sent. A consumer deduplicates by event_id into a ledger table. Unit tests pass under go test -race.
5. Load + chaos harness (~3 h): bench/load.sh drives vegeta at increasing RPS and records p50/p99. bench/chaos.sh sends 1M authorizations while repeatedly restarting Redpanda and killing the relay, then reconciles the authorizations table against the consumer ledger and reports lost and duplicate counts.
6. Results README for a 30-second screen (~2 h): The README opens with a one-line headline result, then an architecture diagram, a latency table, a chaos reconciliation table and the fraud recall/false-decline table. A section explains why the outbox gives at-least-once delivery and how consumer deduplication makes it effectively exactly-once.

## Grounding (use these; do not invent other references)
Papers:
- Temporal Consistency Models for Financial Data Processing in Distributed Systems (2026): https://doi.org/10.22399/ijcesen.4967
- Automatically detecting and fixing concurrency bugs in go software systems (2021): https://doi.org/10.1145/3445814.3446756
- GoPV: Detecting Blocking Concurrency Bugs Related to Shared-Memory Synchronization in Go (2025): https://doi.org/10.1145/3728979
- A benchmark for the performance evaluation of centralized and distributed transaction processing systems (1990): https://doi.org/10.1109/FTDCS.1990.138368
Datasets:
- TristanYan/FraudBench: https://huggingface.co/datasets/TristanYan/FraudBench
Reference implementations:
- none

## Results policy
Every number that ends up in README.md or report.md must be computed by code in this repo and saved under
results/ (JSON/CSV). The candidate's CV will later quote these files, so they must be reproducible.
