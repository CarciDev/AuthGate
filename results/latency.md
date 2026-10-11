# AuthGate latency (vegeta, 30s per level, unique Idempotency-Key per request)

Single machine; load generator, AuthGate, Postgres and Redis share the host. Rules + in-process model enabled.

| target RPS | p50 ms | p99 ms | achieved RPS | max ms | success % |
|---|---|---|---|---|---|
| 200 | 2.4 | 6.53 | 200.0 | 56.0 | 100 |
| 500 | 2.2 | 64.34 | 500.0 | 256.8 | 100 |
| 1000 | 2.01 | 38.81 | 999.9 | 79.2 | 100 |
| 2000 | 2.39 | 87.87 | 1999.9 | 142.0 | 100 |
| 3000 | 2.84 | 153.86 | 2992.6 | 204.4 | 100 |
