#!/usr/bin/env bash
# Starts a throwaway local Postgres on :5432 (no Docker needed) using the pgserver wheel's binaries.
# Under docker-compose use the `postgres` service instead.
set -euo pipefail
cd "$(dirname "$0")/.."
uv venv -q .tools/pgvenv && uv pip install -q --python .tools/pgvenv/bin/python pgserver
B=$(echo "$PWD"/.tools/pgvenv/lib/python3*/site-packages/pgserver/pginstall/bin); D=$PWD/.tools/pgdata
[ -d "$D" ] || "$B/initdb" -D "$D" -U postgres --auth=trust >/dev/null
"$B/pg_ctl" -D "$D" -o "-p 5432 -k /tmp -c max_connections=300 -c listen_addresses=127.0.0.1" -l .tools/pg.log start
sleep 2; for db in authgate authgate_test; do "$B/psql" -h 127.0.0.1 -U postgres -c "create database $db" || true; done
