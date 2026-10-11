#!/usr/bin/env bash
# Shared helpers for load.sh and chaos.sh. Needs DATABASE_URL, REDIS_URL, KAFKA_BOOTSTRAP in the
# environment (same variables docker-compose uses) and `go` on PATH (or in .tools/go/bin).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
[ -x "$ROOT/.tools/go/bin/go" ] && export PATH="$ROOT/.tools/go/bin:$ROOT/.tools/bin:$PATH"
: "${DATABASE_URL:?set DATABASE_URL (e.g. postgres://postgres:postgres@localhost:5432/authgate_bench)}"
: "${REDIS_URL:?}" "${KAFKA_BOOTSTRAP:?}"
PIDS=()
LOGDIR="$ROOT/.tools/logs"; mkdir -p "$LOGDIR" bin results

build() { go build -o bin/ ./cmd/authgate ./cmd/bombard ./cmd/chaosproxy ./cmd/reconcile; }

# fresh_db: recreate the database named in DATABASE_URL so every run starts empty.
fresh_db() {
  local admin="${DATABASE_URL%/*}/postgres" name="${DATABASE_URL##*/}"; name="${name%%\?*}"
  go run ./bench/sqlexec "$admin" "DROP DATABASE IF EXISTS $name WITH (FORCE)" "CREATE DATABASE $name"
}

# start NAME [ENV=VAL ...]: start an authgate process; its PID goes to $LOGDIR/NAME.pid.
start() {
  local name="$1"; shift
  env "$@" bin/authgate >"$LOGDIR/$name.log" 2>&1 &
  echo $! >"$LOGDIR/$name.pid"; PIDS+=($!)
}
stop() { local f="$LOGDIR/$1.pid"; [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true; }
cleanup() { for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
wait_http() { for _ in $(seq 100); do curl -sf "$1" >/dev/null && return 0; sleep 0.2; done; echo "timeout waiting for $1" >&2; return 1; }
