"""Starts a real AuthGate (built from this repo) for tests that call the HTTP API.

Uses $DATABASE_URL (own schema), $REDIS_URL and $KAFKA_BOOTSTRAP, as docker-compose does.
If AUTHGATE_URL is set explicitly, that running service is used instead.
"""
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
_PORT = None


def pytest_configure(config):
    global _PORT
    if "AUTHGATE_URL" not in os.environ:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            _PORT = s.getsockname()[1]
        os.environ["AUTHGATE_URL"] = f"http://127.0.0.1:{_PORT}"


@pytest.fixture(scope="session", autouse=True)
def authgate_service(tmp_path_factory):
    if _PORT is None:
        yield
        return
    dsn = os.environ.get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/authgate?sslmode=disable")
    schema = "authgate_acceptance"
    subprocess.run(["psql", dsn, "-c", f"CREATE SCHEMA IF NOT EXISTS {schema}"], check=True, capture_output=True)
    sep = "&" if "?" in dsn else "?"
    binary = tmp_path_factory.mktemp("bin") / "authgate"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/authgate"], cwd=ROOT, check=True)
    env = {**os.environ, "DATABASE_URL": f"{dsn}{sep}search_path={schema}", "SEED_DEMO": "1",
           "LISTEN_ADDR": f"127.0.0.1:{_PORT}", "RUN_RELAY": "1", "RUN_CONSUMER": "1"}
    model = ROOT / "checkpoints" / "model.txt"
    if model.exists():
        env["MODEL_PATH"] = str(model)
    proc = subprocess.Popen([str(binary)], env=env, cwd=ROOT)
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{_PORT}/", timeout=1)
                break
            except urllib.error.HTTPError:
                break  # server is answering
            except Exception:
                if proc.poll() is not None:
                    raise RuntimeError("authgate exited during startup")
                time.sleep(0.3)
        yield
    finally:
        proc.terminate()
        proc.wait(timeout=10)
