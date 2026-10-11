"""Acceptance tests for AuthGate (the project's definition of done).

Checks that need artifacts from a later milestone are skipped with an explicit
reason until those artifacts exist; once they exist the checks are enforced.
"""
import json
import os
import re
import shutil
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"
GOLDEN = RESULTS / "golden_scores.json"
MODEL = ROOT / "checkpoints" / "model.txt"
AUTHGATE_URL = os.environ.get("AUTHGATE_URL", "http://localhost:8080")


def need(path: Path, milestone: str):
    """Skip the calling test unless `path` exists (produced by `milestone`)."""
    if not path.exists():
        pytest.skip(f"{path.relative_to(ROOT)} not produced yet ({milestone})")


def go_test(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["go", "test", *args], cwd=ROOT, capture_output=True, text=True, timeout=1800)


def need_go(milestone: str):
    if not (ROOT / "go.mod").exists():
        pytest.skip(f"go.mod missing ({milestone})")
    if shutil.which("go") is None:
        pytest.skip("go toolchain not installed")


# 1. docker compose up + POST /authorize -------------------------------------

def test_compose_defines_required_services():
    compose = ROOT / "docker-compose.yml"
    need(compose, "milestone 2")
    text = compose.read_text()
    for svc in ("postgres", "redis", "redpanda", "authgate"):
        assert re.search(rf"^\s+{svc}:\s*$", text, re.M), f"service {svc} missing"


def test_authorize_returns_decision_with_reason_code():
    try:
        req = urllib.request.Request(
            f"{AUTHGATE_URL}/authorize",
            data=json.dumps({"account_id": "acc-1", "card_id": "card-1", "amount_cents": 1000,
                             "merchant": "m-1"}).encode(),
            headers={"Content-Type": "application/json", "Idempotency-Key": "acc-test-1"},
            method="POST",
        )
        body = json.load(urllib.request.urlopen(req, timeout=5))
    except (urllib.error.URLError, ConnectionError, TimeoutError):
        pytest.skip(f"AuthGate not running at {AUTHGATE_URL} (start with `docker compose up`)")
    assert body["decision"] in ("approve", "decline")
    assert body.get("reason_code"), "reason code missing"


# 2. go test -race, double-spend and idempotent replay -----------------------

def test_go_race_suite_passes():
    need_go("milestone 2")
    r = go_test("-race", "-count=1", "./...")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]


@pytest.mark.parametrize("pattern", ["DoubleSpend", "IdempotentReplay"])
def test_required_go_tests_exist_and_run(pattern):
    need_go("milestone 2")
    r = go_test("-race", "-count=1", "-v", "-run", pattern, "./...")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    assert re.search(rf"--- PASS: Test\w*{pattern}", r.stdout), f"no passing test matching {pattern}"
    assert not re.search(rf"--- SKIP: Test\w*{pattern}", r.stdout), "required test was skipped"


# 3. Go/Python score parity on 200 golden rows -------------------------------

def _golden():
    need(GOLDEN, "milestone 1")
    data = json.loads(GOLDEN.read_text())
    return data["rows"] if isinstance(data, dict) else data


def test_golden_scores_has_200_rows():
    rows = _golden()
    assert len(rows) == 200
    assert all("features" in r and "score" in r for r in rows)


def test_python_scores_reproduce_from_model_file():
    rows = _golden()
    need(MODEL, "milestone 1")
    import lightgbm as lgb
    import numpy as np

    booster = lgb.Booster(model_file=str(MODEL))
    X = np.array([r["features"] for r in rows], dtype=float)
    expected = np.array([r["score"] for r in rows])
    assert np.max(np.abs(booster.predict(X) - expected)) < 1e-6


def test_go_scores_match_python_within_1e6():
    need(GOLDEN, "milestone 1")
    need_go("milestone 3")
    r = go_test("-count=1", "-v", "-run", "Parity", "./...")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    assert re.search(r"--- PASS: Test\w*Parity", r.stdout), "no passing parity test"
    assert not re.search(r"--- SKIP: Test\w*Parity", r.stdout)


# 4. chaos: 1M authorizations, >=3 broker restarts, 0 lost -------------------

def test_chaos_script_exists():
    script = ROOT / "bench" / "chaos.sh"
    need(script, "milestone 5")
    assert os.access(script, os.X_OK) or script.read_text().startswith("#!")


def test_chaos_report_zero_lost_events():
    report = RESULTS / "chaos.json"
    need(report, "milestone 5")
    d = json.loads(report.read_text())
    assert d["authorizations"] >= 1_000_000
    assert d["broker_restarts"] >= 3
    assert d["lost_events"] == 0
    assert d["duplicates_in_ledger"] == 0, "consumer must dedupe by event_id"
    assert "relay_kills" in d


# 5. load: results/latency.md with p50/p99 at >=3 RPS levels -----------------

def test_latency_md_has_three_rps_levels():
    md = RESULTS / "latency.md"
    need(md, "milestone 5")
    text = md.read_text()
    assert re.search(r"p50", text, re.I) and re.search(r"p99", text, re.I)
    rows = [l for l in text.splitlines() if re.match(r"\s*\|\s*\d+(\.\d+)?\s*\|", l)]
    assert len({l.split("|")[1].strip() for l in rows}) >= 3, "need >=3 distinct RPS rows"
    for l in rows:
        assert all(re.fullmatch(r"\d+(\.\d+)?", c.strip()) for c in l.strip().strip("|").split("|")[:3]), l


# 6. model metrics: recall at fixed false-decline rate, time-ordered split ---

def test_model_metrics_reports_recall_at_fixed_fdr():
    md = RESULTS / "model_metrics.md"
    need(md, "milestone 1")
    text = md.read_text().lower()
    assert "recall" in text
    for rate in ("0.5%", "1%", "2%"):
        assert rate in text, f"missing false-decline rate {rate}"
    assert "time" in text and "test" in text, "must state the time-ordered held-out split"
    values = re.findall(r"\|\s*(0?\.\d+|1\.0+)\s*\|", text)
    assert len(values) >= 3 and all(0 <= float(v) <= 1 for v in values)


def test_no_unbacked_numbers_dir_hygiene():
    """results/ holds only small JSON/CSV/MD files (repo is pushed to GitHub)."""
    if not RESULTS.exists():
        pytest.skip("results/ not created yet")
    for p in RESULTS.rglob("*"):
        if p.is_file():
            assert p.stat().st_size < 5_000_000, f"{p} too large"
