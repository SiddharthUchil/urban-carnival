"""End-to-end generator tests: a small run must pass fidelity and be reproducible.

Fast checks generate a ~12-day subset into tmp. The full 156-day fidelity suite runs
only if data/synth/clean.parquet already exists (it is gitignored), so a fresh clone
still passes on the fast path.
"""
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "synth"))
import generate  # noqa: E402
import verify  # noqa: E402
from spec import load_spec  # noqa: E402


@pytest.fixture(scope="module")
def small(tmp_path_factory):
    out = tmp_path_factory.mktemp("synth_small")
    generate.main(["--seed", "1", "--limit-days", "12", "--out", str(out)])
    return out


def test_small_run_has_no_fidelity_failures(small):
    results = verify.run(small / "clean.parquet", load_spec())
    failures = [str(r) for r in results if r.status == "FAIL"]
    assert not failures, f"fidelity failures on subset: {failures}"


def test_reproducible_same_seed(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate.main(["--seed", "5", "--limit-days", "4", "--out", str(a)])
    generate.main(["--seed", "5", "--limit-days", "4", "--out", str(b)])
    ta = pq.read_table(a / "clean.parquet")
    tb = pq.read_table(b / "clean.parquet")
    assert ta.equals(tb), "same seed produced different data"


def test_full_dataset_passes_if_present():
    clean = ROOT / "data" / "synth" / "clean.parquet"
    if not clean.exists():
        pytest.skip("full clean.parquet not generated")
    results = verify.run(clean, load_spec())
    failures = [str(r) for r in results if r.status == "FAIL"]
    assert not failures, f"full-dataset fidelity failures: {failures}"
