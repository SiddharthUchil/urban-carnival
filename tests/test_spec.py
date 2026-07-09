"""Loader normalization tests: clamps, window truncation, and structural invariants."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "synth"))
from spec import clamp_pct, load_spec  # noqa: E402


def test_clamp_pct_bounds():
    assert clamp_pct(100.817) == 100.0
    assert clamp_pct(-0.817) == 0.0
    assert clamp_pct(63.85) == 63.85


def test_window_truncated_to_date_max():
    s = load_spec()
    assert len(s.days) == 156                      # 2026-07-07 partial day dropped
    assert str(s.days[0][0]) == "2026-02-01"
    assert str(s.days[-1][0]) == "2026-07-06"


def test_total_rows_matches_volume():
    s = load_spec()
    assert s.total_rows == 1_142_361               # sum(ts_daily hits) == volume.total_rows


def test_schema_shape_and_pop_clamped():
    s = load_spec()
    assert len(s.columns) == 120
    assert all(0.0 <= c.pop <= 1.0 for c in s.columns)
    assert max(c.pop for c in s.columns) == 1.0    # 100.817 clamped to 1.0
    assert sum(c.sensitive for c in s.columns) == 16
    assert sum(bool(c.top_masked) for c in s.columns) == 25


def test_hour_probs_normalized():
    s = load_spec()
    assert len(s.hour_probs) == 7
    for row in s.hour_probs:
        assert len(row) == 24
        assert abs(sum(row) - 1.0) < 1e-9


def test_dims_are_valid_distributions():
    s = load_spec()
    assert len(s.dims) == 20
    for dim in s.dims.values():
        assert abs(sum(dim.probs) - 1.0) < 1e-9
        assert all(p >= 0.0 for p in dim.probs)
        assert len(dim.choices) == len(dim.probs)


def test_events_model():
    s = load_spec()
    assert len(s.events.ids) == 23
    assert all(0.0 <= p <= 1.0 for p in s.events.incl_prob.values())
    assert len(s.events.always_on) == 9           # nine ids fire on 100% of hits
    assert s.events.p50 == 16 and s.events.p95 == 18 and s.events.max == 22


def test_identity_and_clock_skew():
    s = load_spec()
    assert s.identity_cols["mcvisid"]["distinct"] == 51076
    assert s.identity_cols["mcvisid"]["len_max"] == 38
    assert s.hits_per_visit == 1.36
    assert s.clock_skew == [-18000, -14400, -14400]
