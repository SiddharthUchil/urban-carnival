"""Parity test: the PySpark gold KPI build must match the pandas detector build exactly.

`databricks/src/gold_lib.build_kpis_spark` is a port of `detect/kpis.build_kpis`. If they
drift, the gold table would disagree with what the detector scores. This runs both on the
synthetic parquet and asserts equality across all 35 series (counts exact, rates within a
tiny float tolerance). Requires pyspark + a JVM; skipped if either is unavailable.

Fixtures are staged as parquet (Spark reads them JVM-side): the synthetic file's pandas
nanosecond timestamps are downcast to microseconds so Spark 3.5 can read them, and no
DataFrame is built from local Python rows -- both avoid Windows-local PySpark quirks while
exercising the exact production code path.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "detect"))
sys.path.insert(0, str(REPO / "databricks" / "src"))

pytest.importorskip("pyspark")

from registry import SERIES, EVENT_IDS, SeriesSpec  # noqa: E402
from kpis import build_kpis  # noqa: E402
import gold_lib  # noqa: E402
import silver_lib as sl  # noqa: E402

INJECTED = REPO / "data" / "synth" / "injected.parquet"


@pytest.fixture(scope="module")
def spark():
    # Point Spark's Python at this interpreter and pin loopback -- the bare `python` on this
    # box is a Store stub and the default host resolves to a docker-internal name.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    from pyspark.sql import SparkSession

    s = (SparkSession.builder
         .master("local[2]")
         .appName("gold-parity")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _stage_micros(src: Path, cols, dst: Path):
    """Copy `cols` from `src` to `dst`, downcasting ns timestamps to us for Spark 3.5."""
    t = pq.read_table(src, columns=cols)
    arrays = []
    for name in t.schema.names:
        col = t[name]
        if pa.types.is_timestamp(col.type) and col.type.unit == "ns":
            col = col.cast(pa.timestamp("us"))
        arrays.append(col)
    pq.write_table(pa.table(arrays, names=t.schema.names), dst)


@pytest.mark.skipif(not INJECTED.exists(), reason="synthetic injected.parquet not present")
def test_gold_spark_matches_pandas(spark, tmp_path):
    """build_kpis_spark == build_kpis on the real synthetic dataset, all 35 series."""
    pdf = build_kpis(INJECTED).sort_values("process_date").reset_index(drop=True)

    staged = tmp_path / "injected_us.parquet"
    _stage_micros(INJECTED, gold_lib.NEEDED_COLS, staged)
    sdf = spark.read.parquet(str(staged))
    # collect() rather than toPandas() -- pyspark 3.5's toPandas imports distutils, gone in
    # Python 3.12. The production gold notebook writes Delta directly, never toPandas.
    rows = gold_lib.build_kpis_spark(sdf, EVENT_IDS, SERIES).collect()
    wide = (pd.DataFrame([r.asDict() for r in rows])
            .sort_values("process_date").reset_index(drop=True))

    assert len(pdf) == len(wide), f"row count differs: pandas={len(pdf)} spark={len(wide)}"
    p_dates = pd.to_datetime(pdf["process_date"]).dt.date.tolist()
    s_dates = pd.to_datetime(wide["process_date"]).dt.date.tolist()
    assert p_dates == s_dates, "calendar dates differ"

    metric_ids = [spec.metric_id for spec in SERIES]
    assert set(metric_ids).issubset(wide.columns), "spark output missing series columns"
    mism = {}
    for spec in SERIES:
        m = spec.metric_id
        a = pdf[m].to_numpy(dtype=float)
        b = wide[m].to_numpy(dtype=float)
        atol = 0.0 if spec.kind == "count" else 1e-9
        if not np.allclose(a, b, rtol=0.0, atol=atol, equal_nan=True):
            mism[m] = float(np.nanmax(np.abs(a - b)))
    assert not mism, f"series mismatch (metric -> max abs diff): {mism}"


def _ratio_fixture(dst: Path):
    """Two populated days with a deliberate calendar gap between them.

    The gap day is the point of the test: both builders fill it from the gap-free
    calendar, so visits_total is 0 there and the ratio must resolve to 0.0 rather
    than NaN (pandas 0/0) or inf (pandas n/0).
    """
    day1, day3 = pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-03")
    rows = [
        # (date, visid_high, visid_low, visit_num) -- day1: 3 hits over 2 visits,
        # day3: 2 hits over 1 visit.
        (day1, "h1", "l1", 1), (day1, "h1", "l1", 1), (day1, "h1", "l1", 2),
        (day3, "h2", "l2", 1), (day3, "h2", "l2", 1),
    ]
    tbl = pa.table({
        "process_date": pa.array([r[0] for r in rows], pa.timestamp("us")),
        "post_event_list": pa.array(["10036"] * len(rows)),
        "post_pagename": pa.array(["p"] * len(rows)),
        "language": pa.array(["45"] * len(rows)),
        "mcvisid": pa.array([f"{r[1]}{r[2]}" for r in rows]),
        "post_visid_high": pa.array([r[1] for r in rows]),
        "post_visid_low": pa.array([r[2] for r in rows]),
        "visit_num": pa.array([r[3] for r in rows], pa.int64()),
    })
    pq.write_table(tbl, dst)


def test_ratio_parity_and_zero_denominator(spark, tmp_path):
    """G2: kind=ratio builds identically in pandas and Spark, incl. a 0 denominator.

    Uses a local spec list rather than SERIES -- GWAM declares no ratio until doc 20 Q6
    settles the page-view basis, but the two builders must agree the moment one is added.
    """
    series = [
        SeriesSpec("hits_total", "count", "hits"),
        SeriesSpec("visits_total", "count", "visits"),
        # source is unused for kind=ratio; both builders resolve by sibling metric_id.
        SeriesSpec("pv_per_visit", "ratio", "visits",
                   numerator="hits_total", denominator="visits_total"),
    ]
    staged = tmp_path / "ratio_fixture.parquet"
    _ratio_fixture(staged)

    pdf = build_kpis(staged, series=series).sort_values("process_date").reset_index(drop=True)
    rows = gold_lib.build_kpis_spark(spark.read.parquet(str(staged)),
                                     EVENT_IDS, series).collect()
    wide = (pd.DataFrame([r.asDict() for r in rows])
            .sort_values("process_date").reset_index(drop=True))

    assert len(pdf) == len(wide) == 3, "gap-free calendar should yield 3 days"
    for m in ("hits_total", "visits_total", "pv_per_visit"):
        assert np.allclose(pdf[m].to_numpy(dtype=float), wide[m].to_numpy(dtype=float),
                           rtol=0.0, atol=1e-9), f"{m} differs between pandas and spark"

    # day1 = 3 hits / 2 visits, gap day = 0/0 -> 0.0, day3 = 2 hits / 1 visit.
    assert pdf["pv_per_visit"].tolist() == [1.5, 0.0, 2.0]
    assert np.isfinite(pdf["pv_per_visit"]).all(), "ratio produced NaN or inf"
    assert np.isfinite(wide["pv_per_visit"]).all(), "spark ratio produced NaN or inf"


def test_ratio_unknown_sibling_raises(tmp_path):
    """A ratio referencing an undeclared metric_id fails loudly in the pandas builder."""
    staged = tmp_path / "ratio_fixture.parquet"
    _ratio_fixture(staged)
    series = [
        SeriesSpec("hits_total", "count", "hits"),
        SeriesSpec("bad", "ratio", "visits", numerator="hits_total", denominator="nope"),
    ]
    with pytest.raises(ValueError, match="references unknown metric_ids"):
        build_kpis(staged, series=series)


def test_event_list_normalization(spark, tmp_path):
    """Plan D4: `id=value` tokens normalize to bare ids so event counts stay correct."""
    src = pa.table({"post_event_list": [
        "10036=1,20=1,500", "  10036 = 1 , 20 ", None, "10036,20",
    ]})
    staged = tmp_path / "evlist.parquet"
    pq.write_table(src, staged)

    out = (spark.read.parquet(str(staged))
           .withColumn("norm", sl.normalize_event_list_expr("post_event_list"))
           .collect())
    got = [r["norm"] for r in out]
    assert got == ["10036,20,500", "10036,20", None, "10036,20"]
