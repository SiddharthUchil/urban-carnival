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

from registry import SERIES, EVENT_IDS  # noqa: E402
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
