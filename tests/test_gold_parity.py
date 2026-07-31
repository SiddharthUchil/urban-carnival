"""Parity test: the PySpark gold KPI build must match the pandas detector build exactly.

`databricks/src/gold_lib.build_kpis_spark` is a port of `detect/kpis.build_kpis`. If they
drift, the gold table would disagree with what the detector scores. This runs both on the
synthetic parquet and asserts equality across all 42 series (counts exact, rates within a
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
    """build_kpis_spark == build_kpis on the real synthetic dataset, all 42 series."""
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


@pytest.mark.skipif(not INJECTED.exists(), reason="synthetic injected.parquet not present")
def test_synthetic_fixture_can_falsify_the_page_view_basis():
    """The fixture must contain non-page-view hits, or every basis test passes vacuously.

    synth/local_synth_profile.md once recorded post_page_event as "0" on 100% of rows. Under
    that fixture adobe_pv was arithmetically identical to all_hits and share_pv_eq_0 was
    pinned at exactly 0.0, so the basis code could have been entirely wrong and still green.
    synth/generate.PV_MIX exists to prevent that; this asserts it survived regeneration.
    """
    pdf_all = build_kpis(INJECTED, page_view_basis="all_hits")
    pdf_pv = build_kpis(INJECTED, page_view_basis="adobe_pv")

    assert (pdf_pv["page_views_total"] < pdf_all["page_views_total"]).any(), (
        "adobe_pv counted every hit -- post_page_event is degenerate again, so the basis "
        "tests cannot fail. Regenerate the fixture (synth/generate.py).")
    # Zero-page-view visits are the detectable form of the SME's "< 1" signal (doc 20 Part 4).
    assert pdf_pv["pv_bucket_share_0"].mean() > 0.01, (
        "no zero-page-view visits in the fixture; share_pv_eq_0 has nothing to detect")
    assert pdf_all["pv_bucket_share_0"].eq(0.0).all(), (
        "all_hits must have no zero-page-view visits by construction (every visit has >=1 hit)")


def _ratio_fixture(dst: Path):
    """Two populated days with a deliberate calendar gap between them.

    The gap day is the point of the test: both builders fill it from the gap-free
    calendar, so visits_total is 0 there and the ratio must resolve to 0.0 rather
    than NaN (pandas 0/0) or inf (pandas n/0).
    """
    day1, day3 = pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-03")
    rows = [
        # (date, visid_high, visid_low, visit_num, post_page_event) -- day1: 3 hits over 2
        # visits, day3: 2 hits over 1 visit.
        #
        # post_page_event carries a deliberate mix so the page-view basis is testable:
        # "0" is Adobe's page-view marker, "1" is a link-track hit, and "bogus" is
        # non-numeric on purpose -- it must try_cast to NULL and simply fail the == 0 test
        # in BOTH builders rather than raising (ANSI parity; the CoverMe E1 defect).
        (day1, "h1", "l1", 1, "0"), (day1, "h1", "l1", 1, "1"), (day1, "h1", "l1", 2, "0"),
        (day3, "h2", "l2", 1, "0"), (day3, "h2", "l2", 1, "bogus"),
    ]
    tbl = pa.table({
        "process_date": pa.array([r[0] for r in rows], pa.timestamp("us")),
        "post_event_list": pa.array(["10036"] * len(rows)),
        "post_pagename": pa.array(["p"] * len(rows)),
        "post_page_event": pa.array([r[4] for r in rows]),
        "language": pa.array(["45"] * len(rows)),
        "mcvisid": pa.array([f"{r[1]}{r[2]}" for r in rows]),
        "post_visid_high": pa.array([r[1] for r in rows]),
        "post_visid_low": pa.array([r[2] for r in rows]),
        "visit_num": pa.array([r[3] for r in rows], pa.int64()),
    })
    pq.write_table(tbl, dst)


def test_ratio_parity_and_zero_denominator(spark, tmp_path):
    """G2: kind=ratio builds identically in pandas and Spark, incl. a 0 denominator.

    Uses a local spec list rather than SERIES so the ratio machinery is exercised in
    isolation, on a fixture whose expected values can be read off by hand. The registry's
    own ratio (pv_per_visit) is covered by the basis tests below.
    """
    series = [
        SeriesSpec("hits_total", "count", "hits"),
        SeriesSpec("visits_total", "count", "visits"),
        # source is unused for kind=ratio; both builders resolve by sibling metric_id.
        SeriesSpec("hits_per_visit", "ratio", "visits",
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
    for m in ("hits_total", "visits_total", "hits_per_visit"):
        assert np.allclose(pdf[m].to_numpy(dtype=float), wide[m].to_numpy(dtype=float),
                           rtol=0.0, atol=1e-9), f"{m} differs between pandas and spark"

    # day1 = 3 hits / 2 visits, gap day = 0/0 -> 0.0, day3 = 2 hits / 1 visit.
    assert pdf["hits_per_visit"].tolist() == [1.5, 0.0, 2.0]
    assert np.isfinite(pdf["hits_per_visit"]).all(), "ratio produced NaN or inf"
    assert np.isfinite(wide["hits_per_visit"]).all(), "spark ratio produced NaN or inf"


# Registry order is (0, 1, 2, 3-5, 6+); the fixture only ever populates the first three.
_PVB_COLS = ["pv_bucket_share_0", "pv_bucket_share_1", "pv_bucket_share_2",
             "pv_bucket_share_3_5", "pv_bucket_share_6plus"]
_BASIS_SERIES = [
    SeriesSpec("hits_total", "count", "hits"),
    SeriesSpec("visits_total", "count", "visits"),
    SeriesSpec("page_views_total", "count", "page_views"),
    SeriesSpec("pv_per_visit", "ratio", "page_views",
               numerator="page_views_total", denominator="visits_total"),
] + [SeriesSpec(c, "point_mass", "visit_pv_bucket", dim_value=b)
     for c, b in zip(_PVB_COLS, ["0", "1", "2", "3-5", "6+"])]


def _basis_frames(spark, tmp_path, basis):
    """(pandas, spark) KPI frames for `basis` over the ratio fixture."""
    staged = tmp_path / f"basis_{basis}.parquet"
    _ratio_fixture(staged)
    pdf = (build_kpis(staged, series=_BASIS_SERIES, page_view_basis=basis)
           .sort_values("process_date").reset_index(drop=True))
    rows = gold_lib.build_kpis_spark(spark.read.parquet(str(staged)), EVENT_IDS,
                                     _BASIS_SERIES, page_view_basis=basis).collect()
    wide = (pd.DataFrame([r.asDict() for r in rows])
            .sort_values("process_date").reset_index(drop=True))
    return pdf, wide


@pytest.mark.parametrize("basis", ["all_hits", "adobe_pv"])
def test_page_view_basis_parity(spark, tmp_path, basis):
    """doc 20 Q6: page_views/pv_per_visit/bucket shares agree in pandas and Spark, both bases."""
    pdf, wide = _basis_frames(spark, tmp_path, basis)
    for m in [s.metric_id for s in _BASIS_SERIES]:
        assert np.allclose(pdf[m].to_numpy(dtype=float), wide[m].to_numpy(dtype=float),
                           rtol=0.0, atol=1e-9), f"{m} differs between pandas and spark"
    # The five shares partition the visits, so they must sum to exactly 1 on populated days
    # and to 0 on the gap day (no visits to divide).
    tot = pdf[_PVB_COLS].sum(axis=1).to_numpy()
    assert np.allclose(tot, [1.0, 0.0, 1.0], atol=1e-12), f"bucket shares do not partition: {tot}"


def test_page_view_basis_changes_the_numbers(spark, tmp_path):
    """The two bases must actually disagree -- otherwise every basis test above is vacuous.

    This is the guard for the trap that the synthetic profile used to set post_page_event
    to "0" on 100% of rows, which made adobe_pv indistinguishable from all_hits.
    """
    allh, _ = _basis_frames(spark, tmp_path, "all_hits")
    adobe, _ = _basis_frames(spark, tmp_path, "adobe_pv")

    # all_hits counts every hit; the fixture's 5 hits span day1=3, gap=0, day3=2.
    assert allh["page_views_total"].tolist() == [3.0, 0.0, 2.0]
    assert allh["page_views_total"].equals(allh["hits_total"]), "all_hits must equal hits_total"
    # adobe_pv drops the "1" link-track hit on day1 and the non-numeric "bogus" on day3.
    # "bogus" try_casts to NULL rather than raising -- that it counts as 1 here, not 2, IS
    # the ANSI-parity assertion.
    assert adobe["page_views_total"].tolist() == [2.0, 0.0, 1.0]

    # day1 has 2 visits (2pv + 1pv under all_hits -> buckets 2 and 1;
    # 1pv + 1pv under adobe_pv -> both in bucket 1).
    assert allh["pv_bucket_share_1"].tolist()[0] == 0.5
    assert allh["pv_bucket_share_2"].tolist()[0] == 0.5
    assert adobe["pv_bucket_share_1"].tolist()[0] == 1.0
    assert adobe["pv_bucket_share_2"].tolist()[0] == 0.0


def test_unknown_page_view_basis_raises(spark, tmp_path):
    """A typo'd basis fails loudly in both builders rather than silently counting all hits."""
    staged = tmp_path / "ratio_fixture.parquet"
    _ratio_fixture(staged)
    with pytest.raises(ValueError, match="unknown page_view_basis"):
        build_kpis(staged, series=_BASIS_SERIES, page_view_basis="adobe")
    with pytest.raises(ValueError, match="unknown page_view_basis"):
        gold_lib.build_kpis_spark(spark.read.parquet(str(staged)), EVENT_IDS,
                                  _BASIS_SERIES, page_view_basis="adobe")


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
