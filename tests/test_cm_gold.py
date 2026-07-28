"""CoverMe gold KPI build: the gold_lib extensions behind cm_registry.

Exercises the parameterized build (date_col, visit/visitor keys, visit-distinct event
basis) and the new series kinds (ratio, event_dim cube) on a tiny hand-computed dataset.
GWAM behavior is regression-guarded separately by tests/test_gold_parity.py, which must
keep passing unchanged.

Fixture staging mirrors test_gold_parity: data goes through parquet so Spark reads it
JVM-side (no local-row createDataFrame -- Windows-local PySpark quirk).
"""
from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "detect"))
sys.path.insert(0, str(REPO / "databricks" / "src"))

pytest.importorskip("pyspark")
pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

from cm_registry import (  # noqa: E402
    SERIES, EVENT_IDS, NEEDED_COLS, DATE_COL, VISIT_KEY_COLS, VISITOR_KEY_COLS, EVENT_BASIS,
    NULL_SAFE_KEYS,
)
import gold_lib  # noqa: E402

D1, D2, D3 = dt.date(2026, 7, 1), dt.date(2026, 7, 2), dt.date(2026, 7, 3)


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    from pyspark.sql import SparkSession

    s = (SparkSession.builder
         .master("local[2]")
         .appName("cm-gold")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _hit(d, events, evar4, lang, high, low, vnum, vstart):
    return {"hit_date": d, "post_event_list": events, "post_evar4": evar4,
            "language": lang, "post_visid_high": high, "post_visid_low": low,
            "visit_num": vnum, "visit_start_time_gmt": vstart}


# Hand-computed scenario. Day D1:
#   V1 = (h1,l1,1,t1) visitor A: 228 fires on BOTH hits (visit-distinct must count 1),
#        plus 229 and 103; evar4=hd, language=en.
#   V2 = (h1,l1,1,t2): same visid pair AND visit_num as V1 -- only visit_start_time_gmt
#        differs (visit_num reuse; Adobe 4-part key must count it as a second visit).
#        228 with an off-list evar4 -> cube "other" bucket; language=fr.
#   V3 = (h2,l2,5,t3) visitor B: 269+240 with NO quote events (saved-quote resume) and
#        NULL evar4 -> "other" bucket.
# Day D2: no rows (calendar gap -> zero-filled).
# Day D3: one eventless hit (NULL post_event_list), language=unknown.
ROWS = [
    _hit(D1, "228,103", "hd", "en", "h1", "l1", "1", "t1"),
    _hit(D1, "228,229", "hd", "en", "h1", "l1", "1", "t1"),
    _hit(D1, "228", "obscure-cat", "fr", "h1", "l1", "1", "t2"),
    _hit(D1, "269,240", None, "en", "h2", "l2", "5", "t3"),
    _hit(D3, None, "travel", "unknown", "h3", "l3", "1", "t4"),
]

EXPECTED = {
    "hits_total": {D1: 4, D2: 0, D3: 1},
    "visits_total": {D1: 3, D2: 0, D3: 1},
    "visitors_total": {D1: 2, D2: 0, D3: 1},
    "pel_228_quote_start": {D1: 2, D2: 0, D3: 0},
    "pel_229_quote_complete": {D1: 1, D2: 0, D3: 0},
    "pel_232_save_quote": {D1: 0, D2: 0, D3: 0},
    "pel_269_app_start": {D1: 1, D2: 0, D3: 0},
    "pel_240_app_confirm": {D1: 1, D2: 0, D3: 0},
    "pel_103_product_category_inst": {D1: 1, D2: 0, D3: 0},
    "funnel_quote_complete_over_quote_start": {D1: 0.5, D2: 0.0, D3: 0.0},
    "funnel_save_quote_over_quote_start": {D1: 0.0, D2: 0.0, D3: 0.0},
    "funnel_app_start_over_quote_start": {D1: 0.5, D2: 0.0, D3: 0.0},
    "funnel_app_confirm_over_app_start": {D1: 1.0, D2: 0.0, D3: 0.0},
    "funnel_app_confirm_over_quote_start": {D1: 0.5, D2: 0.0, D3: 0.0},
    "language_share_en": {D1: 0.75, D2: 0.0, D3: 0.0},
    "language_share_fr": {D1: 0.25, D2: 0.0, D3: 0.0},
    "language_share_unknown": {D1: 0.0, D2: 0.0, D3: 1.0},
    "pel_228_quote_start__evar4_hd": {D1: 1, D2: 0, D3: 0},
    "pel_228_quote_start__evar4_travel": {D1: 0, D2: 0, D3: 0},
    "pel_228_quote_start__evar4_other": {D1: 1, D2: 0, D3: 0},
    "pel_269_app_start__evar4_other": {D1: 1, D2: 0, D3: 0},
    "pel_240_app_confirm__evar4_other": {D1: 1, D2: 0, D3: 0},
    "pel_240_app_confirm__evar4_hd": {D1: 0, D2: 0, D3: 0},
}


@pytest.fixture(scope="module")
def built(spark, tmp_path_factory):
    """metric_id -> {date: value} from one build over the staged scenario."""
    staged = tmp_path_factory.mktemp("cm") / "hits.parquet"
    cols = {name: [r[name] for r in ROWS] for name in ROWS[0]}
    arrays = [pa.array(cols[n], type=pa.date32() if n == "hit_date" else pa.string())
              for n in cols]
    pq.write_table(pa.table(arrays, names=list(cols)), staged)

    sdf = spark.read.parquet(str(staged))
    wide = gold_lib.build_kpis_spark(
        sdf, EVENT_IDS, SERIES,
        date_col=DATE_COL, needed_cols=NEEDED_COLS,
        visit_key_cols=VISIT_KEY_COLS, visitor_key_cols=VISITOR_KEY_COLS,
        event_basis=EVENT_BASIS, null_safe_keys=NULL_SAFE_KEYS)
    rows = wide.collect()
    out = {}
    for r in rows:
        rd = r.asDict()
        day = rd.pop(DATE_COL)
        day = day.date() if hasattr(day, "date") else day
        for k, v in rd.items():
            out.setdefault(k, {})[day] = v
    return out


def test_calendar_gap_free_and_all_series_present(built):
    assert set(built["hits_total"]) == {D1, D2, D3}, "gap day D2 must be materialized"
    assert set(built) == {s.metric_id for s in SERIES}


@pytest.mark.parametrize("metric_id", sorted(EXPECTED))
def test_series_value(built, metric_id):
    got = {d: float(built[metric_id][d]) for d in (D1, D2, D3)}
    want = {d: float(v) for d, v in EXPECTED[metric_id].items()}
    assert got == pytest.approx(want), f"{metric_id}: {got} != {want}"


def test_null_safe_keys_no_collision_no_phantom_visitor(spark, tmp_path):
    """concat_ws silently drops NULL key components: an all-NULL visid pair becomes a
    countable "" visitor, and (h, NULL, 1, t) collides with (h, 1, NULL, t). With
    null_safe_keys=True the fully-NULL visitor key is skipped (like countDistinct on a
    single column) and NULL components keep their position."""
    rows = [
        _hit(D1, None, None, "en", None, None, "9", "tx"),   # all-NULL visitor pair
        _hit(D1, None, None, "en", None, None, "9", "tx"),   # (two hits, same visit)
        _hit(D1, None, None, "en", "h", None, "1", "t"),     # ambiguous visit key A
        _hit(D1, None, None, "en", "h", "1", None, "t"),     # ambiguous visit key B
    ]
    staged = tmp_path / "hits.parquet"
    cols = {name: [r[name] for r in rows] for name in rows[0]}
    arrays = [pa.array(cols[n], type=pa.date32() if n == "hit_date" else pa.string())
              for n in cols]
    pq.write_table(pa.table(arrays, names=list(cols)), staged)

    wide = gold_lib.build_kpis_spark(
        spark.read.parquet(str(staged)), EVENT_IDS, SERIES,
        date_col=DATE_COL, needed_cols=NEEDED_COLS,
        visit_key_cols=VISIT_KEY_COLS, visitor_key_cols=VISITOR_KEY_COLS,
        event_basis=EVENT_BASIS, null_safe_keys=True)
    row = wide.collect()[0].asDict()
    # visitors: ("h", NULL) and ("h", "1") are distinct; the all-NULL pair is skipped.
    assert row["visitors_total"] == 2
    # visits: the two ambiguous keys stay distinct; the all-NULL-pair visit still counts
    # (visit_num/visit_start_time_gmt are populated) -> 3 total.
    assert row["visits_total"] == 3


def test_non_monotonic_ratio_exceeds_one_unclamped(spark, tmp_path):
    """Saved-quote resume (SME 2026-07-27, meta.sme_confirmations.funnel_basis): a later
    funnel stage can exceed an earlier one on a given day, so population ratios
    legitimately pass 1.0. Pins the 0-safe ratio in gold_lib against a future clamp."""
    rows = [
        _hit(D1, "228", "hd", "en", "h1", "l1", "1", "t1"),      # 1 quote-start visit
        _hit(D1, "269", None, "en", "h2", "l2", "1", "t2"),      # resume visit A
        _hit(D1, "269,240", None, "en", "h3", "l3", "1", "t3"),  # resume visit B
    ]
    staged = tmp_path / "hits.parquet"
    cols = {name: [r[name] for r in rows] for name in rows[0]}
    arrays = [pa.array(cols[n], type=pa.date32() if n == "hit_date" else pa.string())
              for n in cols]
    pq.write_table(pa.table(arrays, names=list(cols)), staged)

    wide = gold_lib.build_kpis_spark(
        spark.read.parquet(str(staged)), EVENT_IDS, SERIES,
        date_col=DATE_COL, needed_cols=NEEDED_COLS,
        visit_key_cols=VISIT_KEY_COLS, visitor_key_cols=VISITOR_KEY_COLS,
        event_basis=EVENT_BASIS, null_safe_keys=NULL_SAFE_KEYS)
    row = wide.collect()[0].asDict()
    assert row["funnel_app_start_over_quote_start"] == pytest.approx(2.0)
    assert row["funnel_app_confirm_over_quote_start"] == pytest.approx(1.0)


def test_melt_to_long_respects_date_col(spark, tmp_path):
    staged = tmp_path / "hits.parquet"
    cols = {name: [r[name] for r in ROWS] for name in ROWS[0]}
    arrays = [pa.array(cols[n], type=pa.date32() if n == "hit_date" else pa.string())
              for n in cols]
    pq.write_table(pa.table(arrays, names=list(cols)), staged)
    wide = gold_lib.build_kpis_spark(
        spark.read.parquet(str(staged)), EVENT_IDS, SERIES,
        date_col=DATE_COL, needed_cols=NEEDED_COLS,
        visit_key_cols=VISIT_KEY_COLS, visitor_key_cols=VISITOR_KEY_COLS,
        event_basis=EVENT_BASIS, null_safe_keys=NULL_SAFE_KEYS)
    long = gold_lib.melt_to_long(wide, date_col=DATE_COL)
    assert long.columns == [DATE_COL, "metric_id", "value"]
    assert long.count() == 3 * len(SERIES)
