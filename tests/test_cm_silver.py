"""CoverMe silver expressions: URL coalesce, scope, domain language, hit eligibility.

Mirrors the verified EDA semantics (eda/coverme_eda.py S4/S4c) and the Workspace-parity
eligibility rule (Adobe datafeeds-calculate; customer_perspective absent from this feed).
Same parquet staging pattern as the other Spark tests.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "databricks" / "src"))
sys.path.insert(0, str(REPO / "databricks"))

pytest.importorskip("pyspark")
pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

import cm_silver_lib as cml  # noqa: E402
from conf.coverme_settings import (  # noqa: E402
    URL_SCOPE_INCLUDE, URL_SCOPE_EXCLUDE, ELIGIBLE_HIT_SOURCE_EXCLUDE,
)


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    from pyspark.sql import SparkSession

    s = (SparkSession.builder
         .master("local[2]")
         .appName("cm-silver")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    # ANSI on: the production cluster runs ansi.enabled=true (doc-17 E1 lesson), so every
    # silver expression is exercised under the strict-cast regime it will actually face.
    # Set as RUNTIME conf: getOrCreate() ignores builder configs when another test module's
    # session is being reused, and runtime conf applies either way.
    s.conf.set("spark.sql.ansi.enabled", "true")
    s.conf.set("spark.sql.session.timeZone", "UTC")
    yield s
    s.stop()


def _stage(spark, tmp_path, cols):
    staged = tmp_path / "rows.parquet"
    names = list(cols)
    pq.write_table(
        pa.table([pa.array(cols[n], type=pa.string()) for n in names], names=names), staged)
    return spark.read.parquet(str(staged))


def test_url_expr_page_url_first_blank_guarded(spark, tmp_path):
    df = _stage(spark, tmp_path, {
        "page_url": ["https://www.CoverMe.com/A", "  ", None, ""],
        "visit_start_page_url": ["ignored", "https://fallback.two/B", " ", None],
        "first_hit_page_url": ["ignored", "ignored", "https://fallback.three/C", ""],
        "post_page_url": ["ignored", "ignored", "ignored", None],
    })
    got = [r["u"] for r in df.withColumn("u", cml.url_expr()).collect()]
    assert got == ["https://www.coverme.com/a",   # lead column wins, lowercased
                   "https://fallback.two/b",      # blank lead -> 2nd candidate
                   "https://fallback.three/c",    # blank 1+2 -> 3rd candidate
                   ""]                            # all blank/NULL -> '' (never NULL)


def test_scope_expr_include_minus_exclude(spark, tmp_path):
    df = _stage(spark, tmp_path, {
        "page_url": [
            "https://www.coverme.com/covme/health-insurance",   # in scope
            "https://www.pourmeproteger.com/sante",             # in scope (FR)
            "https://insttrip.manulife.com/trip",               # in scope (baseline host)
            "https://uat.coverme.com/covme/test",               # exclude beats include
            "https://www.manulife.ca/retirement",               # not included
            None,                                               # NULL url -> out, not NULL
        ],
        "visit_start_page_url": [None] * 6,
        "first_hit_page_url": [None] * 6,
        "post_page_url": [None] * 6,
    })
    got = [r["s"] for r in df.withColumn(
        "s", cml.scope_expr(URL_SCOPE_INCLUDE, URL_SCOPE_EXCLUDE)).collect()]
    assert got == [True, True, True, False, False, False]


def test_lang_from_host_domain_derived(spark, tmp_path):
    df = _stage(spark, tmp_path, {
        "page_url": [
            "https://www.coverme.com/covme/x",          # en
            "http://insttrip.manulife.com/trip",        # en (retired host, history)
            "https://www.pourmeproteger.com/sante",     # fr
            "https://www.assurance-manuvie.ca/x",       # fr
            "https://something.else.com/x",             # unknown
            None,                                       # no URL at all -> unknown
        ],
        "visit_start_page_url": [None] * 6,
        "first_hit_page_url": [None] * 6,
        "post_page_url": [None] * 6,
    })
    got = [r["l"] for r in df.withColumn("l", cml.lang_from_host_expr(cml.url_expr())).collect()]
    assert got == ["en", "en", "fr", "fr", "unknown", "unknown"]


def test_event_ts_ansi_safe_fallback(spark, tmp_path):
    from pyspark.sql import functions as F
    df = _stage(spark, tmp_path, {
        "date_time": ["2026-07-01 10:00:00", "not-a-timestamp", None],
        "hit_time_gmt": ["1751364000", "1751364000", None],
    })
    # Compare as Spark-formatted strings (parse and format share the session tz), so the
    # assertion is independent of the Python process timezone.
    got = [r["s"] for r in df.withColumn(
        "s", F.date_format(cml.event_ts_expr(), "yyyy-MM-dd HH:mm:ss")).collect()]
    assert got[0] == "2026-07-01 10:00:00"   # parseable date_time preferred
    assert got[1] is not None                # junk date_time -> GMT fallback, not an
    #                                          ANSI CAST_INVALID_INPUT crash
    assert got[2] is None                    # nothing usable -> NULL


def test_eligible_expr_workspace_parity(spark, tmp_path):
    df = _stage(spark, tmp_path, {
        "exclude_hit": ["0", "14", "4", "0", "0", None, "junk"],
        "hit_source": ["1", "1", "1", "5", "2", "1", None],
    })
    got = [r["e"] for r in df.withColumn(
        "e", cml.eligible_expr(ELIGIBLE_HIT_SOURCE_EXCLUDE)).collect()]
    # exclude_hit != 0 drops; hit_source in (5,7,8,9) drops; NULL/uncastable keeps
    # (matches the EDA clean-hits formula coalesce(try_cast(exclude_hit as int), 0) = 0).
    assert got == [True, False, False, False, True, True, True]
