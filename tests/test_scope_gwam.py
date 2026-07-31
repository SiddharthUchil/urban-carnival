"""Gate G6 -- the GWAM scope constants, which nothing previously asserted.

doc 19 §3 calls these "the single highest-consequence config in the repo": flipping
SCOPE_URL_MODE or SCOPE_SUITE_MODE re-baselines every downstream KPI, detector threshold and
injected-anomaly calibration, and under mode=incremental it writes a step change mid-series
that the detector reads as a level-shift anomaly. CoverMe's equivalent predicate is tested
(test_cm_silver.test_scope_expr_include_minus_exclude); the GWAM side was not.

SCOPE OF THIS FILE -- read before trusting it. The GWAM scope predicate is built inline in
the databricks/src/01_bronze_ingest.py notebook, which cannot be imported (it calls dbutils
at module level), so unlike the CoverMe test this file cannot exercise the production
expression object directly. It does two things instead:

  1. Pins the constants, so an unreviewed flip fails a test rather than silently
     re-baselining production. This is the actual G6 ask.
  2. Checks the LIKE patterns behave as intended, composed the same way the notebook
     composes them, under real Spark SQL semantics rather than Python `in`.

The gap that leaves: a change to the notebook's composition logic (not to the constants)
would not be caught here. Closing that properly means extracting the predicate into an
importable helper the way cm_silver_lib.scope_expr already is -- worth doing, deliberately
not done here because it would edit production ingest code under a test-only change.
"""
from __future__ import annotations

import os
import sys
from functools import reduce
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "databricks"))

from conf.settings import (  # noqa: E402
    PAGE_VIEW_BASIS, SCOPE_LOGIN_HOST_EXCLUDE, SCOPE_RSID, SCOPE_SUITE_MODE,
    SCOPE_URL_LIKE, SCOPE_URL_LIKE_BROAD, SCOPE_URL_LIKE_EXCLUDE, SCOPE_URL_MODE,
    LEGACY_SCOPE_RSID, LEGACY_SCOPE_URL_LIKE,
)

pytest.importorskip("pyspark")
pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    from pyspark.sql import SparkSession

    s = (SparkSession.builder
         .master("local[2]")
         .appName("gwam-scope")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _matches(spark, tmp_path, urls, patterns):
    """[bool] per url: does it match ANY pattern, composed as 01_bronze_ingest.py does?

    Staged through parquet rather than spark.createDataFrame: createDataFrame from Python
    objects spins up Python workers, and this box's worker interpreter is 3.11 against a
    3.12 driver (PYTHON_VERSION_MISMATCH). Reading parquet keeps the whole expression
    JVM-side, which is the same reason the other Spark tests here stage files.
    """
    from pyspark.sql import functions as F

    staged = tmp_path / f"urls_{abs(hash(tuple(urls))) % 10**8}.parquet"
    pq.write_table(pa.table({"url": pa.array(urls, pa.string())}), staged)
    df = spark.read.parquet(str(staged))
    urlc = F.lower(F.col("url").cast("string"))
    expr = reduce(lambda acc, p: acc | urlc.like(p.lower()), patterns, F.lit(False))
    return [r["m"] for r in df.withColumn("m", expr).collect()]


# --------------------------------------------------------------------------- pinned config

def test_scope_mode_toggles_are_held():
    """Both toggles stay on the shipped branch. Changing either is a full mode=backfill with
    gold truncated (settings.py) -- if you meant it, update this test in the same commit."""
    assert SCOPE_URL_MODE == "en_only", (
        "SCOPE_URL_MODE flipped. 'broad' widens to group-plans/regimes-collectifs and needs "
        "a re-profile plus product sign-off first (doc 19 §2.2, doc 16 D3).")
    assert SCOPE_SUITE_MODE == "current_only", (
        "SCOPE_SUITE_MODE flipped. 'with_legacy' unions the CONCURRENT manugrs suite -- only "
        "~12 eVars are shared, so eVar-derived series are not splice-safe (doc 14).")


def test_scope_identifiers_pinned():
    assert SCOPE_RSID == "manulifeglobalprod"
    assert LEGACY_SCOPE_RSID == "manugrs"
    assert SCOPE_URL_LIKE == "%manulife.com/ca/en/personal/group-plans/group-retirement%"


def test_page_view_basis_pinned():
    """doc 20 Q6 is unanswered, so the basis must stay on the branch that re-baselines
    nothing. Flipping to adobe_pv moves every page-view series (C12: 2.885 -> 1.343)."""
    assert PAGE_VIEW_BASIS == "all_hits", (
        "PAGE_VIEW_BASIS flipped without the SME answering doc 20 Q6. This is a full "
        "mode=backfill with gold truncated, and post_page_event must be backfilled into "
        "bronze history first.")


def test_login_host_exclude_covers_the_ruled_hosts():
    """The 2026-07-20 business rule (doc 16 D8) is an explicit host list, not a %portal%
    pattern -- most of these hosts do not contain 'portal' and FR 'portail' would not match
    it. Pin the membership so a well-meaning simplification cannot silently readmit traffic."""
    assert len(SCOPE_LOGIN_HOST_EXCLUDE) == 6
    for host in ("portal.manulife.ca", "id.manulife.ca", "grsmembers.manulife.com",
                 "gsrs1.manulife.com", "viproom.manulife.com", "portail.manuvie.ca"):
        assert any(host in p for p in SCOPE_LOGIN_HOST_EXCLUDE), f"{host} no longer excluded"


def test_pattern_lists_are_non_empty():
    """A reduce over an empty list collapses to lit(False): an empty include list would
    ingest nothing, an empty exclude list would silently disable the filter."""
    for name, pats in (("SCOPE_URL_LIKE_BROAD", SCOPE_URL_LIKE_BROAD),
                       ("SCOPE_URL_LIKE_EXCLUDE", SCOPE_URL_LIKE_EXCLUDE),
                       ("SCOPE_LOGIN_HOST_EXCLUDE", SCOPE_LOGIN_HOST_EXCLUDE),
                       ("LEGACY_SCOPE_URL_LIKE", LEGACY_SCOPE_URL_LIKE)):
        assert pats, f"{name} is empty -- the reduce would collapse to a constant"
        assert all(p.startswith("%") or p.endswith("%") for p in pats), (
            f"{name} has an unanchored pattern; LIKE without % matches the whole string")


# ------------------------------------------------------------------- behaviour under Spark

def test_login_hosts_are_excluded(spark, tmp_path):
    """Every ruled login host matches the exclusion; ordinary retirement traffic does not."""
    login = [
        "https://portal.manulife.ca/signin",
        "https://stage.portal.manulife.ca/signin",     # substring match covers nonprod
        "https://id.manulife.ca/oauth/authorize",
        "https://grsmembers.manulife.com/wps/portal",
        "https://gsrs1.manulife.com/passport/login.jsp",
        "https://viproom.manulife.com/servlet/vip",
        "https://portail.manuvie.ca/connexion",        # FR portal: 'portal' would miss this
    ]
    assert all(_matches(spark, tmp_path, login, SCOPE_LOGIN_HOST_EXCLUDE)), "a ruled login host got through"

    keep = [
        "https://www.manulife.com/ca/en/personal/group-plans/group-retirement.html",
        "https://www.manulife.ca/ca/fr/particuliers/regimes-collectifs/retraite-collective",
    ]
    assert not any(_matches(spark, tmp_path, keep, SCOPE_LOGIN_HOST_EXCLUDE)), "in-scope url wrongly excluded"


def test_en_only_url_scope_selects_the_shipped_population(spark, tmp_path):
    """SCOPE_URL_LIKE is the shipped English section root and nothing else."""
    urls = [
        "https://www.manulife.com/ca/en/personal/group-plans/group-retirement.html",   # in
        "https://www.manulife.com/ca/en/personal/group-plans/group-retirement/x",      # in
        "https://www.manulife.com/ca/fr/particuliers/regimes-collectifs/retraite",     # out: FR
        "https://www.manulife.com/ca/en/personal/group-plans/group-benefits.html",     # out: benefits
        "https://www.manulifeim.com/group-retirement/ca/en/overview",                  # out: legacy suite
    ]
    assert _matches(spark, tmp_path, urls, [SCOPE_URL_LIKE]) == [True, True, False, False, False]


def test_broad_mode_would_widen_and_still_drop_noise(spark, tmp_path):
    """Inert today (en_only), but pinned so the branch is known-good before anyone flips it:
    broad covers EN + FR retirement roots, and the exclude list still removes AEM author
    hosts and non-CA /ph/ paths."""
    widened = [
        "https://www.manulife.com/ca/en/personal/group-plans/group-retirement.html",
        "https://www.manulifeim.com/group-retirement/ca/fr/apercu",
        "https://www.manulife.ca/ca/fr/particuliers/regimes-collectifs/retraite-collective",
    ]
    assert all(_matches(spark, tmp_path, widened, SCOPE_URL_LIKE_BROAD))

    noise = [
        "https://author-p1.adobeaemcloud.com/content/group-retirement",
        "https://www.manulife.com/ph/en/group-plans/group-retirement.html",
    ]
    assert all(_matches(spark, tmp_path, noise, SCOPE_URL_LIKE_BROAD)), "fixture should hit include first"
    assert all(_matches(spark, tmp_path, noise, SCOPE_URL_LIKE_EXCLUDE)), "exclude must subtract it back out"
