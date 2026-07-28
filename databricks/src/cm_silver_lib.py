"""CoverMe-only pure Spark-column transforms (URL scope, domain language, eligibility).

CoverMe sibling of silver_lib.py (whose event_ts_expr / normalize_event_list_expr /
pseudonymize_expr are product-agnostic and reused as-is). Semantics mirror the verified
EDA run (eda/coverme_eda.py S4/S4c) byte-for-byte. Requires pyspark; no dbutils.
"""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.column import Column

# page_url FIRST -- 0.0005% blank vs 58.9% for post_page_url (inverted vs the GWAM
# production predicate). Duplicated from conf.coverme_settings.URL_COALESCE_COLS so this
# module stays importable as a flat src/ module without conf on the path.
URL_CANDIDATES = ("page_url", "visit_start_page_url", "first_hit_page_url", "post_page_url")


def url_expr(cols=URL_CANDIDATES) -> Column:
    """Blank-guarded coalesce over the URL candidates, lowercased. Adobe writes empty
    strings, not NULLs, so blanks map to NULL before the coalesce, then land on '' so the
    NOT LIKE exclusions stay well-defined (matches eda/coverme_eda.py url_expr)."""
    parts = [F.when(F.trim(F.col(c).cast("string")) != F.lit(""),
                    F.trim(F.col(c).cast("string"))) for c in cols]
    return F.lower(F.coalesce(*parts, F.lit("")))


def like_any(colexpr: Column, patterns) -> Column:
    """Null-safe OR of SQL LIKE patterns. Blank/NULL input yields False, never NULL, so
    ~like_any(...) keeps rather than silently drops the row."""
    m = None
    for p in patterns:
        m = colexpr.like(p.lower()) if m is None else (m | colexpr.like(p.lower()))
    return F.coalesce(m, F.lit(False))


def scope_expr(include, exclude, cols=URL_CANDIDATES) -> Column:
    """CoverMe URL-only scope: the coalesced URL matches any include pattern and none of
    the exclude patterns (single-suite feed -- there is no rsid condition)."""
    u = url_expr(cols)
    return like_any(u, include) & ~like_any(u, exclude)


def lang_from_host_expr(url: Column) -> Column:
    """Domain-derived language (SME interim ruling 2026-07-27 -- eVar8 is likely
    mis-tagged and is NOT language of record). coverme.com/insttrip = en,
    pourmeproteger/manuvie = fr, anything else (incl. no URL) = unknown."""
    host = F.regexp_extract(F.regexp_replace(url, r"^[a-z]+://", ""), r"^([^/]+)", 1)
    return (F.when(host.rlike(r"pourmeproteger|manuvie|assurance-manuvie"), "fr")
             .when(host.rlike(r"coverme\.com|insttrip\.manulife\.com"), "en")
             .otherwise("unknown"))


def event_ts_expr(local_col: str = "date_time", gmt_col: str = "hit_time_gmt") -> Column:
    """ANSI-safe sibling of silver_lib.event_ts_expr. The production cluster runs
    spark.sql.ansi.enabled=true, where a plain cast of an unparseable string THROWS
    (CAST_INVALID_INPUT) instead of returning NULL -- the coalesce fallback would never
    be reached (doc-17 E1 lesson: always prefer try_* in these notebooks). try_cast
    keeps the fallback chain intact; identical output on well-formed data."""
    return F.coalesce(
        F.expr(f"try_cast({local_col} as timestamp)"),
        F.expr(f"cast(from_unixtime(try_cast({gmt_col} as bigint)) as timestamp)"),
    )


def eligible_expr(hit_source_exclude) -> Column:
    """Analysis-Workspace hit eligibility (Adobe datafeeds-calculate): keep hits where
    exclude_hit = 0 and hit_source is not a data-source row. This feed has no
    customer_perspective column (verified 2026-07-27), so that condition is omitted.
    NULL/uncastable values keep the row (matches the EDA clean-hits formula
    ``coalesce(try_cast(exclude_hit as int), 0) = 0``); try_cast keeps the expressions
    safe under the cluster's ANSI mode."""
    keep_excl = F.expr("coalesce(try_cast(exclude_hit as int), 0) = 0")
    src = F.expr("try_cast(hit_source as int)")
    bad_src = F.coalesce(src.isin([int(x) for x in hit_source_exclude]), F.lit(False))
    return keep_excl & ~bad_src
