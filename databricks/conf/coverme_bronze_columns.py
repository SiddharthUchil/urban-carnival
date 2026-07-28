"""Column policy for the CoverMe bronze mirror.

CoverMe analogue of conf/bronze_columns.py. Every name below was verified present in the
source schema via the decoded discovery-probe/EDA exports (1,180 columns, 2026-07-27).
Notable absences vs the Adobe reference doc: the feed has NO customer_perspective column
(the Workspace-parity eligibility filter therefore uses exclude_hit + hit_source only) and
NO rsid column (single-suite feed, URL-only scope).

Plain importable module -- no dbutils/spark at import time.
"""
from __future__ import annotations

# Scope + partition + timestamp + eligibility source columns (needed by ingest/silver).
# All four URL candidates are kept: bronze preserves the raw columns; silver derives the
# blank-guarded page_url-first coalesce (cm_silver_lib.url_expr).
KEY_COLUMNS = [
    "hit_date",              # partition / calendar (typed date)
    "page_url",              # scope filter -- coalesce lead (0.0005% blank)
    "visit_start_page_url",  # coalesce fallback 2
    "first_hit_page_url",    # coalesce fallback 3
    "post_page_url",         # coalesce fallback 4 (58.9% blank)
    "date_time",             # local timestamp (America/Toronto wall-clock, preferred)
    "hit_time_gmt",          # epoch GMT fallback
    "exclude_hit",           # eligibility: Workspace counts exclude_hit = 0 only
    "hit_source",            # eligibility: Workspace drops hit_source in (5,7,8,9)
]

# Everything gold consumes: identity/visit keys (Adobe 4-part visit key incl.
# visit_start_time_gmt), the event list, and the KPI dimensions.
DETECTOR_COLUMNS = [
    "post_event_list",
    "post_pagename",         # 22% null on this feed -- URL path is the primary dim later
    "mcvisid",
    "post_visid_high",
    "post_visid_low",
    "visit_num",
    "visit_start_time_gmt",  # 4th visit-key component (guards visit_num reuse)
    "post_evar4",            # Product Category -- SME-priority funnel breakdown dim
]

# The other business-flagged AD eVars (CoverMeDataMap.xlsx Notes col; registry v0.3.0
# candidates). Mirrored into bronze so promoting one later needs no source re-backfill;
# silver does not carry them yet.
FLAGGED_EVAR_COLUMNS = [
    "post_evar5",    # Product ID
    "post_evar6",    # Sponsor/Distributor/Association
    "post_evar11",   # Quote Session ID
    "post_evar16",   # Transaction ID
    "post_evar52",   # Current Page
    "post_evar111",  # Experience Cloud ID
    "post_evar116",  # Bot Traffic -- corroboration signal only (SME 2026-07-27)
    "post_evar148",  # Bot Detector (corroboration only, SME 2026-07-27)
]

# Sensitive columns: the generic Adobe set (conf/bronze_columns.py) plus the CoverMe data
# map's direct-identifier eVars. Never land in silver; excluded from bronze regardless of
# POPULATED_COLUMNS. Names absent from this feed (e.g. customer_perspective) are harmless.
SENSITIVE_COLUMNS = [
    "cookies", "daily_visitor", "geo_zip", "hourly_visitor", "ip", "ip2", "ipv6",
    "j_jscript", "javascript", "monthly_visitor", "persistent_cookie", "post_cookies",
    "post_persistent_cookie", "post_socialaccountandappids", "post_tnt", "post_zip",
    "quarterly_visitor", "socialaccountandappids", "userid", "weekly_visitor",
    "yearly_visitor", "zip", "customer_perspective", "post_customer_perspective",
    # CoverMe data map identifiers
    "evar14", "post_evar14",     # User ID
    "evar121", "post_evar121",   # Hashed Email ID
    "evar172", "post_evar172",   # UserID
    "evar173", "post_evar173",   # Customer ID
]

# OPTIONAL full-width mirror (same mechanism as GWAM): paste a populated-column census here
# to widen bronze. Leave empty for the minimal (required + flagged) bronze.
POPULATED_COLUMNS: list[str] = []

# Columns the schema-contract check requires the source to expose (fail fast, ADR-0006).
# FLAGGED_EVAR_COLUMNS are included: all verified present, and silent absence would
# invalidate the no-re-backfill promise above.
REQUIRED_SOURCE_COLUMNS = KEY_COLUMNS + DETECTOR_COLUMNS + FLAGGED_EVAR_COLUMNS

# Columns silver projects from bronze before deriving event_ts / url / language and
# applying the eligibility filter.
SILVER_COLUMNS = (["hit_date"] + DETECTOR_COLUMNS
                  + ["date_time", "hit_time_gmt", "exclude_hit", "hit_source"]
                  + ["page_url", "visit_start_page_url", "first_hit_page_url", "post_page_url"])


def bronze_select(available: list[str]) -> list[str]:
    """Ordered, de-duplicated columns to project into bronze, minus sensitive columns.

    Same contract as conf/bronze_columns.bronze_select: absent names drop silently here;
    the REQUIRED set is enforced separately so the failure message is specific.
    """
    want = list(dict.fromkeys(
        KEY_COLUMNS + DETECTOR_COLUMNS + FLAGGED_EVAR_COLUMNS + POPULATED_COLUMNS))
    avail = set(available)
    sensitive = set(SENSITIVE_COLUMNS)
    return [c for c in want if c in avail and c not in sensitive]
