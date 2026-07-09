"""Column policy for the bronze mirror.

The detector needs a small, known set of columns. The EDA census found ~197 of the
1,198 source columns populated in the GWAM CA-retirement slice, but that census exists
only as notebook output, not a machine-readable list -- so we do NOT hardcode a guessed
197-name list here. Bronze keeps the verified required set below and, when the operator
pastes the real EDA populated-column census into POPULATED_COLUMNS, widens to the full
mirror (plan D1). Every name below was verified present in the source schema via the
field profile (new_data/generated_data_profile.json, 1,198 columns).

Plain importable module -- no dbutils/spark at import time.
"""
from __future__ import annotations

# Scope + partition + timestamp source columns (needed by ingest/silver, not the model).
KEY_COLUMNS = [
    "process_date",   # partition / calendar
    "rsid",           # scope filter
    "post_page_url",  # scope filter
    "date_time",      # local timestamp (preferred)
    "hit_time_gmt",   # epoch GMT fallback
]

# Everything the detector + operational rules consume: union of detect/kpis.NEEDED_COLS
# and detect/registry.RULE_DIMS (post_pagename / language appear in both).
DETECTOR_COLUMNS = [
    "post_event_list",
    "post_pagename",
    "language",
    "mcvisid",
    "post_visid_high",
    "post_visid_low",
    "visit_num",
    "ref_type",
    "connection_type",
    "browser",
    "va_closer_id",
]

# Sensitive columns (24, from the field profile's sensitive_column_summary). Never land in
# silver; excluded from bronze regardless of POPULATED_COLUMNS.
SENSITIVE_COLUMNS = [
    "cookies", "daily_visitor", "geo_zip", "hourly_visitor", "ip", "ip2", "ipv6",
    "j_jscript", "javascript", "monthly_visitor", "persistent_cookie", "post_cookies",
    "post_persistent_cookie", "post_socialaccountandappids", "post_tnt", "post_zip",
    "quarterly_visitor", "socialaccountandappids", "userid", "weekly_visitor",
    "yearly_visitor", "zip", "customer_perspective", "post_customer_perspective",
]

# OPTIONAL full-width mirror: paste the EDA "populated columns" census here to keep all
# ~197 populated columns in bronze for Phase-2 investigation. Leave empty for the minimal
# (required-only) bronze. Sensitive columns are always excluded regardless.
POPULATED_COLUMNS: list[str] = []

# Columns the schema-contract check requires the source to expose (fail fast, ADR-0006).
REQUIRED_SOURCE_COLUMNS = KEY_COLUMNS + DETECTOR_COLUMNS

# Columns silver projects from bronze (detector-ready), before deriving event_ts.
SILVER_COLUMNS = ["process_date"] + DETECTOR_COLUMNS + ["date_time", "hit_time_gmt"]


def bronze_select(available: list[str]) -> list[str]:
    """Ordered, de-duplicated columns to project into bronze, minus sensitive columns.

    `available` is spark.table(SOURCE).columns; anything requested but absent is dropped
    silently here -- presence of the REQUIRED set is enforced separately by the contract
    check so the failure message is specific.
    """
    want = list(dict.fromkeys(KEY_COLUMNS + DETECTOR_COLUMNS + POPULATED_COLUMNS))
    avail = set(available)
    sensitive = set(SENSITIVE_COLUMNS)
    return [c for c in want if c in avail and c not in sensitive]
