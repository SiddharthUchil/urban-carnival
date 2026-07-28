"""Central config for the GMAI-Pulse CoverMe Databricks pipeline.

CoverMe is the second product domain (after GWAM CA-retirement, conf/settings.py). Same
widget contract (target_catalog / mode / start_date / repo_root); everything else here is a
CoverMe fact confirmed by the verified 2026-07-27 EDA run (eda/coverme_eda.py) and the SME
rulings of 2026-07-27 (research/claude/18-coverme-sme-questions.md, metric-registry.yaml
v0.3.0). Design spec: docs/superpowers/specs/2026-07-27-coverme-pipeline-port-design.md.

Plain importable module -- no dbutils/spark at import time.
"""
from __future__ import annotations

# --- Source (read-only, confirmed in EDA) ---
# Databricks-native Delta (NOT Synapse). Single Adobe report suite: the table has NO rsid
# column, so scope is URL-only -- there is no SCOPE_RSID / suite-mode machinery here.
SOURCE_TABLE = "csdo_prod_catalog.adobe_coverme_bronze.hit_data"
PARTITION_COL = "hit_date"   # typed date -- predicates prune partitions directly

# URL scope (SME ruling 2026-07-27, item 3): exactly the two production brand domains.
# Everything else (legacy life hits, unclassified prod-adjacent hosts) is out of scope.
URL_SCOPE_INCLUDE = [
    "%coverme.com%",            # EN brand domain
    "%pourmeproteger.com%",     # FR brand domain
]

# Retired insttrip host, BASELINE ONLY (same ruling): dead since 2024-03-11. Ingest admits
# it solely for hit_date <= BASELINE_INCLUDE_END (cm_silver_lib.scope_expr baseline
# clause), so it contributes backfill history and can never re-enter go-forward scope.
URL_SCOPE_BASELINE_INCLUDE = ["%insttrip.manulife.com%"]
BASELINE_INCLUDE_END = "2024-03-11"

# Non-production noise excluded even when matched by the include list (EDA S4b-confirmed):
# AEM authoring/staging hosts, UAT mirrors of both brand domains, local dev.
URL_SCOPE_EXCLUDE = [
    "%adobeaemcloud.com%",
    "%author-aem-prod.manulife.ca%",
    "%uat.coverme.com%",
    "%uat.pourmeproteger.com%",
    "%.uat.%",
    "%www-aem-stage%",
    "%localhost:5000%",
]

# URL coalesce order -- page_url FIRST (0.0005% blank), inverted vs the GWAM production
# pipeline which matches post_page_url (58.9% blank here). Blank-guarded in cm_silver_lib.
URL_COALESCE_COLS = ["page_url", "visit_start_page_url", "first_hit_page_url", "post_page_url"]

# --- Hit eligibility (silver; Analysis Workspace parity) ---
# Adobe datafeeds-calculate (link supplied by Kerrian 2026-07-27): Workspace only counts
# hits where exclude_hit = 0, customer_perspective = 0, and hit_source not in (5,7,8,9).
# This feed has NO customer_perspective column (verified against the 1,180-column schema
# dump in the decoded probe/EDA exports, 2026-07-27), so that condition is omitted.
# hit_source only takes values {1, 2} today -- the (5,7,8,9) guard is a no-op kept for doc
# parity and future data-source feeds. Baseline eligible share ~94.33% (exclude_hit: 0 ->
# 94.33%, 14 -> 4.9%, 4 -> 0.78%). Applied at SILVER; bronze keeps raw scoped rows so the
# excluded volume stays observable.
ELIGIBLE_HIT_SOURCE_EXCLUDE = ["5", "7", "8", "9"]

# --- Target (parameterized) ---
CATALOG_PLACEHOLDER = "__SET_ME__"
BRONZE_SCHEMA = "gmai_pulse_bronze"
SILVER_SCHEMA = "gmai_pulse_silver"
GOLD_SCHEMA = "gmai_pulse_gold"

# Schemas are shared with GWAM; table names carry the product to avoid collision.
BRONZE_TABLE = "adobe_hit_coverme"
SILVER_TABLE = "hits_conformed_coverme"
GOLD_KPI_TABLE = "kpi_daily_coverme"

# --- Incremental / windowing ---
OVERLAP_DAYS = 5               # late-arrival p50/p95/p99 = 1/2/5 days (doc 17) -- GWAM uses 3
BACKFILL_START = "2023-02-28"  # first date with real data; full-history backfill is the
                               # verified EDA basis (57.7M scoped rows / 1,211 daily points)

# --- Identity / privacy (ADR-0007; same secret as GWAM) ---
HMAC_SECRET_SCOPE = "gmai_pulse"
HMAC_SECRET_KEY = "identity_hmac_key"
IDENTITY_COLS = ["mcvisid", "post_visid_high", "post_visid_low"]

# date_time carries America/Toronto wall-clock (constant -4/-5h offset vs hit_time_gmt,
# EDA S10). Pinned as the session timezone in silver so timestamp casts stay deterministic.
TIMEZONE = "America/Toronto"

DOMAIN = "coverme"


class CmSettings:
    """Resolved run configuration + fully-qualified table names."""

    def __init__(self, catalog, mode="incremental", start_date=None, repo_root=None):
        self.catalog = catalog
        self.mode = mode  # "incremental" | "backfill"
        self.start_date = start_date or BACKFILL_START
        self.repo_root = repo_root

    @property
    def bronze(self):
        return f"{self.catalog}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"

    @property
    def silver(self):
        return f"{self.catalog}.{SILVER_SCHEMA}.{SILVER_TABLE}"

    @property
    def gold_kpi(self):
        return f"{self.catalog}.{GOLD_SCHEMA}.{GOLD_KPI_TABLE}"

    def __repr__(self):
        return (f"CmSettings(catalog={self.catalog!r}, mode={self.mode!r}, "
                f"start_date={self.start_date!r})")


def _widget(dbutils, name, default):
    if dbutils is None:
        return default
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    try:
        v = dbutils.widgets.get(name)
    except Exception:
        return default
    return v if v not in (None, "") else default


def resolve(dbutils=None):
    """Build CmSettings from job parameters / notebook widgets.

    Same contract as conf/settings.resolve: fails fast if target_catalog is still the
    placeholder (ADR-0006: one governed compute plane, explicit catalog).
    """
    catalog = _widget(dbutils, "target_catalog", CATALOG_PLACEHOLDER)
    mode = _widget(dbutils, "mode", "incremental")
    start_date = _widget(dbutils, "start_date", BACKFILL_START)
    repo_root = _widget(dbutils, "repo_root", "")

    s = CmSettings(catalog, mode=mode, start_date=start_date, repo_root=(repo_root or None))
    if s.catalog == CATALOG_PLACEHOLDER:
        raise ValueError(
            "target_catalog is unset. Set the 'target_catalog' job parameter (or notebook "
            f"widget) to a writable Unity Catalog; schemas {BRONZE_SCHEMA}/{SILVER_SCHEMA}/"
            f"{GOLD_SCHEMA} are created there."
        )
    return s
