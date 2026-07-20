"""Central config for the GMAI-Pulse GWAM Canada-Retirement Databricks pipeline.

Everything workspace-specific (the writable catalog) is a placeholder resolved at
runtime from job parameters / notebook widgets, so the same code runs unchanged in any
workspace. Source identifiers and the GWAM CA-retirement scope filter are fixed facts
confirmed by the EDA run (see eda/gwam_canada_retirement_eda.py).

Plain importable module -- no dbutils/spark at import time.
"""
from __future__ import annotations

# --- Source (read-only, confirmed in EDA) ---
SOURCE_TABLE = "gwam_prod_catalog.inv_typed_common.adobe_hit_data"
PARTITION_COL = "process_date"

# GWAM Canada-Retirement subset: rsid + retirement URL. EDA: 1,151,474 hits, 157 days.
SCOPE_RSID = "manulifeglobalprod"

# URL scope mode. "en_only" reproduces the shipped population exactly (single English
# section root, applied to post_page_url). "broad" widens to a language-agnostic,
# multi-domain retirement scope. KEEP "en_only" until EDA S4b quantifies the excluded
# volume and the widened population is re-profiled -- flipping this re-baselines every
# downstream KPI, detector threshold, and injected-anomaly calibration.
SCOPE_URL_MODE = "en_only"

# Current production scope -- English section root only.
SCOPE_URL_LIKE = "%manulife.com/ca/en/personal/group-plans/group-retirement%"

# Proposed broad scope: SQL LIKE patterns OR-ed together, matched case-insensitively on the
# COMPLETE url (coalesce(page_url, post_page_url) -- post_page_url is ~37% blank on this report
# suite, EDA S4b). NOTE group-plans is the umbrella that CONTAINS group-retirement (also pulls
# in group-benefits / business / advisor), so product sign-off is needed before activating.
SCOPE_URL_LIKE_BROAD = [
    "%/group-retirement%",     # EN retirement subsection + pagename section token
    "%/group-plans%",          # EN group-plans umbrella (personal/business/advisor)
    "%/regimes-collectifs%",   # FR equivalent (particuliers/entreprises/conseillers) -- S4b-confirmed
]

# Excluded even when matched by BROAD: Adobe AEM authoring/staging hosts (content-author
# previews, not real traffic) and non-CA Philippines paths. S4b-confirmed noise.
SCOPE_URL_LIKE_EXCLUDE = [
    "%adobeaemcloud.com%",
    "%/ph/%",
]

# Individual-login (member/auth) hosts -- excluded from anomaly scope in EVERY mode.
# Business rule (2026-07-20): individual page-login traffic is not considered for anomaly
# detection. An explicit host list, NOT a %portal% pattern: most of these hosts don't
# contain "portal", and FR "portail" wouldn't match it. Substring match also covers
# nonprod subdomains (stage.portal.manulife.ca, ...). Confirmed by the 2026-07-20 URL
# scope inventory run (eda/gwam_url_scope_inventory.ipynb); candidates NOT yet ruled on:
# retirement.sponsor.manulife.com (sponsor login), manulifeplan.ca, epargnemanuvie.ca.
SCOPE_LOGIN_HOST_EXCLUDE = [
    "%portal.manulife.ca%",       # member portal (Storefront), ~130M hits on manugrs
    "%id.manulife.ca%",           # login / identity, 62.6M hits
    "%grsmembers.manulife.com%",  # legacy GRS member portal (WebSphere /wps, /passport)
    "%gsrs1.manulife.com%",       # legacy passport JSP screens (sponsor/member)
    "%viproom.manulife.com%",     # legacy VIP room servlet
    "%portail.manuvie.ca%",       # FR member portal
]

# Report-suite scope mode. "current_only" ingests only the shipped suite (SCOPE_RSID);
# "with_legacy" ALSO unions the pre-Storefront CA-Retirement suite `manugrs`, which carries
# ~2.5 yr of history that ends exactly as manulifeglobalprod begins (2026-02-01 cutover;
# research/claude/14-manugrs-cross-suite-analysis.md). KEEP "current_only" until the business
# signs off unioning the legacy suite -- flipping this re-baselines every downstream KPI, and
# only ~12 eVars are shared across the two suites (eVar-derived series are NOT splice-safe).
SCOPE_SUITE_MODE = "current_only"

# Legacy CA-Retirement suite (pre-Storefront, old site manulifeim.com/group-retirement/ca/en).
# URL scope is a list so the S4b-confirmed French `/ca/fr` path can be appended after sign-off
# without a shape change. Matched on the COMPLETE url (coalesce(page_url, post_page_url)) because
# post_page_url is ~48% blank on this suite (EDA S4b, manugrs run).
LEGACY_SCOPE_RSID = "manugrs"
LEGACY_SCOPE_URL_LIKE = [
    "%manulifeim.com/group-retirement/ca/en%",   # EN legacy root (shipped-equivalent)
]

# --- Target (parameterized) ---
CATALOG_PLACEHOLDER = "__SET_ME__"
BRONZE_SCHEMA = "gmai_pulse_bronze"
SILVER_SCHEMA = "gmai_pulse_silver"
GOLD_SCHEMA = "gmai_pulse_gold"

BRONZE_TABLE = "adobe_hit_gwam_ca_ret"
SILVER_TABLE = "hits_conformed"
GOLD_KPI_TABLE = "kpi_daily"
GOLD_ANOMALIES_TABLE = "anomalies"
GOLD_RUNMETA_TABLE = "run_meta"

# UC Volume (in the gold schema) used as a scratch path for the driver-side detector input.
SCRATCH_VOLUME = "scratch"
SCRATCH_SUBDIR = "detect_input"

# --- Incremental / windowing ---
OVERLAP_DAYS = 3               # reprocess trailing N days to absorb late micro-batches
BACKFILL_START = "2026-02-01"  # first date with real data (EDA)

# --- Identity / privacy (ADR-0007) ---
HMAC_SECRET_SCOPE = "gmai_pulse"
HMAC_SECRET_KEY = "identity_hmac_key"
# Identity fields pseudonymized (deterministically -> distinct counts preserved) at Silver.
IDENTITY_COLS = ["mcvisid", "post_visid_high", "post_visid_low"]

# --- Detector ---
DETECT_METHOD = "ecod"
DETECT_SEED = 7
DOMAIN = "gwam_retirement"


class Settings:
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

    @property
    def gold_anomalies(self):
        return f"{self.catalog}.{GOLD_SCHEMA}.{GOLD_ANOMALIES_TABLE}"

    @property
    def gold_runmeta(self):
        return f"{self.catalog}.{GOLD_SCHEMA}.{GOLD_RUNMETA_TABLE}"

    @property
    def scratch_dir(self):
        return f"/Volumes/{self.catalog}/{GOLD_SCHEMA}/{SCRATCH_VOLUME}/{SCRATCH_SUBDIR}"

    def __repr__(self):
        return (f"Settings(catalog={self.catalog!r}, mode={self.mode!r}, "
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
    """Build Settings from job parameters / notebook widgets.

    Job-level parameters surface to notebooks as widgets, so target_catalog / mode /
    start_date / repo_root are all read the same way. Fails fast if target_catalog is
    still the placeholder (ADR-0006: one governed compute plane, explicit catalog).
    """
    catalog = _widget(dbutils, "target_catalog", CATALOG_PLACEHOLDER)
    mode = _widget(dbutils, "mode", "incremental")
    start_date = _widget(dbutils, "start_date", BACKFILL_START)
    repo_root = _widget(dbutils, "repo_root", "")

    s = Settings(catalog, mode=mode, start_date=start_date, repo_root=(repo_root or None))
    if s.catalog == CATALOG_PLACEHOLDER:
        raise ValueError(
            "target_catalog is unset. Set the 'target_catalog' job parameter (or notebook "
            f"widget) to a writable Unity Catalog; schemas {BRONZE_SCHEMA}/{SILVER_SCHEMA}/"
            f"{GOLD_SCHEMA} are created there."
        )
    return s
