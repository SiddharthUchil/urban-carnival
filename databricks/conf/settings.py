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

# GWAM Canada-Retirement subset: rsid + retirement URL. EDA: 1,151,474 hits, 157 days
# -- that is the en_only figure, NOT the suite total (the 2026-07-20 inventory measured
# 8,412,803 unfiltered hits on this rsid; en_only captures 31.8% of them).
SCOPE_RSID = "manulifeglobalprod"

# URL scope mode. "en_only" reproduces the shipped population exactly (single English
# section root, applied to post_page_url). "broad" widens to a language-agnostic,
# multi-domain retirement scope.
#
# KEEP "en_only". The 2026-07-20 scope inventory satisfied the first half of the old
# gate (excluded volume is now quantified: doc-16 §1 D3) but two conditions remain --
# the widened population has not been re-profiled (phase P2), and "%/group-plans%"
# still needs the product sign-off noted below. Flipping re-baselines every downstream
# KPI, detector threshold, and injected-anomaly calibration; done under
# mode=incremental it also writes a step change mid-series that the detector reads as
# a level-shift anomaly. Any flip must be a full mode=backfill with gold truncated.
SCOPE_URL_MODE = "en_only"

# Current production scope -- English section root only.
SCOPE_URL_LIKE = "%manulife.com/ca/en/personal/group-plans/group-retirement%"

# Proposed broad scope: SQL LIKE patterns OR-ed together, matched case-insensitively on the
# COMPLETE url (coalesce(page_url, post_page_url) -- post_page_url measured 36.41% blank on
# this suite and 45.75% on manugrs, vs <=0.013% for page_url; 2026-07-20 inventory).
# NOTE group-plans is the umbrella that CONTAINS group-retirement (also pulls in
# group-benefits / business / advisor), so product sign-off is needed before activating.
#
# These patterns are already language-agnostic: the 2026-07-20 inventory confirmed they
# cover every French path it found -- manulifeim.com/group-retirement/ca/fr/* via
# "%/group-retirement%" and /ca/fr/particuliers/regimes-collectifs/retraite-collective via
# "%/regimes-collectifs%". No FR-specific additions are needed here. Known NOT covered:
# epargnemanuvie.ca (separate FR brand domain, blocked on the SCOPE_LOGIN_HOST_EXCLUDE
# ruling below) and the unhyphenated "groupretirement" portal paths (login, excluded by design).
SCOPE_URL_LIKE_BROAD = [
    "%/group-retirement%",     # EN retirement subsection + pagename section token; also FR /ca/fr
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
# scope inventory run (doc-16 §1 D8; notebook retired, output in git history at 408de5a);
# candidates NOT yet ruled on:
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
# ~2.5 yr of marketing-site history (research/claude/14-manugrs-cross-suite-analysis.md).
#
# The "2026-02-01 clean cutover" holds only for the URL-FILTERED marketing population: the
# 2026-07-20 inventory measured manugrs still running 8-13M hits/month through 2026-07-19 at
# suite level, in parallel with manulifeglobalprod. Only the manulifeim.com marketing site
# wound down. So "with_legacy" is a union of two CONCURRENT suites, not a history splice.
#
# KEEP "current_only" until the business signs off unioning the legacy suite -- flipping this
# re-baselines every downstream KPI, and only ~12 eVars are shared across the two suites
# (eVar-derived series are NOT splice-safe).
SCOPE_SUITE_MODE = "current_only"

# Legacy CA-Retirement suite (pre-Storefront, old site manulifeim.com/group-retirement/ca).
# Matched on the COMPLETE url (coalesce(page_url, post_page_url)) because post_page_url
# measured 45.75% blank on this suite (2026-07-20 inventory; the earlier ~48% was sampled).
# The FR root is included: the inventory found manulifeim.com/group-retirement/ca/fr to be the
# single largest uncovered retirement prefix (801,461 hits). Inert while SCOPE_SUITE_MODE is
# "current_only" -- that branch is dead code -- so this carries no re-baseline risk today.
LEGACY_SCOPE_RSID = "manugrs"
LEGACY_SCOPE_URL_LIKE = [
    "%manulifeim.com/group-retirement/ca/en%",   # EN legacy root (shipped-equivalent)
    "%manulifeim.com/group-retirement/ca/fr%",   # FR legacy root -- 801,461 hits, 2026-07-20 inventory
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
