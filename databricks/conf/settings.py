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
SCOPE_URL_LIKE = "%manulife.com/ca/en/personal/group-plans/group-retirement%"

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
