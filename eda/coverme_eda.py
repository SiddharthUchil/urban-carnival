# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — CoverMe EDA
# MAGIC
# MAGIC **Purpose.** Read-only exploratory profiling of the Adobe Analytics hit-level table
# MAGIC (`csdo_prod_catalog.adobe_coverme_bronze.hit_data`) for the CoverMe product suite, to:
# MAGIC 1. Fill evidence gaps: volume, history depth, load cadence, schema population census.
# MAGIC 2. Discover real metric candidates (post_event_list event IDs, live eVars/props).
# MAGIC 3. Capture time-series shape (seasonality, volatility) for anomaly-model design.
# MAGIC 4. Produce a machine-readable **synthesis spec** for generating synthetic data.
# MAGIC
# MAGIC This is the CoverMe analogue of `gwam_canada_retirement_eda.py` — same S0–S12 spine and
# MAGIC SHAREABLE/emit protocol — with three data-shape deltas established by the two CoverMe
# MAGIC probe notebooks (`coverme_discovery_probe`, `coverme_url_scope_inventory`):
# MAGIC
# MAGIC **Scope is URL-only (single-suite).** This table is a single Adobe report suite — Adobe
# MAGIC pins the suite in feed config, so there is NO `rsid` column on the row. CoverMe is scoped
# MAGIC by URL alone. Default `url_scope_mode=broad` uses the `url_scope_list` widget verbatim,
# MAGIC seeded to the three production hosts that carry ~99.9% of real CoverMe traffic:
# MAGIC `%coverme.com%` (EN), `%pourmeproteger.com%` (FR), `%insttrip.manulife.com%` (EN travel,
# MAGIC dead after 2024-03-11). `url_scope_exclude` drops UAT / AEM-authoring / staging noise.
# MAGIC
# MAGIC **URL coalesce is INVERTED vs GWAM — `page_url` FIRST.** On this table `page_url` is
# MAGIC 0.0005% blank while `post_page_url` is 58.9% blank (the exact opposite of GWAM). The D4
# MAGIC blank-guarded coalesce order is `page_url, visit_start_page_url, first_hit_page_url,
# MAGIC post_page_url`. Adobe writes empty strings, not NULLs, so blanks map to NULL first.
# MAGIC
# MAGIC **Language is split by DOMAIN, not path** (~50/50): coverme.com/insttrip = EN,
# MAGIC pourmeproteger = FR. The funnel of interest is the quote→application conversion path
# MAGIC (Quote Start → Quote Complete → Save Quote → App Start → App Confirm), profiled by name
# MAGIC in **S6b**.
# MAGIC
# MAGIC **Data visibility (ADR-0007 §5, full-raw).** EVERY column profiles raw and in full —
# MAGIC eVars, props, events, URLs, pagenames, campaigns, referrers, AND the direct/quasi-identifier
# MAGIC set. There is no shape-only carve-out. URL query strings profile raw by default (the
# MAGIC `strip_url_query` widget strips them). ⚠ SHAREABLE blocks may carry raw identifiers/PII — a
# MAGIC human read-through is required before any block leaves the governed workspace.
# MAGIC
# MAGIC **How to run.** Databricks → Workspace → Import → File → select this `.py` (it imports as a
# MAGIC notebook). Attach any Unity-Catalog cluster (DBR 13+); heavy sections run on a 5% sample.
# MAGIC Run the **S0 config cell** once so widgets appear, then **Run All**. Each section prints a
# MAGIC `===== BEGIN SHAREABLE: <id> =====` block — copy those verbatim (multi-part blocks
# MAGIC reassemble by concatenation). A failure prints `===== SKIPPED: <id> | <reason> =====` and
# MAGIC the run continues.

# COMMAND ----------

# MAGIC %md
# MAGIC ## S0 — Config, constants, helpers

# COMMAND ----------

import json
import re
import math
import hashlib
import datetime
import traceback

from pyspark.sql import functions as F

# ---------------------------------------------------------------- widgets ----
dbutils.widgets.text("table_fqn", "csdo_prod_catalog.adobe_coverme_bronze.hit_data", "1. Table (catalog.schema.table)")
dbutils.widgets.text("window_months", "13", "2. Deep-profiling window (months)")
dbutils.widgets.text("sample_fraction", "0.05", "3. Sample fraction for per-column stats")
dbutils.widgets.text("col_batch_size", "150", "4. Columns per aggregation batch")
dbutils.widgets.text("top_n", "25", "5. Top-N cap for value lists")
dbutils.widgets.text("hourly_days", "35", "6. Days for hourly profile")
dbutils.widgets.text("max_csv_lines", "450", "7. Max CSV lines per shareable block")
dbutils.widgets.text("top_events_k", "12", "8. Top-K events for daily series")
dbutils.widgets.text("cache_sample", "false", "9. Persist sample df (true/false)")
dbutils.widgets.dropdown("url_scope_mode", "broad", ["broad", "tight"], "10. URL scope mode (tight = coverme.com only)")
dbutils.widgets.text("url_scope_list", "%coverme.com%,%pourmeproteger.com%,%insttrip.manulife.com%",
                     "11. URL include patterns — ADD URLS HERE (SQL LIKE, comma-sep)")
dbutils.widgets.text("url_scope_exclude",
                     "%adobeaemcloud.com%,%author-aem-prod.manulife.ca%,%uat.coverme.com%,"
                     "%uat.pourmeproteger.com%,%.uat.%,%www-aem-stage%,%localhost:5000%",
                     "12. URL patterns to exclude (UAT/AEM/staging noise)")
dbutils.widgets.text("max_profiled_cols", "1200", "13. Max columns emitted with full stats")
dbutils.widgets.dropdown("strip_url_query", "false", ["false", "true"], "14. Strip URL query strings before profiling")

TABLE_FQN       = dbutils.widgets.get("table_fqn").strip()
WINDOW_MONTHS   = int(dbutils.widgets.get("window_months"))
SAMPLE_FRACTION = float(dbutils.widgets.get("sample_fraction"))
COL_BATCH_SIZE  = int(dbutils.widgets.get("col_batch_size"))
TOP_N           = int(dbutils.widgets.get("top_n"))
HOURLY_DAYS     = int(dbutils.widgets.get("hourly_days"))
MAX_CSV_LINES   = int(dbutils.widgets.get("max_csv_lines"))
TOP_EVENTS_K    = int(dbutils.widgets.get("top_events_k"))
CACHE_SAMPLE    = dbutils.widgets.get("cache_sample").strip().lower() == "true"
MAX_PROFILED_COLS = int(dbutils.widgets.get("max_profiled_cols"))
STRIP_URL_QUERY   = dbutils.widgets.get("strip_url_query").strip().lower() == "true"

def _csv(widget):
    return [p.strip().lower() for p in dbutils.widgets.get(widget).split(",") if p.strip()]

URL_SCOPE_MODE = dbutils.widgets.get("url_scope_mode").strip().lower()
URL_EXCLUDE    = _csv("url_scope_exclude")

# Scope modes. The `url_scope_list` widget is AUTHORITATIVE in `broad`: whatever patterns are
# visible there are the patterns that run, so adding a URL means editing that widget and nothing
# else. `tight` is the single override — coverme.com only (English-dominant signal isolation).
# The default broad list is the "medium" tier from the URL-scope inventory: the three production
# hosts that carry ~99.9% of real CoverMe traffic. insttrip.manulife.com went dead 2024-03-11 —
# kept for historical completeness; drop it here for a cleaner live series at detector time.
URL_SCOPE_TIGHT = ["%coverme.com%"]
URL_INCLUDE = URL_SCOPE_TIGHT if URL_SCOPE_MODE == "tight" else _csv("url_scope_list")

# CoverMe product-surface regex (parity with the probe notebooks' S4c). Used only for the S4b/S4c
# scope-coverage audit, never as a hard filter.
CM_STRICT = (r"health-insurance|assurance-sante|travel-insurance|assurance-voyage"
             r"|life-insurance|assurance-vie|my-next-chapter|vitality|/covme/health-insurance")
CM_BROAD  = r"insurance|assurance|coverme|covme|manulife|manuvie|pourmeproteger"

# -------------------------------------------------------- privacy stance ------
# ADR-0007 §5 full-raw. EDA runs inside the governed Databricks workspace. Every column profiles
# and emits RAW values; there is NO shape-only carve-out. This changes NOTHING about the shipped
# pipeline's pseudonymization — only what the EDA notebook PRINTS.
PII_EXPORT_WARNING = (
    "PII NOTICE (ADR-0007 §5): SHAREABLE blocks below may carry RAW identifiers and PII "
    "(IPs, postal codes, device IDs, User-Agent, tracking eVars). They may leave the "
    "governed Databricks workspace only after a human read-through."
)
print("\n" + "=" * 78 + "\n" + PII_EXPORT_WARNING + "\n" + "=" * 78 + "\n")

# --------------------------------------------------------- semantic labels ----
# Built from CoverMeDataMap.xlsx — the Enabled rows of the post_eVar / post_prop / post_event_list
# tabs. Keyed by variable number; applies to both `evarN` and `post_evarN`. Business flagged
# eVars 4,5,6,11,16,52,111,148 and the quote/app funnel events for Anomaly Detection.
EVAR_LABELS = {
    1: "Time Stamp", 2: "New/Repeat Visitors", 3: "Visit Number", 4: "Product Category",
    5: "Product ID", 6: "Sponsor/Distributor/Association", 7: "Sub-Line of Business",
    8: "Language", 9: "Province", 10: "Policy Number - Merchandising", 11: "Quote Session ID",
    12: "Recipient ID", 13: "Message ID", 14: "User ID", 16: "Transaction ID",
    22: "Line of Business", 23: "Content Name", 24: "Content Category", 25: "Content Type",
    29: "Search Term", 30: "Search Number of Results", 32: "Video Title and ID",
    33: "Current URL", 34: "Download File Type", 35: "Document Link Label",
    36: "Document File Name", 37: "Exit Link", 40: "Experience ID and Visit Number",
    41: "User Sub-Type", 42: "Export Variable", 43: "Plan Type",
    44: "Organic Landing Page Name", 45: "Organic Landing Page URL", 49: "Reason for quote",
    51: "Applicant Profile - Merchandising", 52: "Current Page", 54: "Current Domain",
    55: "Internal Promotion", 56: "Coverage End Date (FollowMe)",
    59: "Sponsor/Distribution/Association Group", 61: "Event Category", 62: "Event Action",
    63: "Event Label", 64: "Event Detail", 65: "OneTrust Consent Model (v65)",
    66: "Referrer 2.0", 67: "Previous Page Name", 68: "Previous Page URL", 70: "SCID",
    71: "UTM_Source", 73: "Revenue (Offline)", 74: "Trip Start Date (Travel Only) (Offline)",
    75: "Trip End date (Travel Only) (Offline)", 76: "Sales Channel", 77: "Traveller Type",
    78: "Policy Status", 79: "Policy Name", 80: "Chat Category", 81: "OneTrust Consent ID",
    84: "Visitor Province (Offline)", 85: "Search Theme", 88: "Quote Applicant Profile",
    99: "Vitality Health Goals Response", 101: "Family Coverage (Travel)",
    102: "Super Visa (Travel)", 103: "Traveller Count", 104: "Trip Duration",
    105: "Trip Destination", 106: "Travel Add-on", 111: "Experience Cloud ID",
    115: "User Agent", 116: "Bot Traffic", 118: "Full URL", 121: "Hashed Email ID",
    130: "Name", 131: "Type", 132: "SiteType", 133: "SiteSection", 134: "ContentType",
    135: "Brand", 136: "Custom eVar 136", 137: "Segment", 148: "Bot Detector", 149: "Language",
    150: "Page Name", 166: "Page URL", 172: "UserID", 173: "Customer ID", 183: "User Type",
    193: "Click Name", 194: "Click Href", 199: "Google ID - v199",
    200: "OneTrust ID | Categories ID",
}
PROP_LABELS = {
    1: "Product Category", 2: "Product ID", 3: "Sponsor/Distributor/Association",
    4: "Sub-Line of Business", 5: "Language", 6: "Province", 8: "Current URL",
    9: "Previous Page", 10: "Time Stamp", 11: "Total Percent Viewed", 18: "Page Type",
    19: "Current Page", 23: "Line of Business", 24: "Content Name", 25: "Content Category",
    26: "Content Type", 30: "Search Term", 32: "Navigational Element",
    33: "Video Title and ID", 35: "Download File Type", 36: "Download Link Label",
    37: "Download File Name", 40: "Social Media Type", 47: "Current Domain",
    50: "Sponsor/Distributor/Association Group", 51: "Internal Navigation Link",
}
_VAR_RE = re.compile(r"(?:post_)?(evar|prop)(\d+)$")
def dim_label(col):
    """Semantic label for evarN/propN columns; '' when unknown."""
    m = _VAR_RE.match(str(col).lower())
    if not m:
        return ""
    n = int(m.group(2))
    return (EVAR_LABELS if m.group(1) == "evar" else PROP_LABELS).get(n, "")

# post_event_list labels — Enabled non-"Instance of eVar" events from the data map. The
# Instance-of-eVar events (ids 100-199, 10000-10099) are resolved by the decode_event() formula.
EVENT_LABELS = {
    "203": "Exits", "204": "Downloads", "207": "Scroll Event", "211": "Custom Event 12",
    "217": "Custom Event 18", "218": "Internal Search Results",
    "219": "Internal Search Attempts", "225": "Video: Begin", "226": "Video: 50% Viewed",
    "227": "Video: 100% Viewed", "228": "Quote Start", "229": "Quote Complete",
    "230": "Online Quote Step 1.1", "231": "Pre-App Step", "232": "Save Quote",
    "233": "Emails Sent", "234": "Emails Delivered", "235": "Emails Opened",
    "236": "Emails Clicked", "237": "Emails Unsubscribed", "238": "Emails Total Bounces",
    "239": "Quote Confirm", "240": "App Confirm", "241": "Conductor Denominator",
    "244": "CTA Clicks", "249": "Banner Click", "256": "Banner/Promo Impressions",
    "257": "Banner/Promo Clicks", "260": "Product Views", "269": "App Start",
    "270": "Online Application Step 2", "271": "Online Application Step 3",
    "272": "Online Application Step 4", "281": "Monthly Premium", "282": "Annual Premium",
    "283": "Premium (Offline)", "284": "Trip Days (Offline)",
    "285": "Online Quote Step 3 - Vitality", "286": "Total Quote Price",
    "287": "Live Chat Click", "288": "Live Chat Form Start", "289": "Live Chat Form Submit",
    "292": "TF Chat Click", "293": "TF: Close Live Chat", "294": "Feedback Click",
    "295": "Transaction Error", "296": "eSign start", "297": "eSign complete",
    "20100": "Page Interactive Time", "20159": "Link Click", "20300": "Modal Views",
    "20301": "Modal - details entered (phone number)", "20302": "Modal Skipped",
    "20350": "Premium (Offline)", "20351": "Trip Days (Offline)",
}
ADOBE_STD_EVENTS = {
    "1": "purchase", "2": "product_view", "10": "cart_open", "11": "checkout",
    "12": "cart_add", "13": "cart_remove", "14": "cart_view", "20": "campaign_view",
}

# The quote -> application conversion funnel (business-flagged for Anomaly Detection), in
# LOGICAL step order. Profiled by name in S6b; a low daily count here is a KPI worth watching,
# not a reason to drop it.
FUNNEL_EVENTS = [
    ("228", "Quote Start"), ("229", "Quote Complete"), ("232", "Save Quote"),
    ("269", "App Start"), ("240", "App Confirm"),
]

# ------------------------------------------------------------ emit helpers ----
RESULTS = {}   # section_id -> payload (drives S12 consolidation)
SKIPPED = {}   # section_id -> reason
MAX_EMIT_STR = 2000

def _scrub_str(s):
    if len(s) > MAX_EMIT_STR:
        s = s[:MAX_EMIT_STR] + "...<trunc>"
    return s

def _scrub(obj):
    """Walk a payload: truncate over-long strings and round floats. No PII redaction."""
    if isinstance(obj, dict):
        return {(_scrub_str(k) if isinstance(k, str) else k): _scrub(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub(v) for v in obj]
    if isinstance(obj, str):
        return _scrub_str(obj)
    if isinstance(obj, float):
        return round(obj, 4) if math.isfinite(obj) else None
    return obj

def emit(section_id, payload):
    """Single output chokepoint: every shareable output goes through here."""
    payload = _scrub(payload)
    RESULTS[section_id] = payload
    body = json.dumps(payload, separators=(",", ":"), default=str)
    print(f"===== BEGIN SHAREABLE: {section_id} =====")
    if len(body) <= 48000:
        print(body)
    else:
        n_parts = math.ceil(len(body) / 40000)
        for i in range(n_parts):
            print(f"----- part {i+1} of {n_parts} (concatenate parts to reassemble) -----")
            print(body[i * 40000:(i + 1) * 40000])
    print(f"===== END SHAREABLE: {section_id} =====")

def run_section(section_id, fn):
    print(f"\n>>> running {section_id} ...")
    t0 = datetime.datetime.now()
    try:
        fn()
        print(f">>> {section_id} done in {(datetime.datetime.now() - t0).total_seconds():.0f}s")
    except Exception as e:
        reason = f"{type(e).__name__}: {str(e)[:300]}"
        SKIPPED[section_id] = reason
        print(f"===== SKIPPED: {section_id} | {reason} =====")
        traceback.print_exc()

# ------------------------------------------------------------ data helpers ----
def qcol(col_name):
    """F.col with backtick quoting — Adobe schemas carry dotted column names."""
    return F.col("`" + col_name.replace("`", "``") + "`")

def nonblank(col_name):
    """Adobe feeds use empty strings, not NULLs."""
    c = qcol(col_name)
    return c.isNotNull() & (F.trim(c.cast("string")) != "")

def strip_query(u):
    return str(u).split("?")[0].split("#")[0]

def maybe_strip(u):
    return strip_query(u) if STRIP_URL_QUERY else str(u)

def batched_agg(df, agg_exprs, batch_size):
    """Run many agg expressions in batches to dodge codegen limits.
    agg_exprs: list of (alias, Column). Returns {alias: value}."""
    out = {}
    for i in range(0, len(agg_exprs), batch_size):
        batch = agg_exprs[i:i + batch_size]
        exprs = [c.alias(a) for a, c in batch]
        try:
            row = df.agg(*exprs).collect()[0]
        except Exception:
            spark.conf.set("spark.sql.codegen.wholeStage", "false")
            try:
                row = df.agg(*exprs).collect()[0]
            finally:
                spark.conf.set("spark.sql.codegen.wholeStage", "true")
        out.update(row.asDict())
    return out

def pick_col(df, *candidates):
    """First candidate column present in the schema, else None."""
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def resolve_ts_expr(df):
    """Fallback chain for the canonical hit timestamp (for hour-of-day). Returns (Column, desc)."""
    dtypes = dict(df.dtypes)
    if "date_time" in dtypes:
        if dtypes["date_time"] in ("timestamp", "date"):
            return F.col("date_time"), "date_time (typed)"
        return F.to_timestamp(F.col("date_time")), "to_timestamp(date_time)"
    if "hit_time_gmt" in dtypes:
        return F.from_unixtime(F.col("hit_time_gmt").cast("long")).cast("timestamp"), "from_unixtime(hit_time_gmt)"
    raise ValueError("No usable timestamp column (date_time / hit_time_gmt) found")

# --- CoverMe scope: URL-only (single-suite). Resolved once against the schema. ------------------
# D4 coalesce order is INVERTED vs GWAM — page_url FIRST (0.0005% blank vs 58.9% for post_page_url).
URL_CANDIDATES = ("page_url", "visit_start_page_url", "first_hit_page_url", "post_page_url")
URL_COLS = None       # present URL candidates, in coalesce order
URL_COL  = None       # lead URL column
PARTITION_COL = None  # hit_date (typed date) when present — used for pruning + daily grouping

def _resolve_scope_cols(df):
    global URL_COLS, URL_COL, PARTITION_COL
    have = set(df.columns)
    URL_COLS = [c for c in URL_CANDIDATES if c in have]
    URL_COL = URL_COLS[0] if URL_COLS else None
    PARTITION_COL = pick_col(df, "hit_date")

def day_expr(df):
    """Calendar-date column for daily grouping + window filtering. Prefers the typed-date
    partition column `hit_date` (so predicates prune partitions); falls back to to_date(ts)."""
    if PARTITION_COL is None:
        _resolve_scope_cols(df)
    if PARTITION_COL:
        return F.col(PARTITION_COL)
    ts, _ = resolve_ts_expr(df)
    return F.to_date(ts)

def window_pred(df, start_date):
    """Dtype-aware >= predicate on the partition/day column (date cast prunes when typed date)."""
    return day_expr(df) >= F.lit(str(start_date)).cast("date")

def like_any(colexpr, patterns):
    """Null-safe OR of SQL LIKE patterns; None when `patterns` is empty. Blank/NULL input yields
    False, never NULL, so ~like_any(...) keeps rather than silently drops the row."""
    if not patterns:
        return None
    m = None
    for p in patterns:
        m = colexpr.like(p) if m is None else (m | colexpr.like(p))
    return F.coalesce(m, F.lit(False))

def url_expr(df):
    """D4 blank-guarded coalesce over URL_CANDIDATES (page_url first), lowercased. Adobe writes
    empty strings, not NULLs, so map blank -> NULL before the coalesce, then land on '' so the
    NOT LIKE exclusions stay well-defined."""
    if URL_COLS is None:
        _resolve_scope_cols(df)
    if not URL_COLS:
        return None
    parts = [F.when(F.trim(F.col(c).cast("string")) != F.lit(""), F.trim(F.col(c).cast("string")))
             for c in URL_COLS]
    return F.lower(F.coalesce(*parts, F.lit("")))

def scope_condition(df):
    """CoverMe subset selector — URL-only. Returns (Column|None, meta). The coalesced URL matches
    any URL_INCLUDE pattern AND matches none of URL_EXCLUDE. Empty include -> no URL filter."""
    if URL_COLS is None:
        _resolve_scope_cols(df)
    conds, active, missing = [], [], []
    u = url_expr(df)
    if u is None:
        if URL_INCLUDE or URL_EXCLUDE:
            missing.append("url (no page_url/visit_start_page_url/first_hit_page_url/post_page_url column)")
    else:
        inc = like_any(u, URL_INCLUDE)
        if inc is not None:
            conds.append(inc)
            active.append(f"url[coalesce{tuple(URL_COLS)}] LIKE any {URL_INCLUDE}")
        exc = like_any(u, URL_EXCLUDE)
        if exc is not None:
            conds.append(~exc)
            active.append(f"url NOT LIKE any {URL_EXCLUDE}")
    cond = None
    for c in conds:
        cond = c if cond is None else (cond & c)
    meta = {"single_suite": True, "rsid_col": None, "url_col": URL_COL,
            "url_cols_coalesced": URL_COLS, "url_scope_mode": URL_SCOPE_MODE,
            "url_include": URL_INCLUDE or None, "url_exclude": URL_EXCLUDE or None,
            "partition_col": PARTITION_COL,
            "active_conditions": active, "missing_conditions": missing,
            "scoped": cond is not None}
    return cond, meta

# Globals populated by S1/S3/S4; ensure_frames() rebuilds them for re-runs.
DF = None
DF_CM = None
DF_W = None
DF_S = None
TS_EXPR = None
TS_EXPR_DESC = None
WINDOW_START = None
WINDOW_END = None
SAMPLE_ROWS = None

def ensure_frames():
    """Make DF/DF_CM/DF_W/DF_S available even when a section is re-run standalone.
    DF = full table (S1/S2/S3). DF_CM = CoverMe URL-scoped subset; DF_W/DF_S derive from it."""
    global DF, DF_CM, DF_W, DF_S, TS_EXPR, TS_EXPR_DESC, WINDOW_START, WINDOW_END, SAMPLE_ROWS
    if DF is None:
        DF = spark.table(TABLE_FQN)
    if URL_COLS is None:
        _resolve_scope_cols(DF)
    if TS_EXPR is None:
        TS_EXPR, TS_EXPR_DESC = resolve_ts_expr(DF)
    if DF_CM is None:
        cond, _ = scope_condition(DF)
        DF_CM = DF.filter(cond) if cond is not None else DF
    if DF_W is None:
        if WINDOW_END is None:
            dv = RESULTS.get("daily_volume", {})
            WINDOW_END = datetime.date.fromisoformat(dv["cm_date_max"]) if dv.get("cm_date_max") else datetime.date.today()
        WINDOW_START = (WINDOW_END.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
        for _ in range(WINDOW_MONTHS - 1):
            WINDOW_START = (WINDOW_START - datetime.timedelta(days=1)).replace(day=1)
        DF_W = DF_CM.filter(window_pred(DF_CM, WINDOW_START))
    if DF_S is None:
        DF_S = DF_W.sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=42)
        DF_S = DF_S.persist()
        SAMPLE_ROWS = DF_S.count()
    return DF, DF_W, DF_S

print(f"Config OK. table={TABLE_FQN} window={WINDOW_MONTHS}mo fraction={SAMPLE_FRACTION} "
      f"batch={COL_BATCH_SIZE} top_n={TOP_N} max_cols={MAX_PROFILED_COLS} emit_mode=raw-all")
print(f"Scope (URL-only, single-suite): url_mode={URL_SCOPE_MODE} "
      f"include={URL_INCLUDE or '(off)'} exclude={URL_EXCLUDE or '(off)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## S1 — Unity Catalog discovery
# MAGIC Finds candidate adobe/hit/clickstream tables and verifies the configured table resolves.
# MAGIC Confirms the table is single-suite (no rsid column). Metadata-only; runs in seconds.

# COMMAND ----------

def s1_discovery():
    global DF
    candidates = []
    try:
        rows = spark.sql("""
            SELECT table_catalog, table_schema, table_name
            FROM system.information_schema.tables
            WHERE lower(table_name) RLIKE 'adobe|hit|clickstream|coverme'
              AND table_schema <> 'information_schema'
            LIMIT 100
        """).collect()
        for r in rows:
            fqn = f"{r.table_catalog}.{r.table_schema}.{r.table_name}"
            n_cols = None
            try:
                n_cols = spark.sql(f"""
                    SELECT count(*) AS n FROM system.information_schema.columns
                    WHERE table_catalog = '{r.table_catalog}'
                      AND table_schema  = '{r.table_schema}'
                      AND table_name    = '{r.table_name}'
                """).collect()[0].n
            except Exception:
                pass
            candidates.append({"fqn": fqn, "n_cols": n_cols})
    except Exception as e:
        print(f"information_schema unavailable ({type(e).__name__}); skipping catalog scan")

    resolves, n_cols_chosen, err, scope_meta = False, None, None, None
    rsid_col = None
    try:
        DF = spark.table(TABLE_FQN)
        n_cols_chosen = len(DF.columns)
        resolves = True
        _resolve_scope_cols(DF)
        rsid_col = pick_col(DF, "rsid", "report_suite", "reportsuite", "reportsuiteid", "post_rsid")
        _, scope_meta = scope_condition(DF)
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)[:200]}"

    emit("uc_discovery", {
        "configured_table": TABLE_FQN,
        "resolves": resolves,
        "n_cols": n_cols_chosen,
        "resolve_error": err,
        "rsid_col": rsid_col,
        "single_suite": rsid_col is None,
        "url_cols_present": URL_COLS,
        "partition_col": PARTITION_COL,
        "candidates": candidates[:30],
        "scope": scope_meta,
        "note": ("single_suite=true is expected — Adobe pins the report suite in feed config, so "
                 "there is no rsid column on the row. CoverMe scope is URL-only. If resolves=false, "
                 "set the table_fqn widget to one of the candidates and re-run."),
    })

run_section("S1", s1_discovery)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S2 — Delta metadata & load cadence
# MAGIC `DESCRIBE DETAIL` + `DESCRIBE HISTORY`: freshness/arrival-cadence evidence with zero data scan.

# COMMAND ----------

def s2_delta_meta():
    detail = spark.sql(f"DESCRIBE DETAIL {TABLE_FQN}").collect()[0].asDict()
    detail_safe = {k: detail.get(k) for k in
                   ["format", "numFiles", "sizeInBytes", "partitionColumns",
                    "clusteringColumns", "createdAt", "lastModified"]}
    writes = {"available": False}
    try:
        hist = spark.sql(f"DESCRIBE HISTORY {TABLE_FQN} LIMIT 100").collect()
        write_ops = [h for h in hist if h.operation and
                     any(k in h.operation.upper() for k in ["WRITE", "MERGE", "UPDATE", "COPY", "REPLACE"])]
        ts = sorted([h.timestamp for h in write_ops])
        gaps_h = sorted((b - a).total_seconds() / 3600 for a, b in zip(ts, ts[1:]))
        recent = []
        for h in write_ops[:20]:
            om = h.operationMetrics or {}
            rows_written = om.get("numOutputRows") or om.get("numTargetRowsInserted")
            recent.append({"ts": str(h.timestamp), "op": h.operation, "rows": rows_written})
        ops_by_type = {}
        for h in hist:
            ops_by_type[h.operation] = ops_by_type.get(h.operation, 0) + 1
        writes = {
            "available": True, "n_history_rows": len(hist), "ops_by_type": ops_by_type,
            "n_write_ops": len(write_ops),
            "median_interarrival_hours": gaps_h[len(gaps_h) // 2] if gaps_h else None,
            "min_gap_hours": gaps_h[0] if gaps_h else None,
            "max_gap_hours": gaps_h[-1] if gaps_h else None,
            "recent_writes": recent,
        }
    except Exception as e:
        writes = {"available": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}
    emit("delta_meta", {"detail": detail_safe, "writes": writes})

run_section("S2", s2_delta_meta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S3 — Full-range daily volume (exact)
# MAGIC The one full-table scan (narrow projection): daily row counts over ALL history →
# MAGIC history depth, missing days, day-of-week profile, monthly totals, biggest day-over-day
# MAGIC jumps. Grouped by the `hit_date` partition column so the scan stays cheap.

# COMMAND ----------

DAILY_ROWS = []        # [(date, cm_count)] — the CoverMe subset series; drives S4/S8/S10
DAILY_TOTAL_ROWS = []  # [(date, total_count)] — whole-table series (chart/context)

def s3_daily_volume():
    global DF, TS_EXPR, TS_EXPR_DESC, WINDOW_END, DAILY_ROWS, DAILY_TOTAL_ROWS
    if DF is None:
        DF = spark.table(TABLE_FQN)
    _resolve_scope_cols(DF)
    TS_EXPR, TS_EXPR_DESC = resolve_ts_expr(DF)
    cond, scope_meta = scope_condition(DF)
    cm_expr = F.when(cond, 1).otherwise(0) if cond is not None else F.lit(1)
    _u = url_expr(DF)
    url_blank_expr = F.when(_u == F.lit(""), 1).otherwise(0) if _u is not None else F.lit(0)

    rows = (DF.select(day_expr(DF).alias("d"), cm_expr.alias("cm"), url_blank_expr.alias("urlblank"))
              .groupBy("d")
              .agg(F.count("*").alias("total"), F.sum("cm").alias("cm"),
                   F.sum("urlblank").alias("urlblank"))
              .orderBy("d").collect())
    null_dates = sum(r["total"] for r in rows if r["d"] is None)
    per_day = [(r["d"], r["total"], r["cm"] or 0) for r in rows if r["d"] is not None]
    DAILY_TOTAL_ROWS = [(d, t) for d, t, _ in per_day]
    DAILY_ROWS = [(d, cm) for d, _, cm in per_day]
    url_blank_total = sum(r["urlblank"] or 0 for r in rows)
    if not per_day:
        emit("daily_volume", {"error": "no non-null dates", "date_expr": TS_EXPR_DESC})
        return

    total_all = sum(t for _, t, _ in per_day)
    total_cm = sum(cm for _, _, cm in per_day)
    cm_daily = [(d, cm) for d, cm in DAILY_ROWS if cm > 0]
    if not cm_daily:
        emit("daily_volume", {
            "error": "scope filter matched 0 rows — check url_scope_* widgets and uc_discovery.scope",
            "date_expr": TS_EXPR_DESC, "scope": scope_meta,
            "total_rows_all": total_all, "url_blank_rows": url_blank_total})
        return
    dmin, dmax = cm_daily[0][0], cm_daily[-1][0]
    WINDOW_END = dmax
    cm_by_date = dict(DAILY_ROWS)

    missing = []
    d = dmin
    while d <= dmax:
        if cm_by_date.get(d, 0) == 0:
            missing.append(str(d))
        d += datetime.timedelta(days=1)

    dow_sum, dow_n = [0] * 7, [0] * 7
    for d, cm in DAILY_ROWS:
        if dmin <= d <= dmax:
            dow_sum[d.weekday()] += cm
            dow_n[d.weekday()] += 1
    dow_mean = [round(dow_sum[i] / dow_n[i]) if dow_n[i] else None for i in range(7)]

    monthly_cm, monthly_total = {}, {}
    for d, t, cm in per_day:
        monthly_cm[d.strftime("%Y-%m")] = monthly_cm.get(d.strftime("%Y-%m"), 0) + cm
        monthly_total[d.strftime("%Y-%m")] = monthly_total.get(d.strftime("%Y-%m"), 0) + t

    jumps = []
    for (d0, c0), (d1, c1) in zip(cm_daily, cm_daily[1:]):
        if c0 > 0 and c1 > 0:
            jumps.append((abs(math.log(c1 / c0)), str(d1), c0, c1))
    jumps.sort(reverse=True)
    top_jumps = [{"date": j[1], "prev": j[2], "curr": j[3], "ratio": round(j[3] / j[2], 3)}
                 for j in jumps[:5]]

    if len(per_day) <= MAX_CSV_LINES:
        csv_daily = [f"{d},{t},{cm}" for d, t, cm in per_day]
        csv_note = "full history, daily"
    else:
        cutoff = (dmax.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
        for _ in range(WINDOW_MONTHS - 1):
            cutoff = (cutoff - datetime.timedelta(days=1)).replace(day=1)
        csv_daily = [f"{d},{t},{cm}" for d, t, cm in per_day if d >= cutoff][:MAX_CSV_LINES]
        csv_note = f"daily since {cutoff}; older history in monthly_totals"

    emit("daily_volume", {
        "date_expr": TS_EXPR_DESC, "day_col": PARTITION_COL or "to_date(ts)", "scope": scope_meta,
        "total_rows_all": total_all, "total_rows_cm": total_cm,
        "cm_share_pct": round(100.0 * total_cm / max(total_all, 1), 3),
        "url_blank_rows": url_blank_total, "null_date_rows": null_dates,
        "cm_date_min": str(dmin), "cm_date_max": str(dmax),
        "n_cm_days_present": len(cm_daily), "n_cm_days_missing": len(missing),
        "missing_days": missing[:50],
        "dow_mean_cm_hits_mon_to_sun": dow_mean,
        "monthly_totals_cm": monthly_cm, "monthly_totals_all": monthly_total,
        "top5_day_over_day_jumps_cm": top_jumps,
        "csv_note": csv_note, "csv_header": "date,total_hits,cm_hits", "csv": csv_daily})

run_section("S3", s3_daily_volume)

# COMMAND ----------

# chart for your own inspection (not part of the shareable output)
if DAILY_ROWS:
    display(spark.createDataFrame(
        [(str(d), t, cm) for (d, t), (_, cm) in zip(DAILY_TOTAL_ROWS, DAILY_ROWS)],
        ["date", "total_hits", "cm_hits"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## S4 — Profiling window + sample frames
# MAGIC Builds `df_w` (last N months of the CoverMe subset) and `df_s` (random sample) used by
# MAGIC S5–S11; cross-checks the window count against S3 and emits filter diagnostics plus a
# MAGIC **host breakdown** (the single-suite analogue of GWAM's rsid breakdown) so a wrong or
# MAGIC single-host scope fails loudly.

# COMMAND ----------

def s4_frames():
    global DF_CM, DF_W, DF_S, SAMPLE_ROWS
    DF_CM = None; DF_W = None; DF_S = None
    ensure_frames()
    window_rows = DF_W.count()
    s3_window_sum = sum(c for d, c in DAILY_ROWS if d >= WINDOW_START) if DAILY_ROWS else None

    cond, scope_meta = scope_condition(DF)
    raw_window = DF.filter(window_pred(DF, WINDOW_START))
    _u = url_expr(DF)
    _inc = like_any(_u, URL_INCLUDE) if _u is not None else None
    url_cond = _inc if _inc is not None else F.lit(True)
    url_blank_cond = F.lit(False) if _u is None else (_u == F.lit(""))
    diag = raw_window.agg(
        F.count("*").alias("total"),
        F.sum(F.when(url_cond, 1).otherwise(0)).alias("url_match"),
        F.sum(F.when(url_blank_cond, 1).otherwise(0)).alias("url_blank"),
    ).collect()[0]

    # host breakdown INSIDE the final scope — single-suite analogue of rsid_breakdown. A scope
    # collapsed to one host (e.g. FR pourmeproteger silently dropped) is the failure this catches.
    host = F.regexp_extract(F.regexp_replace(url_expr(DF_W), r"^[a-z]+://", ""), r"^([^/?#]+)", 1)
    host_breakdown = [{"host": r["host"], "rows": r["count"],
                       "pct_of_scope": round(100.0 * r["count"] / max(window_rows, 1), 3)}
                      for r in (DF_W.select(host.alias("host")).groupBy("host").count()
                                    .orderBy(F.desc("count")).limit(15).collect())]

    warning = None
    if (diag["total"] or 0) > 0 and window_rows == 0:
        warning = ("SCOPE FILTER MATCHED 0 ROWS in the window. Downstream sections would profile "
                   "an empty frame. Check the url_scope_list / url_scope_mode widgets.")
        print("!!!!! " + warning)

    emit("window_frame", {
        "window_start": str(WINDOW_START), "window_end": str(WINDOW_END),
        "window_rows_cm": window_rows, "s3_crosscheck_sum_cm": s3_window_sum,
        "crosscheck_ok": (s3_window_sum == window_rows) if s3_window_sum is not None else None,
        "sample_fraction": SAMPLE_FRACTION, "sample_rows": SAMPLE_ROWS, "sample_cached": CACHE_SAMPLE,
        "filter": {
            **scope_meta,
            "window_total_rows": diag["total"], "url_match": diag["url_match"],
            "host_breakdown": host_breakdown,
            "url_blank_rows": diag["url_blank"],
            "url_blank_pct": round(100.0 * (diag["url_blank"] or 0) / max(diag["total"], 1), 3),
            "warning": warning,
        },
    })

run_section("S4", s4_frames)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S4b — URL scope audit (column choice + coverage)
# MAGIC Ports `coverme_url_scope_inventory` S4a/S5/S7 onto the profiling window: per-URL-column
# MAGIC blank % (confirms `page_url` first), the recommended coalesce, top hosts, and — the number
# MAGIC that matters — CoverMe-looking traffic (`CM_STRICT`) that falls OUTSIDE the current scope.

# COMMAND ----------

def s4b_url_scope_audit():
    ensure_frames()
    _resolve_scope_cols(DF)
    raw_window = DF.filter(window_pred(DF, WINDOW_START))
    present = [c for c in URL_CANDIDATES if pick_col(raw_window, c)]
    if not present:
        emit("url_scope_audit", {"error": "no candidate URL column in source"})
        return

    complete = F.coalesce(*[F.when(nonblank(c), qcol(c)) for c in present])
    u = F.lower(F.trim(complete.cast("string")))
    u = F.regexp_replace(u, r"^[a-z]+://", "")
    hp = F.regexp_extract(u, r"^([^?#]*)", 1)
    host = F.regexp_extract(hp, r"^([^/]+)", 1)

    # per-column blank % + cardinality (headline: page_url ~0%, post_page_url ~59%)
    exprs = [F.count("*").alias("rows")]
    for c in present:
        exprs += [F.sum(F.when(nonblank(c), 1).otherwise(0)).alias(c + "_nb"),
                  F.approx_count_distinct(qcol(c)).alias(c + "_dist")]
    r = raw_window.agg(*exprs).collect()[0]
    total = r["rows"] or 0
    per_col = {c: {"blank_pct": round(100.0 * (total - (r[c + "_nb"] or 0)) / max(total, 1), 3),
                   "approx_distinct": r[c + "_dist"]} for c in present}
    recommended = sorted(present, key=lambda c: per_col[c]["blank_pct"])

    # scope coverage + uncovered CoverMe (CM_STRICT, in scope vs not)
    inc = like_any(hp, URL_INCLUDE)
    in_scope = inc if inc is not None else F.lit(True)
    exc = like_any(hp, URL_EXCLUDE)
    is_noise = exc if exc is not None else F.lit(False)
    cm_strict = hp.rlike(CM_STRICT)
    uncovered = cm_strict & ~is_noise & ~in_scope
    cov = raw_window.filter(hp != F.lit("")).agg(
        F.count("*").alias("n"),
        F.sum(in_scope.cast("int")).alias("in_scope"),
        F.sum(cm_strict.cast("int")).alias("cm_strict"),
        F.sum(uncovered.cast("int")).alias("uncovered"),
    ).collect()[0]
    top_uncovered = [{"host_path": x["hp5"], "hits": x["n"]}
                     for x in (raw_window.filter(uncovered)
                               .select(F.regexp_extract(hp, r"^([^/]+(?:/[^/]+){0,4})", 1).alias("hp5"))
                               .groupBy("hp5").agg(F.count("*").alias("n"))
                               .orderBy(F.desc("n")).limit(TOP_N).collect())]
    top_hosts = [{"host": x["h"], "hits": x["n"]}
                 for x in (raw_window.filter(host != F.lit(""))
                           .select(host.alias("h")).groupBy("h").agg(F.count("*").alias("n"))
                           .orderBy(F.desc("n")).limit(TOP_N).collect())]

    emit("url_scope_audit", {
        "note": ("window population; breakdown on coalesce(page_url, visit_start_page_url, "
                 "first_hit_page_url, post_page_url); host/path only (no raw query). uncovered = "
                 "CM_STRICT and not noise and not in current scope."),
        "window_rows": total,
        "per_url_column": per_col,
        "recommended_coalesce_order": recommended,
        "top_hosts": top_hosts,
        "coverage": {
            "nonblank_url_rows": cov["n"],
            "in_scope_rows": cov["in_scope"],
            "in_scope_pct": round(100.0 * (cov["in_scope"] or 0) / max(cov["n"], 1), 3),
            "cm_strict_rows": cov["cm_strict"],
            "uncovered_cm_rows": cov["uncovered"],
            "uncovered_cm_pct": round(100.0 * (cov["uncovered"] or 0) / max(cov["n"], 1), 4),
        },
        "top_uncovered_cm_host_paths": top_uncovered,
    })
    display(spark.createDataFrame([{"url_column": c, **per_col[c]} for c in present]))

run_section("S4b", s4b_url_scope_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S4c — URL-column & pagename category audit
# MAGIC Per-candidate-column CoverMe-category match (`CM_STRICT` / `CM_BROAD`), plus a `pagename`
# MAGIC sweep and a language-by-domain split. Sizes how much CoverMe traffic each column would add
# MAGIC BEYOND the recommended coalesce.

# COMMAND ----------

def s4c_url_column_audit():
    ensure_frames()
    _resolve_scope_cols(DF)
    raw_window = DF.filter(window_pred(DF, WINDOW_START))
    present = [c for c in URL_CANDIDATES if pick_col(raw_window, c)]
    if not present:
        emit("url_column_audit", {"error": "no candidate URL column in source"})
        return

    def hp(colexpr):
        uu = F.regexp_replace(F.lower(colexpr.cast("string")), r"^[a-z]+://", "")
        return F.regexp_extract(uu, r"^([^?#]*)", 1)

    coal = F.coalesce(*[F.when(nonblank(c), qcol(c)) for c in present])
    coal_cm = hp(coal).rlike(CM_STRICT)

    exprs = [F.count("*").alias("rows")]
    for c in present:
        h = hp(qcol(c))
        exprs += [F.sum(h.rlike(CM_STRICT).cast("int")).alias(c + "_cs"),
                  F.sum(h.rlike(CM_BROAD).cast("int")).alias(c + "_cb"),
                  F.sum((h.rlike(CM_STRICT) & ~coal_cm).cast("int")).alias(c + "_beyond")]
    rd = raw_window.agg(*exprs).collect()[0].asDict()
    total = rd["rows"] or 0
    per_col = {c: {"cm_strict_rows": rd[c + "_cs"], "cm_broad_rows": rd[c + "_cb"],
                   "cm_rows_beyond_coalesce": rd[c + "_beyond"]} for c in present}

    # pagename sweep
    pn_col = pick_col(raw_window, "pagename", "post_pagename")
    pagename = {"present": False}
    if pn_col:
        pn = F.lower(qcol(pn_col).cast("string"))
        pr = raw_window.agg(
            F.sum(F.when(nonblank(pn_col), 1).otherwise(0)).alias("nb"),
            F.approx_count_distinct(pn).alias("dist"),
            F.sum(pn.rlike(CM_BROAD).cast("int")).alias("cb")).collect()[0]
        top_pn = [{"pagename": x[pn_col], "hits": x["n"]}
                  for x in (raw_window.filter(nonblank(pn_col))
                            .groupBy(pn_col).agg(F.count("*").alias("n"))
                            .orderBy(F.desc("n")).limit(TOP_N).collect())]
        pagename = {"present": True, "col": pn_col,
                    "blank_pct": round(100.0 * (total - (pr["nb"] or 0)) / max(total, 1), 3),
                    "approx_distinct": pr["dist"], "cm_broad_rows": pr["cb"],
                    "top_pagenames": top_pn}

    # language by domain (CoverMe splits language by host, not path)
    host = F.regexp_extract(hp(coal), r"^([^/]+)", 1)
    lang = (F.when(host.rlike(r"pourmeproteger|manuvie|assurance-manuvie"), "fr")
             .when(host.rlike(r"coverme\.com|insttrip\.manulife\.com"), "en")
             .otherwise("unknown"))
    lang_rows = [{"lang": x["lang"], "hits": x["n"]}
                 for x in (raw_window.select(lang.alias("lang")).groupBy("lang")
                           .agg(F.count("*").alias("n")).orderBy(F.desc("n")).collect())]

    emit("url_column_audit", {
        "note": ("window population; per-column CM_STRICT/CM_BROAD; language split by DOMAIN "
                 "(coverme.com/insttrip=en, pourmeproteger/manuvie=fr)."),
        "window_rows": total,
        "columns_present": present,
        "recommended_scope_col": "coalesce(" + ", ".join(present) + ")",
        "per_url_column": per_col,
        "pagename": pagename,
        "language_by_domain": lang_rows,
    })
    display(spark.createDataFrame([{"url_column": c, **per_col[c]} for c in present]))

run_section("S4c", s4c_url_column_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S5 — Population census
# MAGIC Which columns are actually populated? Batched non-blank counts on the sample, then
# MAGIC approx-distinct only for live columns. ~1,180 columns on this table.

# COMMAND ----------

CENSUS = {}
CORE_MIN_PCT = 99.0

def s5_population_census():
    global CENSUS
    ensure_frames()
    all_cols = DF_S.columns
    dtypes = dict(DF_S.dtypes)

    pop_exprs = [(c, F.sum(F.when(nonblank(c), 1).otherwise(0))) for c in all_cols]
    pop_counts = batched_agg(DF_S, pop_exprs, COL_BATCH_SIZE)

    n = max(SAMPLE_ROWS, 1)
    populated = {c: cnt for c, cnt in pop_counts.items() if (cnt or 0) / n >= 0.001}
    sparse    = [c for c, cnt in pop_counts.items() if 0 < (cnt or 0) / n < 0.001]
    dead      = [c for c, cnt in pop_counts.items() if not cnt]

    dist_exprs = [(c, F.approx_count_distinct(qcol(c))) for c in populated]
    distincts = batched_agg(DF_S, dist_exprs, COL_BATCH_SIZE) if dist_exprs else {}

    CENSUS = {c: {"dtype": dtypes.get(c), "pop_pct": round(100.0 * pop_counts[c] / n, 3),
                  "apx_distinct": distincts.get(c)} for c in populated}
    core = {c for c in CENSUS if CENSUS[c]["pop_pct"] >= CORE_MIN_PCT}

    ranked = sorted(CENSUS.items(), key=lambda kv: -kv[1]["pop_pct"])
    emit("population_census", {
        "basis": "sample", "sample_rows": SAMPLE_ROWS, "n_total_cols": len(all_cols),
        "n_populated": len(populated), "n_sparse": len(sparse), "n_dead": len(dead),
        "n_core": len(core), "core_min_pct": CORE_MIN_PCT,
        "populated": [{"col": c, "label": dim_label(c), **v} for c, v in ranked[:MAX_PROFILED_COLS]],
        "populated_names_beyond_cap": [c for c, _ in ranked[MAX_PROFILED_COLS:]],
        "sparse_cols": sparse,
        "evar_live": sorted(c for c in populated if re.match(r"post_evar\d+$|evar\d+$", c)),
        "prop_live": sorted(c for c in populated if re.match(r"post_prop\d+$|prop\d+$", c)),
        "evar_core": sorted(c for c in core if re.match(r"post_evar\d+$|evar\d+$", c)),
        "prop_core": sorted(c for c in core if re.match(r"post_prop\d+$|prop\d+$", c)),
    })

run_section("S5", s5_population_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S6 — post_event_list decode
# MAGIC Event-ID frequency table (the raw material for the metric-registry slots) + events-per-hit
# MAGIC distribution. IDs labeled from the CoverMeDataMap event dictionary; Instance-of-eVar IDs
# MAGIC (100-199, 10000-10099) resolved by formula.

# COMMAND ----------

def decode_event(eid):
    """Resolve an Adobe post_event_list numeric ID to a label. Order: CoverMe event dictionary ->
    standard commerce -> 'Instance of eVarN (label)' formula -> unknown."""
    e = str(eid)
    if e in EVENT_LABELS:
        return EVENT_LABELS[e]
    if e in ADOBE_STD_EVENTS:
        return ADOBE_STD_EVENTS[e]
    try:
        n = int(e)
    except (TypeError, ValueError):
        return "unknown — resolve via CoverMe event dictionary"
    vn = None
    if 100 <= n <= 199:
        vn = n - 99
    elif 10000 <= n <= 10099:
        vn = n - 9899
    if vn is not None:
        lbl = EVAR_LABELS.get(vn)
        return f"Instance of eVar{vn}" + (f" ({lbl})" if lbl else "")
    return "unknown — resolve via CoverMe event dictionary"

TOP_EVENT_IDS = []

def s6_event_decode():
    global TOP_EVENT_IDS
    ensure_frames()
    ev_col = pick_col(DF_S, "post_event_list", "event_list")
    if not ev_col:
        emit("event_decode", {"error": "no post_event_list/event_list column"})
        return

    events_arr = F.filter(
        F.transform(F.split(qcol(ev_col), ","), lambda x: F.trim(x)), lambda x: x != "")
    base = DF_S.select(F.when(nonblank(ev_col), events_arr)
                        .otherwise(F.array().cast("array<string>")).alias("ev"))
    per_hit = base.agg(
        F.count("*").alias("hits"),
        F.sum(F.when(F.size("ev") > 0, 1).otherwise(0)).alias("hits_with_events"),
        F.expr("percentile_approx(size(ev), array(0.5, 0.95))").alias("pcts"),
        F.max(F.size("ev")).alias("max_events")).collect()[0]

    with_ev = base.filter(F.size("ev") > 0)
    inst = (with_ev.select(F.explode("ev").alias("e"))
            .select(F.split("e", "=")[0].alias("event_id"),
                    F.expr("try_cast(element_at(split(e, '='), 2) as double)").alias("val"))
            .groupBy("event_id")
            .agg(F.count("*").alias("instances"),
                 F.sum(F.when(F.col("val").isNotNull(), 1).otherwise(0)).alias("with_value"),
                 F.avg("val").alias("val_mean"), F.max("val").alias("val_max")))
    pres = (with_ev.select(F.explode(F.array_distinct(
                F.transform("ev", lambda x: F.split(x, "=")[0]))).alias("event_id"))
            .groupBy("event_id").agg(F.count("*").alias("hits_with")))
    freq = (inst.join(pres, "event_id", "outer").orderBy(F.desc("hits_with")).limit(60).collect())

    hits = max(per_hit["hits"], 1)
    event_freq = []
    for r in freq:
        eid = r["event_id"]
        event_freq.append({
            "event_id": eid, "label": decode_event(eid),
            "hits_with_pct": round(100.0 * (r["hits_with"] or 0) / hits, 3),
            "instances": r["instances"],
            "has_value_pct": round(100.0 * (r["with_value"] or 0) / r["instances"], 2) if r["instances"] else None,
            "val_mean": r["val_mean"], "val_max": r["val_max"]})
    TOP_EVENT_IDS = [e["event_id"] for e in event_freq[:TOP_EVENTS_K]]

    emit("event_decode", {
        "basis": "sample", "source_col": ev_col, "sample_hits": per_hit["hits"],
        "pct_hits_with_events": round(100.0 * per_hit["hits_with_events"] / hits, 2),
        "events_per_hit_p50_p95": list(per_hit["pcts"]) if per_hit["pcts"] else None,
        "events_per_hit_max": per_hit["max_events"], "event_freq": event_freq})

run_section("S6", s6_event_decode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S6b — Funnel KPI (quote → application)
# MAGIC Exact, full-scoped-history daily counts for the business-flagged conversion funnel
# MAGIC (Quote Start → Quote Complete → Save Quote → App Start → App Confirm). These fire below the
# MAGIC discovery-probe's top-60 sample cutoff, so this section confirms — on real data — that each
# MAGIC event is present and usable as an anomaly KPI, with per-event totals, active-day counts,
# MAGIC first/last-seen, step conversion rates, and a daily series for the timeline.

# COMMAND ----------

def s6b_funnel_kpi():
    ensure_frames()
    ev_col = pick_col(DF_CM, "post_event_list", "event_list")
    if not ev_col:
        emit("funnel_kpi", {"error": "no post_event_list/event_list column"})
        return
    funnel_ids = [eid for eid, _ in FUNNEL_EVENTS]
    label_by_id = dict(FUNNEL_EVENTS)

    # One pass over the FULL scoped history: explode hit-present event ids, keep only the funnel
    # ids, group by (day, event). Filtered early so the scan stays cheap despite full history.
    ids = F.array_distinct(F.transform(
        F.filter(F.transform(F.split(qcol(ev_col), ","), lambda x: F.trim(x)), lambda x: x != ""),
        lambda x: F.split(x, "=")[0]))
    daily = (DF_CM.filter(nonblank(ev_col))
             .select(day_expr(DF_CM).alias("d"), F.explode(ids).alias("eid"))
             .filter(F.col("eid").isin(funnel_ids))
             .groupBy("d", "eid").count().collect())

    by_date, per_event = {}, {eid: {"hits": 0, "days": set(), "first": None, "last": None}
                             for eid in funnel_ids}
    for r in daily:
        if r["d"] is None:
            continue
        d, eid, c = r["d"], r["eid"], r["count"]
        by_date.setdefault(d, {})[eid] = c
        pe = per_event[eid]
        pe["hits"] += c
        pe["days"].add(d)
        pe["first"] = d if pe["first"] is None or d < pe["first"] else pe["first"]
        pe["last"] = d if pe["last"] is None or d > pe["last"] else pe["last"]

    scoped_hits = DF_CM.count()
    events = []
    for eid, name in FUNNEL_EVENTS:
        pe = per_event[eid]
        events.append({
            "event_id": eid, "name": name,
            "total_hits_with_event": pe["hits"],
            "pct_of_scoped_hits": round(100.0 * pe["hits"] / max(scoped_hits, 1), 4),
            "active_days": len(pe["days"]),
            "first_seen": str(pe["first"]) if pe["first"] else None,
            "last_seen": str(pe["last"]) if pe["last"] else None,
            "fires": pe["hits"] > 0,
        })

    def _tot(eid):
        return per_event[eid]["hits"]
    qs = max(_tot("228"), 1)
    conversion = {
        "quote_complete_over_quote_start": round(_tot("229") / qs, 4),
        "save_quote_over_quote_start": round(_tot("232") / qs, 4),
        "app_start_over_quote_start": round(_tot("269") / qs, 4),
        "app_confirm_over_app_start": round(_tot("240") / max(_tot("269"), 1), 4),
        "app_confirm_over_quote_start": round(_tot("240") / qs, 4),
    }
    missing = [e["name"] for e in events if not e["fires"]]

    # daily series (last MAX_CSV_LINES days) for the timeline
    dates = sorted(by_date.keys())
    csv = [",".join([str(d)] + [str(by_date.get(d, {}).get(eid, 0)) for eid in funnel_ids])
           for d in dates][-MAX_CSV_LINES:]

    emit("funnel_kpi", {
        "basis": "exact_full_scoped_history",
        "funnel_order": [name for _, name in FUNNEL_EVENTS],
        "scoped_hits": scoped_hits,
        "events": events,
        "conversion_rates_hit_presence": conversion,
        "events_not_firing": missing,
        "note": ("hit-presence counts (a hit carrying the event), not unique visitors — a proxy "
                 "for funnel volume. Any name in events_not_firing needs a business conversation "
                 "before it can be an anomaly KPI."),
        "csv_header": "date," + ",".join(f"{label_by_id[e]}({e})" for e in funnel_ids),
        "csv": csv,
    })

run_section("S6b", s6b_funnel_kpi)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S7 — Live eVars / props / campaign
# MAGIC Shape + RAW top-value distributions for every live custom dimension (ADR-0007 §5). The
# MAGIC eight business-flagged eVars (4,5,6,11,16,52,111,148) surface here labeled.

# COMMAND ----------

def s7_live_custom_dims():
    ensure_frames()
    live_all = [c for c in CENSUS
                if re.match(r"post_evar\d+$|evar\d+$|post_prop\d+$|prop\d+$|^post_campaign$|^campaign$", c)]
    live = sorted(live_all, key=lambda c: -CENSUS[c]["pop_pct"])[:MAX_PROFILED_COLS]
    if not live:
        emit("live_custom_dims", {"error": "no live eVar/prop/campaign columns (run S5 first)"})
        return
    n_core = sum(1 for c in live_all if CENSUS[c]["pop_pct"] >= CORE_MIN_PCT)

    out = []
    for c in live:
        stats = DF_S.filter(nonblank(c)).agg(
            F.expr(f"percentile_approx(length(`{c}`), 0.5)").alias("len_p50"),
            F.max(F.length(qcol(c))).alias("len_max"),
            F.avg(F.length(qcol(c))).alias("len_avg"),
            F.avg(F.when(qcol(c).cast("string").startswith("http"), 1.0).otherwise(0.0)).alias("url_frac"),
        ).collect()[0]
        top = (DF_S.filter(nonblank(c)).groupBy(qcol(c).alias("v")).count()
                   .orderBy(F.desc("count")).limit(TOP_N).collect())
        pop_rows = max(SAMPLE_ROWS * CENSUS[c]["pop_pct"] / 100.0, 1)
        out.append({
            "col": c, "label": dim_label(c),
            "pop_pct": CENSUS[c]["pop_pct"], "apx_distinct": CENSUS[c]["apx_distinct"],
            "len": {"p50": stats["len_p50"], "avg": stats["len_avg"], "max": stats["len_max"]},
            "looks_like_url": (stats["url_frac"] or 0) > 0.5,
            "free_text": (stats["len_avg"] or 0) > 80,
            "top": [{"v": str(r["v"]), "len": len(str(r["v"])),
                     "pct": round(100.0 * r["count"] / pop_rows, 2)} for r in top],
            "mode": "raw"})
    emit("live_custom_dims", {"basis": "sample", "n_live": len(live_all), "n_core": n_core, "dims": out})

run_section("S7", s7_live_custom_dims)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S8 — Time-series pack (exact, on the full window)
# MAGIC Daily hits/visits/visitors/clean-hits + per-day series for the top-K event IDs + 7×24
# MAGIC day-of-week × hour profile. This is what the anomaly-model design consumes.

# COMMAND ----------

TS_DAILY_PDF = None

def s8_time_series():
    global TS_DAILY_PDF
    ensure_frames()
    vis_hi = pick_col(DF_W, "post_visid_high", "visid_high")
    vis_lo = pick_col(DF_W, "post_visid_low", "visid_low")
    visit_num = pick_col(DF_W, "visit_num")
    excl = pick_col(DF_W, "exclude_hit")

    aggs = [F.count("*").alias("hits")]
    if vis_hi and vis_lo and visit_num:
        aggs.append(F.approx_count_distinct(F.concat_ws(":", vis_hi, vis_lo, visit_num)).alias("visits"))
        aggs.append(F.approx_count_distinct(F.concat_ws(":", vis_hi, vis_lo)).alias("visitors"))
    if excl:
        aggs.append(F.sum(F.when(F.coalesce(F.expr(f"try_cast(`{excl}` as int)"), F.lit(0)) == 0, 1)
                          .otherwise(0)).alias("clean_hits"))

    daily = (DF_W.groupBy(day_expr(DF_W).alias("d")).agg(*aggs).orderBy("d").collect())
    cols = ["hits", "visits", "visitors", "clean_hits"]
    series = {c: [] for c in cols}
    dates = []
    for r in daily:
        if r["d"] is None:
            continue
        dates.append(r["d"]); rd = r.asDict()
        for c in cols:
            series[c].append(rd.get(c))
    csv_daily = [",".join([str(d)] + [str(series[c][i]) if series[c][i] is not None else ""
                                      for c in cols]) for i, d in enumerate(dates)]

    csv_events, ev_cols = [], []
    ev_col = pick_col(DF_W, "post_event_list", "event_list")
    if ev_col and TOP_EVENT_IDS:
        ev_daily = (DF_W.filter(nonblank(ev_col))
                    .select(day_expr(DF_W).alias("d"),
                            F.explode(F.array_distinct(F.transform(
                                F.filter(F.transform(F.split(qcol(ev_col), ","), lambda x: F.trim(x)),
                                         lambda x: x != ""),
                                lambda x: F.split(x, "=")[0]))).alias("event_id"))
                    .filter(F.col("event_id").isin(TOP_EVENT_IDS))
                    .groupBy("d", "event_id").count().collect())
        by_date = {}
        for r in ev_daily:
            if r["d"] is not None:
                by_date.setdefault(r["d"], {})[r["event_id"]] = r["count"]
        ev_cols = TOP_EVENT_IDS
        csv_events = [",".join([str(d)] + [str(by_date.get(d, {}).get(e, 0)) for e in ev_cols])
                      for d in dates]

    hour_matrix = None
    if dates:
        h_start = dates[-1] - datetime.timedelta(days=HOURLY_DAYS)
        hourly = (DF_W.filter(day_expr(DF_W) >= F.lit(str(h_start)).cast("date"))
                  .select(day_expr(DF_W).alias("d"), F.hour(TS_EXPR).alias("h"))
                  .groupBy("d", "h").count()
                  .groupBy(F.dayofweek("d").alias("dow"), "h")
                  .agg(F.avg("count").alias("mean_hits")).collect())
        mat = [[0] * 24 for _ in range(7)]
        for r in hourly:
            if r["h"] is not None:
                mat[(r["dow"] + 5) % 7][r["h"]] = round(r["mean_hits"], 1)
        hour_matrix = mat

    profiles = {}
    try:
        import pandas as pd
        s = pd.Series(series["hits"], index=pd.to_datetime([str(d) for d in dates]))
        overall = s.mean()
        dow_idx = (s.groupby(s.index.dayofweek).mean() / overall).round(3)
        roll = s.rolling(7, center=True).median()
        shift_scores = (roll / roll.shift(7)).apply(lambda x: abs(math.log(x)) if x and x > 0 else 0)
        top_shifts = shift_scores.nlargest(5)
        profiles = {
            "cv": round(float(s.std() / overall), 4) if overall else None,
            "autocorr_lag7": round(float(s.autocorr(7)), 4) if len(s) > 14 else None,
            "autocorr_lag28": round(float(s.autocorr(28)), 4) if len(s) > 56 else None,
            "dow_index_mon_to_sun": [float(dow_idx.get(i, float("nan"))) for i in range(7)],
            "level_shift_candidates": [
                {"date": str(d.date()), "abs_log_ratio_wow": round(float(v), 3)}
                for d, v in top_shifts.items() if v > math.log(1.3)]}
        TS_DAILY_PDF = s.reset_index()
    except Exception as e:
        profiles = {"error": f"pandas stats failed: {type(e).__name__}: {str(e)[:150]}"}

    emit("ts_daily", {
        "basis": "exact_window", "csv_header": "date," + ",".join(cols),
        "csv": csv_daily[-MAX_CSV_LINES:],
        "visits_visitors_note": "approx_count_distinct (~5% rsd)" if vis_hi else "visid columns missing"})
    if csv_events:
        emit("ts_events", {
            "basis": "exact_window (hits containing event, not instances)",
            "csv_header": "date," + ",".join("ev" + e for e in ev_cols),
            "csv": csv_events[-MAX_CSV_LINES:]})
    emit("ts_profiles", {
        "hour_matrix_rows_mon_to_sun_cols_0_23h": hour_matrix,
        "hourly_days": HOURLY_DAYS, **profiles})

run_section("S8", s8_time_series)

# COMMAND ----------

# chart for your own inspection
if TS_DAILY_PDF is not None:
    display(spark.createDataFrame(TS_DAILY_PDF.astype(str)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## S9 — Dimension candidates
# MAGIC Cardinality + top values for EVERY populated dimension (census-driven; eVars/props are in
# MAGIC S7). All values print raw (ADR-0007 §5); URL/pagename keep the full path, query raw by default.

# COMMAND ----------

def s9_dimensions():
    ensure_frames()
    KNOWN_DIMS = [
        "pagename", "post_pagename", "page_url", "post_page_url", "visit_start_page_url",
        "first_hit_page_url", "referrer", "ref_domain", "ref_type", "geo_country",
        "geo_region", "geo_city", "browser", "os", "connection_type", "language",
        "hit_source", "exclude_hit", "new_visit", "va_closer_id",
    ]
    _s7_re = re.compile(r"post_evar\d+$|evar\d+$|post_prop\d+$|prop\d+$|^post_campaign$|^campaign$")
    cols_present = set(DF_S.columns)
    ordered = [c for c in KNOWN_DIMS if c in CENSUS and c in cols_present]
    extra = sorted((c for c in CENSUS
                    if c not in KNOWN_DIMS and c in cols_present and not _s7_re.match(c)),
                   key=lambda c: -CENSUS[c]["pop_pct"])
    dim_candidates = (ordered + extra)[:MAX_PROFILED_COLS]

    LOOKUP_ID_DIMS = {"browser", "os", "language", "connection_type",
                      "geo_country", "geo_region", "geo_dma", "color", "javascript"}
    out = []
    for c in dim_candidates:
        is_url = ("url" in c) or c in ("referrer", "post_referrer")
        top = (DF_S.filter(nonblank(c)).groupBy(qcol(c).alias("v")).count()
                   .orderBy(F.desc("count")).limit(TOP_N * (3 if is_url else 1)).collect())
        pop_rows = max(SAMPLE_ROWS * CENSUS[c]["pop_pct"] / 100.0, 1)
        if is_url or c in ("pagename", "post_pagename"):
            top_vals = [{"v": maybe_strip(r["v"]), "pct": round(100.0 * r["count"] / pop_rows, 2)}
                        for r in top[:TOP_N]]
            mode = "raw, query-stripped" if STRIP_URL_QUERY else "raw"
        else:
            top_vals = [{"v": str(r["v"]), "pct": round(100.0 * r["count"] / pop_rows, 2)}
                        for r in top[:TOP_N]]
            mode = "raw"
        out.append({"dim": c, "mode": mode, "label": dim_label(c),
                    "coverage_pct": CENSUS[c]["pop_pct"], "apx_distinct": CENSUS[c]["apx_distinct"],
                    "top": top_vals, "note": ("numeric lookup-ID code" if c in LOOKUP_ID_DIMS else "")})
    emit("dim_candidates", {"basis": "sample", "n_dims": len(out), "dims": out})

run_section("S9", s9_dimensions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S10 — Data-quality baseline
# MAGIC Bot-filter distributions (exclude_hit × hit_source), clock skew, duplicate rate (exact, one
# MAGIC recent day), late-arrival evidence if a load-timestamp column exists.

# COMMAND ----------

def s10_dq_baseline():
    ensure_frames()
    key_cols = [c for c in ["date_time", "hit_time_gmt", "hit_date", "visit_num", "visit_page_num",
                            "post_event_list", "pagename", "page_url", "exclude_hit",
                            "hit_source"] if c in set(DF_S.columns)]
    key_nulls = {c: round(100.0 - CENSUS.get(c, {}).get("pop_pct", 0.0), 3) if c in CENSUS
                 else 100.0 for c in key_cols}

    dist = []
    if pick_col(DF_S, "exclude_hit") and pick_col(DF_S, "hit_source"):
        dist = [{"exclude_hit": str(r["exclude_hit"]), "hit_source": str(r["hit_source"]),
                 "pct": round(100.0 * r["count"] / max(SAMPLE_ROWS, 1), 3)}
                for r in (DF_S.groupBy("exclude_hit", "hit_source").count()
                              .orderBy(F.desc("count")).limit(20).collect())]

    skew = None
    if pick_col(DF_S, "hit_time_gmt"):
        skew_row = (DF_S.filter(nonblank("hit_time_gmt"))
                    .select((F.unix_timestamp(TS_EXPR) - F.col("hit_time_gmt").cast("long")).alias("skew_s"))
                    .agg(F.expr("percentile_approx(skew_s, array(0.05, 0.5, 0.95))").alias("p")).collect()[0])
        skew = {"p5_p50_p95_seconds": list(skew_row["p"]) if skew_row["p"] else None,
                "note": "constant offset = timezone of date_time; spread = clock skew"}

    dup = None
    vis_hi = pick_col(DF_S, "post_visid_high", "visid_high")
    vis_lo = pick_col(DF_S, "post_visid_low", "visid_low")
    seq = pick_col(DF_S, "visit_page_num", "hit_time_gmt")
    cm_days = [d for d, cm in DAILY_ROWS if cm > 0]
    if cm_days and vis_hi and vis_lo and pick_col(DF_S, "visit_num") and seq:
        check_day = cm_days[-2] if len(cm_days) >= 2 else cm_days[-1]
        day_df = DF_CM.filter(day_expr(DF_CM) == F.lit(str(check_day)).cast("date"))
        total = day_df.count()
        distinct = day_df.select(vis_hi, vis_lo, "visit_num", seq).distinct().count()
        dup = {"day": str(check_day), "rows": total, "distinct_keys": distinct,
               "dup_pct": round(100.0 * (total - distinct) / max(total, 1), 4),
               "key": f"{vis_hi},{vis_lo},visit_num,{seq}", "basis": "exact_one_day"}

    load_cols = [c for c in DF_S.columns
                 if re.search(r"(load|ingest|etl|insert|_created|processed).*(ts|time|date)|_ts$",
                              c, re.IGNORECASE)]
    late = {"load_timestamp_cols_found": load_cols[:10]}
    if load_cols:
        lc = load_cols[0]
        try:
            late_row = (DF_S.filter(nonblank(lc))
                        .select(F.datediff(F.to_date(qcol(lc).cast("timestamp")),
                                           day_expr(DF_S)).alias("lag_days"))
                        .agg(F.expr("percentile_approx(lag_days, array(0.5, 0.95, 0.99))").alias("p")).collect()[0])
            late["lag_days_p50_p95_p99"] = list(late_row["p"]) if late_row["p"] else None
            late["col_used"] = lc
        except Exception as e:
            late["error"] = f"{type(e).__name__}: {str(e)[:150]}"
    else:
        late["note"] = "no load-timestamp column; use S2 write cadence as arrival evidence"

    emit("dq_baseline", {
        "basis": "sample (dup check exact on one day)",
        "key_col_null_blank_pct": key_nulls, "exclude_hit_x_hit_source_pct": dist,
        "clock_skew": skew, "duplicates": dup, "late_arrival": late})

run_section("S10", s10_dq_baseline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S11 — Identity evidence (ADR-0007)
# MAGIC Cardinality/null evidence for every identity column: confirms the CoverMe identity shape
# MAGIC (mcvisid populated; cust_visid / post_cust_visid all-null; userid cardinality-1).

# COMMAND ----------

def s11_identity():
    ensure_frames()
    identity_cols = [c for c in [
        "mcvisid", "visid_high", "visid_low", "post_visid_high", "post_visid_low",
        "cust_visid", "post_cust_visid", "userid", "username", "user_hash",
        "cookies", "persistent_cookie", "visid_type", "visid_new",
    ] if c in set(DF_S.columns)]

    out = []
    for c in identity_cols:
        r = DF_S.agg(
            F.avg(F.when(nonblank(c), 0.0).otherwise(1.0)).alias("null_blank_frac"),
            F.approx_count_distinct(qcol(c)).alias("apx_distinct"),
            F.min(F.when(nonblank(c), F.length(qcol(c).cast("string")))).alias("len_min"),
            F.avg(F.when(nonblank(c), F.length(qcol(c).cast("string")))).alias("len_avg"),
            F.max(F.when(nonblank(c), F.length(qcol(c).cast("string")))).alias("len_max"),
        ).collect()[0]
        out.append({"col": c, "null_blank_pct": round(100.0 * r["null_blank_frac"], 3),
                    "apx_distinct": r["apx_distinct"],
                    "len": {"min": r["len_min"], "avg": r["len_avg"], "max": r["len_max"]}})

    by_col = {o["col"]: o for o in out}
    flags = {
        "cust_visid_all_null": by_col.get("cust_visid", {}).get("null_blank_pct") == 100.0
                               if "cust_visid" in by_col else None,
        "post_cust_visid_all_null": by_col.get("post_cust_visid", {}).get("null_blank_pct") == 100.0
                                    if "post_cust_visid" in by_col else None,
        "userid_cardinality_1": by_col.get("userid", {}).get("apx_distinct") == 1
                                if "userid" in by_col else None,
    }
    ratios = {}
    ts = RESULTS.get("ts_daily", {})
    if ts.get("csv"):
        try:
            rows = [ln.split(",") for ln in ts["csv"]]
            hits = [float(r[1]) for r in rows if len(r) > 3 and r[1]]
            visits = [float(r[2]) for r in rows if len(r) > 3 and r[2]]
            visitors = [float(r[3]) for r in rows if len(r) > 3 and r[3]]
            if visits and visitors:
                ratios = {"mean_hits_per_visit": round(sum(hits) / sum(visits), 3),
                          "mean_visits_per_visitor_daily": round(sum(visits) / sum(visitors), 3)}
        except Exception:
            pass

    emit("identity_evidence", {
        "basis": "sample",
        "note": "identity-column cardinality/null evidence; raw identifier values are in S7/S9 "
                "per ADR-0007 §5 (full-raw)",
        "columns": out, "flags": flags, "daily_ratios": ratios})

run_section("S11", s11_identity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S12 — Synthesis spec (master paste-back artifact)
# MAGIC Consolidates all prior sections into one machine-readable spec for the synthetic data
# MAGIC generator. Tolerant of skipped sections.

# COMMAND ----------

def s12_synthesis_spec():
    expected = ["uc_discovery", "delta_meta", "daily_volume", "window_frame", "url_scope_audit",
                "url_column_audit", "population_census", "event_decode", "funnel_kpi",
                "live_custom_dims", "ts_daily", "ts_events", "ts_profiles", "dim_candidates",
                "dq_baseline", "identity_evidence"]
    missing = [s for s in expected if s not in RESULTS]

    census = RESULTS.get("population_census", {})
    live_dims = {d["col"]: d for d in RESULTS.get("live_custom_dims", {}).get("dims", [])}
    schema_spec = []
    for entry in census.get("populated", []):
        col = entry["col"]
        spec = {"col": col, "dtype": entry.get("dtype"), "pop_pct": entry.get("pop_pct"),
                "apx_distinct": entry.get("apx_distinct"), "label": dim_label(col)}
        if col in live_dims:
            spec["len"] = live_dims[col].get("len")
            spec["top_values"] = live_dims[col].get("top")
        schema_spec.append(spec)

    dv = RESULTS.get("daily_volume", {})
    prof = RESULTS.get("ts_profiles", {})
    _, scope_meta = scope_condition(DF) if DF is not None else (None, None)
    emit("synthesis_spec", {
        "meta": {
            "table": TABLE_FQN, "product": "coverme", "single_suite": True,
            "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "scope": {"url_scope_mode": URL_SCOPE_MODE, "url_include": URL_INCLUDE or None,
                      "url_exclude": URL_EXCLUDE or None,
                      "url_cols_coalesced": (scope_meta or {}).get("url_cols_coalesced"),
                      "cm_share_pct": dv.get("cm_share_pct")},
            "window": {"start": str(WINDOW_START), "end": str(WINDOW_END), "months": WINDOW_MONTHS},
            "sample_fraction": SAMPLE_FRACTION, "sample_rows": SAMPLE_ROWS,
            "sections_missing": missing, "sections_skipped": SKIPPED,
        },
        "volume": {
            "total_rows": dv.get("total_rows_cm"),
            "date_min": dv.get("cm_date_min"), "date_max": dv.get("cm_date_max"),
            "monthly_totals": dv.get("monthly_totals_cm"),
            "dow_mean_hits_mon_to_sun": dv.get("dow_mean_cm_hits_mon_to_sun"),
            "missing_days": dv.get("n_cm_days_missing"),
            "cv": prof.get("cv"), "autocorr_lag7": prof.get("autocorr_lag7"),
            "autocorr_lag28": prof.get("autocorr_lag28"),
            "dow_index": prof.get("dow_index_mon_to_sun"),
            "hour_matrix": prof.get("hour_matrix_rows_mon_to_sun_cols_0_23h"),
            "level_shifts": prof.get("level_shift_candidates"),
        },
        "series_ref": "daily values in the ts_daily / ts_events / funnel_kpi shareable blocks",
        "schema": schema_spec,
        "events": RESULTS.get("event_decode", {}),
        "funnel": RESULTS.get("funnel_kpi", {}),
        "dims": RESULTS.get("dim_candidates", {}).get("dims"),
        "dq": RESULTS.get("dq_baseline", {}),
        "identity": RESULTS.get("identity_evidence", {}),
    })

run_section("S12", s12_synthesis_spec)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run manifest — integrity check for the export
# MAGIC Byte length + sha1 of every shareable section, from the exact JSON that was printed.

# COMMAND ----------

def s_run_manifest():
    sections = {}
    for sid, payload in RESULTS.items():
        body = json.dumps(payload, separators=(",", ":"), default=str)
        sections[sid] = {"bytes": len(body), "sha1": hashlib.sha1(body.encode("utf-8")).hexdigest()}
    emit("run_manifest", {"sections": sections, "n_sections": len(sections), "skipped": SKIPPED})

run_section("run_manifest", s_run_manifest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done — how to hand back the results
# MAGIC
# MAGIC **Primary: export the run notebook itself.** After a full Run All, use
# MAGIC `File → Export → IPython Notebook (.ipynb)`. Every `===== BEGIN SHAREABLE: <id> =====` block
# MAGIC is captured in the cell outputs, so nothing is copied by hand.
# MAGIC
# MAGIC **Verify nothing was truncated.** The final `run_manifest` block lists the byte length +
# MAGIC sha1 of every section; re-hash an exported block and compare.
# MAGIC
# MAGIC **First sanity checks on any run:**
# MAGIC - `window_frame.filter.host_breakdown` must show `coverme.com`, `pourmeproteger.com`, and
# MAGIC   (until 2024-03) `insttrip.manulife.com` with non-zero rows — a single-host result means the
# MAGIC   scope silently collapsed to one brand/language.
# MAGIC - `funnel_kpi.events_not_firing` must be empty (or the named events need a business
# MAGIC   conversation before they become anomaly KPIs).
# MAGIC - `url_scope_audit.coverage.uncovered_cm_pct` should be small (~0.1%); a large value means
# MAGIC   real CoverMe traffic is escaping the scope — widen `url_scope_list`.
