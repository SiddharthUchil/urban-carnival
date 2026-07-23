# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — GWAM Canada Retirement EDA
# MAGIC
# MAGIC **Purpose.** Read-only exploratory profiling of the Adobe Analytics hit-level table
# MAGIC (`gwam_prod_catalog.inv_typed_common.adobe_hit_data`, provisional) to:
# MAGIC
# MAGIC **Scope.** The table holds ALL GWAM Adobe data. CA Retirement is the subset with
# MAGIC `rsid` IN (`manugrs`, `manulifeglobalprod`) AND a URL matching the `url_scope_mode`
# MAGIC include list — default `broad`: `%/group-retirement%`, `%/group-plans%`,
# MAGIC `%/regimes-collectifs%`. Those patterns are language- AND domain-agnostic, so one
# MAGIC list covers both suites (`manulifeim.com` and `manulife.com`) in EN and FR.
# MAGIC
# MAGIC Two exclusions apply. `url_scope_exclude` drops AEM authoring/staging hosts and
# MAGIC non-CA `/ph/` paths. `login_host_exclude` drops the six D8 individual-login hosts
# MAGIC (member/auth portals) and is subtracted in EVERY mode — it is an explicit host list,
# MAGIC NOT a `%portal%` pattern, because four of the six carry no "portal" substring
# MAGIC (`id.manulife.ca` alone is 62.6M hits) and the FR spelling is "portail".
# MAGIC
# MAGIC URL matching uses the D4 blank-guarded `coalesce(page_url, post_page_url)`:
# MAGIC `post_page_url` is blank 36-46% of the time vs <=0.013% for `page_url`, and Adobe
# MAGIC writes empty strings rather than NULLs, so blanks are mapped to NULL before the
# MAGIC coalesce. All of the above are widgets. All profiling sections (S4–S11) and the
# MAGIC synthesis spec describe this subset; S3 reports total-table vs subset daily volume
# MAGIC side by side, and `window_frame.filter.rsid_breakdown` gives per-suite row counts.
# MAGIC 1. Fill evidence gaps: volume, history depth, load cadence, schema population census.
# MAGIC 2. Discover real metric candidates (post_event_list event IDs, live eVars/props).
# MAGIC 3. Capture time-series shape (seasonality, volatility) for anomaly-model design.
# MAGIC 4. Produce a machine-readable **synthesis spec** for generating synthetic data.
# MAGIC
# MAGIC **Data visibility (ADR-0007 §5, full-raw revision 2026-07-23).** EVERY column profiles
# MAGIC **raw and in full** — eVars, props, events, URLs, pagenames, campaigns, referrers, AND
# MAGIC the direct/quasi-identifier set (visitor IDs, cookies, IPs, geo_zip, user_agent, tracking
# MAGIC eVars). There is no shape-only carve-out: a comprehensive view of all in-scope columns
# MAGIC was the explicit requirement, and this feed carries no person-level identifier in scope
# MAGIC (`cust_visid` wholly NULL, `userid` a single constant — ADR-0007 Context §2).
# MAGIC
# MAGIC URL query strings profile raw by default (the `strip_url_query` widget strips them).
# MAGIC ⚠ SHAREABLE blocks now carry raw identifiers/PII (IPs, postal codes, device IDs) — a
# MAGIC human read-through is required before any block leaves the governed workspace.
# MAGIC
# MAGIC **How to run.** Databricks → Workspace → Import → File → select this `.py` (it
# MAGIC imports as a notebook — the file is in Databricks "source" format). Attach to any
# MAGIC cluster with Unity Catalog access (DBR 13+ recommended); a small cluster is fine,
# MAGIC since the heavy sections run on a 5% sample. Run the **S0 config cell** once so the
# MAGIC widgets appear, then **Run All**. Expect S1/S2 in seconds, S3 as the one full-table
# MAGIC scan (minutes), S5–S11 on the sample, S8 two exact passes over the window.
# MAGIC
# MAGIC **What to paste back.** Each section prints a
# MAGIC `===== BEGIN SHAREABLE: <id> =====` block — copy those verbatim. Multi-part blocks
# MAGIC (`part 1 of N`) reassemble by concatenation, so paste every part. Priority order if
# MAGIC splitting across messages: (1) `synthesis_spec`; (2) `ts_daily`, `ts_events`,
# MAGIC `ts_profiles`; (3) `event_decode`, `live_custom_dims`, `population_census`;
# MAGIC (4) `daily_volume`, `delta_meta`; (5) `dim_candidates`, `dq_baseline`,
# MAGIC `identity_evidence`, `uc_discovery`, `window_frame`.
# MAGIC
# MAGIC **If something goes wrong.** Sections are independent: a failure prints
# MAGIC `===== SKIPPED: <id> | <reason> =====` and the run continues — paste SKIPPED lines
# MAGIC back too. If everything downstream is empty the scope filter matched 0 rows: check
# MAGIC `window_frame.filter.top_rsids` for the real rsid values/casing and
# MAGIC `window_frame.filter.rsid_breakdown` for per-suite counts, then adjust the
# MAGIC `rsid_list` / `url_scope_*` widgets and re-run from S3. Too slow on a small cluster?
# MAGIC Lower `sample_fraction` to 0.01 and/or `window_months` to 6 and re-run from S4
# MAGIC (sections rebuild their frames).

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
dbutils.widgets.text("table_fqn", "gwam_prod_catalog.inv_typed_common.adobe_hit_data", "1. Table (catalog.schema.table)")
dbutils.widgets.text("window_months", "13", "2. Deep-profiling window (months)")
dbutils.widgets.text("sample_fraction", "0.05", "3. Sample fraction for per-column stats")
dbutils.widgets.text("col_batch_size", "150", "4. Columns per aggregation batch")
dbutils.widgets.text("top_n", "25", "5. Top-N cap for value lists")
dbutils.widgets.text("hourly_days", "35", "6. Days for hourly profile")
dbutils.widgets.text("max_csv_lines", "450", "7. Max CSV lines per shareable block")
dbutils.widgets.text("top_events_k", "12", "8. Top-K events for daily series")
dbutils.widgets.text("cache_sample", "false", "9. Persist sample df (true/false)")
dbutils.widgets.text("rsid_list", "manugrs,manulifeglobalprod", "10. rsid list (comma-sep, empty = off)")
dbutils.widgets.dropdown("url_scope_mode", "broad", ["broad", "en_only"], "11. URL scope mode (en_only = pipeline parity)")
dbutils.widgets.text("url_scope_list", "%/group-retirement%,%/group-plans%,%/regimes-collectifs%", "12. URL include patterns — ADD URLS HERE (SQL LIKE, comma-sep)")
dbutils.widgets.text("url_scope_exclude", "%adobeaemcloud.com%,%/ph/%", "13. URL patterns to exclude")
dbutils.widgets.text("login_host_exclude",
                     "%portal.manulife.ca%,%id.manulife.ca%,%grsmembers.manulife.com%,"
                     "%gsrs1.manulife.com%,%viproom.manulife.com%,%portail.manuvie.ca%",
                     "14. Individual-login hosts to exclude (D8)")
dbutils.widgets.text("max_profiled_cols", "1200", "15. Max columns emitted with full stats (raise for full coverage)")
dbutils.widgets.dropdown("strip_url_query", "false", ["false", "true"], "16. Strip URL query strings before profiling")
dbutils.widgets.text("event_lookup_path", "new_data/event.tsv", "17. Event-ID lookup TSV (blank = inline map only)")

TABLE_FQN      = dbutils.widgets.get("table_fqn").strip()
WINDOW_MONTHS  = int(dbutils.widgets.get("window_months"))
SAMPLE_FRACTION = float(dbutils.widgets.get("sample_fraction"))
COL_BATCH_SIZE = int(dbutils.widgets.get("col_batch_size"))
TOP_N          = int(dbutils.widgets.get("top_n"))
HOURLY_DAYS    = int(dbutils.widgets.get("hourly_days"))
MAX_CSV_LINES  = int(dbutils.widgets.get("max_csv_lines"))
TOP_EVENTS_K   = int(dbutils.widgets.get("top_events_k"))
CACHE_SAMPLE   = dbutils.widgets.get("cache_sample").strip().lower() == "true"
MAX_PROFILED_COLS = int(dbutils.widgets.get("max_profiled_cols"))
STRIP_URL_QUERY   = dbutils.widgets.get("strip_url_query").strip().lower() == "true"
EVENT_LOOKUP_PATH = dbutils.widgets.get("event_lookup_path").strip()
def _csv(widget):
    return [p.strip().lower() for p in dbutils.widgets.get(widget).split(",") if p.strip()]

RSID_LIST      = _csv("rsid_list")
URL_SCOPE_MODE = dbutils.widgets.get("url_scope_mode").strip().lower()
URL_EXCLUDE    = _csv("url_scope_exclude")
LOGIN_EXCLUDE  = _csv("login_host_exclude")

# Scope modes (doc-16 D5). The `url_scope_list` widget is AUTHORITATIVE: whatever patterns
# are visible there are the patterns that run, so adding a URL means editing that widget and
# nothing else. `en_only` is the single override — it pins the one pattern the bronze
# pipeline still ingests, so an EDA run can be compared like-for-like against production.
#
# The default list is language- AND domain-agnostic, so three patterns cover both suites
# (manugrs on manulifeim.com, manulifeglobalprod on manulife.com) in EN and FR, including
# every gap the 2026-07-20 URL scope inventory found: /ca/fr retirement (801,461 rows) via
# %/group-retirement%, /ca/fr/particuliers/regimes-collectifs/* (183,698) via
# %/regimes-collectifs%, and /ca/en/{business,advisor,personal}/group-plans/* (285,266+)
# via %/group-plans%.
URL_SCOPE_EN_ONLY = ["%manulife.com/ca/en/personal/group-plans/group-retirement%"]
URL_INCLUDE = URL_SCOPE_EN_ONLY if URL_SCOPE_MODE == "en_only" else _csv("url_scope_list")

# -------------------------------------------------------- privacy stance ------
# ADR-0007 §5 (analysis-time visibility), full-raw revision 2026-07-23. EDA runs
# inside the governed Databricks workspace against a feed that carries NO
# person-level identifier in the login-excluded marketing scope: cust_visid /
# post_cust_visid are wholly NULL and userid is a single constant account value
# (cardinality 1) — confirmed with the data owner 2026-07-04, ADR-0007 Context §2.
#
# Per the data-owner decision recorded in ADR-0007 §5, EVERY column now profiles and
# emits RAW values — including the direct/quasi-identifier set (mcvisid, visid_*,
# cookies, ip/ip2/ipv6, geo_zip, user_agent) and tracking eVars (ECID eVar131,
# Medallia UUID eVar140). There is NO shape-only carve-out: a comprehensive view of
# all in-scope columns was the explicit requirement.
#
# This changes NOTHING about the shipped pipeline — HMAC-SHA-256 pseudonymization at
# Bronze->Silver (ADR-0007 §2) still stands. Only what the EDA notebook PRINTS widens.
PII_EXPORT_WARNING = (
    "PII NOTICE (ADR-0007 §5): SHAREABLE blocks below carry RAW identifiers and PII "
    "(IPs, postal codes, device IDs, User-Agent, tracking eVars). They may leave the "
    "governed Databricks workspace only after a human read-through."
)
print("\n" + "=" * 78 + "\n" + PII_EXPORT_WARNING + "\n" + "=" * 78 + "\n")

# --------------------------------------------------------- semantic labels ----
# EDDL eVar/prop dictionary (research/claude/16-e2e-production-blueprint.md), keyed by
# variable number; applies to both `evarN` and `post_evarN`. Labels are EDDL-derived
# annotations only (not logic) — a few eVars carry documented semantic conflicts
# (e.g. 107↔121 domain, 110↔185 platform); see doc-16 before trusting an edge label.
EVAR_LABELS = {
    101: "Page Name", 102: "Page Type", 103: "Site Type", 104: "Content Type",
    105: "Brand|Line of Business|Segment", 106: "Country|Region|City",
    107: "Full Page URL|Domain|Hash|Query|Path", 108: "User Agent", 109: "Language",
    110: "Platform", 121: "Domain (Page)", 122: "Login Step",
    126: "Download File Label", 127: "Download URL",
    131: "Anonymous ID (ECID)", 132: "Primary Member Customer ID",
    133: "Secondary Member Customer ID", 134: "Tertiary Member Customer ID",
    135: "Login Method", 136: "Email (hashed)",
    137: "Age|Gender|Spouse Age|Spouse Gender|#Dependents|Smoking",
    138: "User Type", 139: "User Sub-Type", 140: "Medallia UUID",
    144: "Navigation History", 145: "New vs Repeat",
    161: "Search Results", 162: "Search Keywords", 163: "Search Type",
    181: "Error Code", 182: "Error Description", 183: "Error Type",
    184: "Error Category", 185: "Platform", 189: "Link Region",
    191: "Form Name", 192: "Form Step", 193: "Link Click Name", 194: "Link Click Href",
    199: "Google ID (GLID)", 200: "OneTrust Categories-ID",
}
PROP_LABELS = {
    51: "Page Title", 52: "Page URL parts", 53: "Bot Detector", 54: "Language",
    55: "Previous Page / Referrer", 56: "Navigation Position", 57: "New vs Repeat",
}
_VAR_RE = re.compile(r"(?:post_)?(evar|prop)(\d+)$")
def dim_label(col):
    """EDDL semantic label for evarN/propN columns; '' when unknown."""
    m = _VAR_RE.match(str(col).lower())
    if not m:
        return ""
    n = int(m.group(2))
    return (EVAR_LABELS if m.group(1) == "evar" else PROP_LABELS).get(n, "")

# ------------------------------------------------------------ emit helpers ----
RESULTS = {}   # section_id -> payload (drives S12 consolidation)
SKIPPED = {}   # section_id -> reason

# Emit-time FORMATTING only (ADR-0007 §5 full-raw). Values are NOT redacted: the
# earlier email/IPv4 scrubbers were removed with the data-owner decision to profile
# identifiers raw. This keeps only display hygiene — truncate absurdly long strings so a
# single value can't blow the Databricks per-cell stdout cap, and round floats.
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
    """Single privacy chokepoint: every shareable output goes through here."""
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
    """F.col with backtick quoting — the schema has dotted column names
    (mobileappperformanceappid.*) that unquoted F.col parses as struct access."""
    return F.col("`" + col_name.replace("`", "``") + "`")

def nonblank(col_name):
    """Adobe feeds use empty strings, not NULLs."""
    c = qcol(col_name)
    return c.isNotNull() & (F.trim(c.cast("string")) != "")

def strip_query(u):
    return str(u).split("?")[0].split("#")[0]

def maybe_strip(u):
    """Strip URL query/fragment only when the strip_url_query widget is on; else raw."""
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

def resolve_date_expr(df):
    """Fallback chain for the canonical hit timestamp. Returns (Column, description)."""
    dtypes = dict(df.dtypes)
    if "date_time" in dtypes:
        if dtypes["date_time"] in ("timestamp", "date"):
            return F.col("date_time"), "date_time (typed)"
        return F.to_timestamp(F.col("date_time")), "to_timestamp(date_time)"
    if "hit_time_gmt" in dtypes:
        return F.from_unixtime(F.col("hit_time_gmt").cast("long")).cast("timestamp"), "from_unixtime(hit_time_gmt)"
    raise ValueError("No usable timestamp column (date_time / hit_time_gmt) found")

def pick_col(df, *candidates):
    """First candidate column present in the schema, else None."""
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

# CA-Retirement scope columns (resolved once against the schema).
RSID_COL = None   # report-suite column
URL_COL  = None   # page-URL column

URL_COLS = None   # page-URL columns present, in D4 coalesce order

def _resolve_scope_cols(df):
    global RSID_COL, URL_COL, URL_COLS
    RSID_COL = pick_col(df, "rsid", "report_suite", "reportsuite", "reportsuiteid", "post_rsid")
    # D4: page_url FIRST. post_page_url is blank 36.41% (manulifeglobalprod) /
    # 45.75% (manugrs) of the time vs <=0.013% for page_url, so preferring
    # post_page_url — as this notebook used to — silently drops ~40% of rows.
    have = set(df.columns)
    URL_COLS = [c for c in ("page_url", "post_page_url") if c in have]
    URL_COL  = URL_COLS[0] if URL_COLS else None

def like_any(colexpr, patterns):
    """Null-safe OR of SQL LIKE patterns. Returns None when `patterns` is empty.

    Blank/NULL input yields False, never NULL, so `~like_any(...)` stays
    well-defined — a NULL there would silently drop the row instead of keeping it.
    """
    if not patterns:
        return None
    m = None
    for p in patterns:
        m = colexpr.like(p) if m is None else (m | colexpr.like(p))
    return F.coalesce(m, F.lit(False))

def url_expr(df):
    """D4 blank-guarded coalesce(page_url, post_page_url), lowercased.

    Adobe writes empty strings, not NULLs, so a plain coalesce() returns "" from
    page_url and never falls through to post_page_url. Map blank -> NULL first,
    then coalesce, then land on "" so the NOT LIKE exclusions stay well-defined
    (a NULL would make ~like(...) NULL and silently drop the row).
    """
    if URL_COLS is None:
        _resolve_scope_cols(df)
    if not URL_COLS:
        return None
    parts = []
    for c in URL_COLS:
        t = F.trim(F.col(c).cast("string"))
        parts.append(F.when(t != F.lit(""), t))
    return F.lower(F.coalesce(*parts, F.lit("")))

def scope_condition(df):
    """CA-Retirement subset selector. Returns (Column|None, meta).

    rsid IN RSID_LIST (case-insensitive) AND the D4 coalesced URL matches any
    URL_INCLUDE pattern AND matches none of URL_EXCLUDE AND none of the D8
    individual-login hosts. Empty widget -> that condition is dropped. Missing
    schema column -> dropped and flagged in meta, so the run cannot silently
    profile the wrong population. NOTE: the include test excludes hits with a
    blank URL; that share is measured in S4 filter diagnostics.
    """
    if RSID_COL is None and URL_COLS is None:
        _resolve_scope_cols(df)
    conds, active, missing = [], [], []
    if RSID_LIST:
        if RSID_COL:
            conds.append(F.lower(F.trim(F.col(RSID_COL).cast("string"))).isin(RSID_LIST))
            active.append(f"rsid[{RSID_COL}] in {RSID_LIST}")
        else:
            missing.append("rsid (no report-suite column found)")
    u = url_expr(df)
    if u is None:
        if URL_INCLUDE or URL_EXCLUDE or LOGIN_EXCLUDE:
            missing.append("url (no page_url/post_page_url column found)")
    else:
        inc = like_any(u, URL_INCLUDE)
        if inc is not None:
            conds.append(inc)
            active.append(f"url[coalesce{tuple(URL_COLS)}] LIKE any {URL_INCLUDE}")
        exc = like_any(u, URL_EXCLUDE)
        if exc is not None:
            conds.append(~exc)
            active.append(f"url NOT LIKE any {URL_EXCLUDE}")
        # D8: individual-login / member-auth hosts, subtracted in EVERY mode.
        # An explicit host list, NOT a %portal% pattern: four of the six carry no
        # "portal" substring (id.manulife.ca alone is 62.6M hits) and the FR
        # "portail" spelling would not match one either.
        lex = like_any(u, LOGIN_EXCLUDE)
        if lex is not None:
            conds.append(~lex)
            active.append(f"login hosts excluded [D8]: {LOGIN_EXCLUDE}")
    cond = None
    for c in conds:
        cond = c if cond is None else (cond & c)
    meta = {"rsid_col": RSID_COL, "url_col": URL_COL, "url_cols_coalesced": URL_COLS,
            "rsid_list": RSID_LIST or None, "url_scope_mode": URL_SCOPE_MODE,
            "url_include": URL_INCLUDE or None, "url_exclude": URL_EXCLUDE or None,
            "login_host_exclude": LOGIN_EXCLUDE or None,
            "active_conditions": active, "missing_conditions": missing,
            "scoped": cond is not None}
    return cond, meta

# Globals populated by S1/S4; ensure_frames() rebuilds them for re-runs.
DF = None
DF_CA = None
DF_W = None
DF_S = None
DATE_EXPR = None
DATE_EXPR_DESC = None
WINDOW_START = None
WINDOW_END = None
SAMPLE_ROWS = None

def ensure_frames():
    """Make DF/DF_CA/DF_W/DF_S available even when a section is re-run standalone.
    DF = full table (S1/S2/S3). DF_CA = CA-Retirement subset; DF_W/DF_S derive from it."""
    global DF, DF_CA, DF_W, DF_S, DATE_EXPR, DATE_EXPR_DESC, WINDOW_START, WINDOW_END, SAMPLE_ROWS
    if DF is None:
        DF = spark.table(TABLE_FQN)
    if DATE_EXPR is None:
        DATE_EXPR, DATE_EXPR_DESC = resolve_date_expr(DF)
    if DF_CA is None:
        cond, _ = scope_condition(DF)
        DF_CA = DF.filter(cond) if cond is not None else DF
    if DF_W is None:
        if WINDOW_END is None:
            dv = RESULTS.get("daily_volume", {})
            WINDOW_END = datetime.date.fromisoformat(dv["ca_date_max"]) if dv.get("ca_date_max") else datetime.date.today()
        WINDOW_START = (WINDOW_END.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
        for _ in range(WINDOW_MONTHS - 1):
            WINDOW_START = (WINDOW_START - datetime.timedelta(days=1)).replace(day=1)
        DF_W = DF_CA.filter(F.to_date(DATE_EXPR) >= F.lit(WINDOW_START))
    if DF_S is None:
        DF_S = DF_W.sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=42)
        DF_S = DF_S.persist()      # always freeze: count() and per-column aggs must read identical rows
        SAMPLE_ROWS = DF_S.count() # materializes the persisted sample
    return DF, DF_W, DF_S

print(f"Config OK. table={TABLE_FQN} window={WINDOW_MONTHS}mo fraction={SAMPLE_FRACTION} "
      f"batch={COL_BATCH_SIZE} top_n={TOP_N} max_cols={MAX_PROFILED_COLS} emit_mode=raw-all")
print(f"Scope filter: rsid={RSID_LIST or '(off)'} url_mode={URL_SCOPE_MODE} "
      f"include={URL_INCLUDE or '(off)'} exclude={URL_EXCLUDE or '(off)'} "
      f"login_hosts_excluded={len(LOGIN_EXCLUDE)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## S1 — Unity Catalog discovery
# MAGIC Finds candidate adobe/hit/clickstream tables and verifies the configured table resolves.
# MAGIC Metadata-only; runs in seconds.

# COMMAND ----------

def s1_discovery():
    global DF
    candidates = []
    try:
        rows = spark.sql("""
            SELECT table_catalog, table_schema, table_name
            FROM system.information_schema.tables
            WHERE lower(table_name) RLIKE 'adobe|hit|clickstream'
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
        # Fallback: SHOW loops (capped) for workspaces without information_schema access.
        print(f"information_schema unavailable ({type(e).__name__}); falling back to SHOW loops")
        scanned = 0
        for cat_row in spark.sql("SHOW CATALOGS").collect():
            cat = cat_row[0]
            if cat in ("system", "samples") or scanned >= 200:
                continue
            try:
                schemas = spark.sql(f"SHOW SCHEMAS IN `{cat}`").collect()
            except Exception:
                continue
            for s_row in schemas:
                sch = s_row[0]
                if scanned >= 200:
                    break
                try:
                    tables = spark.sql(f"SHOW TABLES IN `{cat}`.`{sch}`").collect()
                except Exception:
                    continue
                for t_row in tables:
                    scanned += 1
                    tname = t_row.tableName
                    if re.search(r"adobe|hit|clickstream", tname, re.IGNORECASE):
                        candidates.append({"fqn": f"{cat}.{sch}.{tname}", "n_cols": None})

    resolves, n_cols_chosen, err, scope_meta = False, None, None, None
    try:
        DF = spark.table(TABLE_FQN)
        n_cols_chosen = len(DF.columns)
        resolves = True
        _resolve_scope_cols(DF)
        _, scope_meta = scope_condition(DF)
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)[:200]}"

    emit("uc_discovery", {
        "configured_table": TABLE_FQN,
        "resolves": resolves,
        "n_cols": n_cols_chosen,
        "resolve_error": err,
        "candidates": candidates[:30],
        "scope": scope_meta,
        "note": "If resolves=false, set the table_fqn widget to one of the candidates and re-run.",
    })

run_section("S1", s1_discovery)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S2 — Delta metadata & load cadence
# MAGIC `DESCRIBE DETAIL` + `DESCRIBE HISTORY`: freshness/arrival-cadence evidence with zero data scan.
# MAGIC (Closes open blocker #8 in 10-data-profile-alignment.md.)

# COMMAND ----------

def s2_delta_meta():
    detail = spark.sql(f"DESCRIBE DETAIL {TABLE_FQN}").collect()[0].asDict()
    # Deliberately exclude 'location' and free-form properties (internal paths).
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
            "available": True,
            "n_history_rows": len(hist),
            "ops_by_type": ops_by_type,
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
# MAGIC history depth, missing days, day-of-week profile, monthly totals (RRSP-season evidence),
# MAGIC biggest day-over-day jumps.

# COMMAND ----------

DAILY_ROWS = []       # [(date, ca_count)] — the CA subset series; drives S4/S8/S10
DAILY_TOTAL_ROWS = [] # [(date, total_count)] — whole-table series (chart/context)

def s3_daily_volume():
    global DF, DATE_EXPR, DATE_EXPR_DESC, WINDOW_END, DAILY_ROWS, DAILY_TOTAL_ROWS
    if DF is None:
        DF = spark.table(TABLE_FQN)
    DATE_EXPR, DATE_EXPR_DESC = resolve_date_expr(DF)
    _resolve_scope_cols(DF)
    cond, scope_meta = scope_condition(DF)
    ca_expr = F.when(cond, 1).otherwise(0) if cond is not None else F.lit(1)
    url_blank_expr = (F.when(~nonblank(URL_COL), 1).otherwise(0)
                      if URL_COL else F.lit(0))

    rows = (DF.select(F.to_date(DATE_EXPR).alias("d"),
                      ca_expr.alias("ca"), url_blank_expr.alias("urlblank"))
              .groupBy("d")
              .agg(F.count("*").alias("total"),
                   F.sum("ca").alias("ca"),
                   F.sum("urlblank").alias("urlblank"))
              .orderBy("d").collect())
    null_dates = sum(r["total"] for r in rows if r["d"] is None)
    per_day = [(r["d"], r["total"], r["ca"] or 0) for r in rows if r["d"] is not None]
    DAILY_TOTAL_ROWS = [(d, t) for d, t, _ in per_day]
    DAILY_ROWS = [(d, ca) for d, _, ca in per_day]     # CA subset series
    url_blank_total = sum(r["urlblank"] or 0 for r in rows)
    if not per_day:
        emit("daily_volume", {"error": "no non-null dates", "date_expr": DATE_EXPR_DESC})
        return

    total_all = sum(t for _, t, _ in per_day)
    total_ca = sum(ca for _, _, ca in per_day)

    # CA subset drives the calendar/seasonality stats
    ca_daily = [(d, ca) for d, ca in DAILY_ROWS if ca > 0]
    if not ca_daily:
        emit("daily_volume", {
            "error": "scope filter matched 0 rows — check rsid/url widgets and uc_discovery.scope",
            "date_expr": DATE_EXPR_DESC, "scope": scope_meta,
            "total_rows_all": total_all, "url_blank_rows": url_blank_total,
        })
        return
    dmin, dmax = ca_daily[0][0], ca_daily[-1][0]
    WINDOW_END = dmax   # subset may end earlier than the table
    ca_by_date = dict(DAILY_ROWS)

    # missing calendar days within the CA active range (zero-hit days included)
    missing = []
    d = dmin
    while d <= dmax:
        if ca_by_date.get(d, 0) == 0:
            missing.append(str(d))
        d += datetime.timedelta(days=1)

    # day-of-week means over the CA active range (Mon..Sun)
    dow_sum, dow_n = [0] * 7, [0] * 7
    for d, ca in DAILY_ROWS:
        if dmin <= d <= dmax:
            dow_sum[d.weekday()] += ca
            dow_n[d.weekday()] += 1
    dow_mean = [round(dow_sum[i] / dow_n[i]) if dow_n[i] else None for i in range(7)]

    # monthly totals (both series) — RRSP seasonality evidence
    monthly_ca, monthly_total = {}, {}
    for d, t, ca in per_day:
        monthly_ca[d.strftime("%Y-%m")] = monthly_ca.get(d.strftime("%Y-%m"), 0) + ca
        monthly_total[d.strftime("%Y-%m")] = monthly_total.get(d.strftime("%Y-%m"), 0) + t

    # top |log-ratio| day-over-day jumps on the CA series (level-shift candidates)
    jumps = []
    for (d0, c0), (d1, c1) in zip(ca_daily, ca_daily[1:]):
        if c0 > 0 and c1 > 0:
            jumps.append((abs(math.log(c1 / c0)), str(d1), c0, c1))
    jumps.sort(reverse=True)
    top_jumps = [{"date": j[1], "prev": j[2], "curr": j[3],
                  "ratio": round(j[3] / j[2], 3)} for j in jumps[:5]]

    # CSV: date,total_hits,ca_hits — full daily if it fits, else last WINDOW_MONTHS
    if len(per_day) <= MAX_CSV_LINES:
        csv_daily = [f"{d},{t},{ca}" for d, t, ca in per_day]
        csv_note = "full history, daily"
    else:
        cutoff = (dmax.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
        for _ in range(WINDOW_MONTHS - 1):
            cutoff = (cutoff - datetime.timedelta(days=1)).replace(day=1)
        csv_daily = [f"{d},{t},{ca}" for d, t, ca in per_day if d >= cutoff][:MAX_CSV_LINES]
        csv_note = f"daily since {cutoff}; older history in monthly_totals"

    emit("daily_volume", {
        "date_expr": DATE_EXPR_DESC,
        "scope": scope_meta,
        "total_rows_all": total_all,
        "total_rows_ca": total_ca,
        "ca_share_pct": round(100.0 * total_ca / max(total_all, 1), 3),
        "url_blank_rows": url_blank_total,
        "null_date_rows": null_dates,
        "ca_date_min": str(dmin), "ca_date_max": str(dmax),
        "n_ca_days_present": len(ca_daily),
        "n_ca_days_missing": len(missing),
        "missing_days": missing[:50],
        "dow_mean_ca_hits_mon_to_sun": dow_mean,
        "monthly_totals_ca": monthly_ca,
        "monthly_totals_all": monthly_total,
        "top5_day_over_day_jumps_ca": top_jumps,
        "csv_note": csv_note,
        "csv_header": "date,total_hits,ca_hits",
        "csv": csv_daily,
    })

run_section("S3", s3_daily_volume)

# COMMAND ----------

# chart for your own inspection (not part of the shareable output)
if DAILY_ROWS:
    display(spark.createDataFrame(
        [(str(d), t, ca) for (d, t), (_, ca) in zip(DAILY_TOTAL_ROWS, DAILY_ROWS)],
        ["date", "total_hits", "ca_hits"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## S4 — Profiling window + sample frames
# MAGIC Builds `df_w` (last N months of the CA subset) and `df_s` (random sample) used by
# MAGIC S5–S11; cross-checks the CA window count against S3 and emits filter diagnostics
# MAGIC (rsid-only / url-only / both matches, top report-suites) so a wrong filter fails loudly.

# COMMAND ----------

def s4_frames():
    global DF_CA, DF_W, DF_S, SAMPLE_ROWS
    DF_CA = None; DF_W = None; DF_S = None  # force rebuild with S3's date_max
    ensure_frames()
    window_rows = DF_W.count()   # DF_W is CA-scoped, so this is the CA window count
    s3_window_sum = sum(c for d, c in DAILY_ROWS if d >= WINDOW_START) if DAILY_ROWS else None

    # ---- filter diagnostics on the UNFILTERED window (rsid-only / url-only / both) ----
    cond, scope_meta = scope_condition(DF)
    raw_window = DF.filter(F.to_date(DATE_EXPR) >= F.lit(WINDOW_START))
    rsid_cond = (F.lower(F.trim(F.col(RSID_COL).cast("string"))).isin(RSID_LIST)
                 if (RSID_COL and RSID_LIST) else F.lit(False))
    _u = url_expr(DF)
    _inc = like_any(_u, URL_INCLUDE) if _u is not None else None
    url_cond = _inc if _inc is not None else F.lit(False)
    # blank on the D4 coalesce, not on one column: a row is only URL-blank when
    # BOTH page_url and post_page_url are empty.
    url_blank_cond = F.lit(False) if _u is None else (_u == F.lit(""))
    diag = raw_window.agg(
        F.count("*").alias("total"),
        F.sum(F.when(rsid_cond, 1).otherwise(0)).alias("rsid_match"),
        F.sum(F.when(url_cond, 1).otherwise(0)).alias("url_match"),
        F.sum(F.when(rsid_cond & url_cond, 1).otherwise(0)).alias("both"),
        F.sum(F.when(url_blank_cond, 1).otherwise(0)).alias("url_blank"),
    ).collect()[0]

    # top report-suite values (config identifiers, not PII) to catch value/casing mismatch
    top_rsids = []
    if RSID_COL:
        top_rsids = [{"rsid": (str(r[RSID_COL]) if r[RSID_COL] is not None else None),
                      "pct": round(100.0 * r["count"] / max(diag["total"], 1), 3)}
                     for r in (raw_window.groupBy(RSID_COL).count()
                                         .orderBy(F.desc("count")).limit(10).collect())]

    # Per-suite row counts INSIDE the final scope. D1 requires one run to cover both
    # suites, so a suite missing here means it contributed nothing and every downstream
    # section is silently single-suite — the failure mode this breakdown exists to catch.
    rsid_breakdown = []
    if RSID_COL:
        rsid_breakdown = [{"rsid": (str(r[RSID_COL]) if r[RSID_COL] is not None else None),
                           "rows": r["count"],
                           "pct_of_scope": round(100.0 * r["count"] / max(window_rows, 1), 3)}
                          for r in (DF_W.groupBy(RSID_COL).count()
                                        .orderBy(F.desc("count")).collect())]
        _seen = {(b["rsid"] or "").lower() for b in rsid_breakdown}
        _missing = [s for s in RSID_LIST if s not in _seen]
        if _missing:
            print(f"!!!!! WARNING: 0 rows in scope for rsid(s): {_missing} — "
                  f"downstream sections cover only {sorted(_seen & set(RSID_LIST))}")

    both = diag["both"] or 0
    warning = None
    if both == 0:
        warning = ("SCOPE FILTER MATCHED 0 ROWS in the window. Downstream sections would "
                   "profile an empty frame. Check window_frame.filter.top_rsids for the "
                   "actual rsid value/casing and adjust the rsid_list / url_scope_* widgets.")
        print("!!!!! " + warning)

    emit("window_frame", {
        "window_start": str(WINDOW_START), "window_end": str(WINDOW_END),
        "window_rows_ca": window_rows,
        "s3_crosscheck_sum_ca": s3_window_sum,
        "crosscheck_ok": (s3_window_sum == window_rows) if s3_window_sum is not None else None,
        "sample_fraction": SAMPLE_FRACTION,
        "sample_rows": SAMPLE_ROWS,
        "sample_cached": CACHE_SAMPLE,
        "filter": {
            **scope_meta,
            "window_total_rows": diag["total"],
            "rsid_only_match": diag["rsid_match"],
            "url_only_match": diag["url_match"],
            "both_match": both,
            "rsid_breakdown": rsid_breakdown,
            "url_blank_rows": diag["url_blank"],
            "url_blank_pct": round(100.0 * (diag["url_blank"] or 0) / max(diag["total"], 1), 3),
            "top_rsids": top_rsids,
            "warning": warning,
        },
    })

run_section("S4", s4_frames)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S4b — URL scope audit (column choice + excluded retirement volume)
# MAGIC Answers the two scope-filter review questions, measured on the **rsid-only** window
# MAGIC population (before the URL filter narrows it):
# MAGIC 1. **Which URL column?** blank %, cardinality, and today's-filter match rate for
# MAGIC    `post_page_url` vs `page_url`, plus where the two disagree — so the scope column is
# MAGIC    chosen on evidence, not by default.
# MAGIC 2. **What does the English filter exclude?** host + leading path segments (query strings
# MAGIC    stripped) flagging retirement traffic the current `…/ca/en/…group-retirement` substring
# MAGIC    misses — French `/ca/fr/`, alternate domains (e.g. `manulife-group-plans.ca`), other
# MAGIC    paths — with excluded-hit counts that show whether widening is worth a re-baseline.

# COMMAND ----------

# S4b runs on the rsid-only window (NOT url-scoped) so we can see what the current
# English URL filter drops. The breakdown is measured on the COMPLETE url —
# coalesce(page_url, post_page_url) — because post_page_url is ~37% blank on this report
# suite (EDA S4b), so measuring on it alone undercounts. Host/path only (query strings
# stripped) — no raw query strings surfaced (privacy guard).

def s4b_url_scope_audit():
    ensure_frames()
    _resolve_scope_cols(DF)
    raw_window = DF.filter(F.to_date(DATE_EXPR) >= F.lit(WINDOW_START))
    rsid_cond = (F.lower(F.trim(F.col(RSID_COL).cast("string"))).isin(RSID_LIST)
                 if (RSID_COL and RSID_LIST) else F.lit(True))
    url_cols = [c for c in ("page_url", "post_page_url") if pick_col(raw_window, c)]   # D4 order
    if not url_cols:
        emit("url_scope_audit", {"error": "no page_url / post_page_url column in source"})
        return

    pop = raw_window.filter(rsid_cond).select(*url_cols)
    if CACHE_SAMPLE:
        pop = pop.persist()

    def host_path(c):  # host + path; scheme and query/fragment removed
        u = F.regexp_replace(F.lower(F.col(c).cast("string")), r"^https?://", "")
        return F.regexp_extract(u, r"^([^?#]*)", 1)

    def matches(c):    # null-safe: blank/null URL -> False (never NULL), so ~matches() works
        m = like_any(host_path(c), URL_INCLUDE)
        return m if m is not None else F.lit(True)

    # ---- 1) column reconciliation: coverage, cardinality, today's-filter match ----
    exprs = [F.count("*").alias("rows")]
    for c in url_cols:
        exprs += [F.sum(F.when(nonblank(c), 1).otherwise(0)).alias(c + "_nb"),
                  F.approx_count_distinct(host_path(c)).alias(c + "_dist"),
                  F.sum(matches(c).cast("int")).alias(c + "_cur")]
    if len(url_cols) == 2:
        a, b = matches(url_cols[0]), matches(url_cols[1])   # null-safe -> disagreement reconciles
        exprs += [F.sum((a & b).cast("int")).alias("agree_both"),
                  F.sum((a & ~b).cast("int")).alias("only0"),
                  F.sum((~a & b).cast("int")).alias("only1")]
    r = pop.agg(*exprs).collect()[0]
    total = r["rows"] or 0
    reconciliation = {"rsid_only_rows": total, "current_url_include": URL_INCLUDE or None}
    for c in url_cols:
        nb = r[c + "_nb"] or 0
        reconciliation[c] = {
            "blank_pct": round(100.0 * (total - nb) / max(total, 1), 3),
            "approx_distinct": r[c + "_dist"],
            "rows_matching_current_filter": r[c + "_cur"],
            "pct_of_rsid_matched": round(100.0 * (r[c + "_cur"] or 0) / max(total, 1), 3),
        }
    if len(url_cols) == 2:
        reconciliation["disagreement_on_current_filter"] = {
            "both_match": r["agree_both"],
            "only_" + url_cols[0]: r["only0"],
            "only_" + url_cols[1]: r["only1"],
        }

    # ---- 2) host/path breakdown on the COMPLETE url (post_page_url is ~37% blank) ----
    complete = F.coalesce(*[F.col(c) for c in ("page_url", "post_page_url") if c in url_cols])
    u = F.regexp_replace(F.lower(complete.cast("string")), r"^https?://", "")
    hp = F.regexp_extract(u, r"^([^?#]*)", 1)
    host = F.regexp_extract(hp, r"^([^/]+)", 1)
    # noise = Adobe AEM authoring/staging hosts + non-CA (Philippines) paths -> not GWAM-CA traffic
    noise = host.rlike(r"adobeaemcloud") | hp.rlike(r"/ph/")
    _cur_m = like_any(hp, URL_INCLUDE)
    tag = pop.filter(hp != F.lit("")).select(
        F.regexp_extract(hp, r"^([^/]+(?:/[^/]+){0,4})", 1).alias("host_path"),
        (_cur_m if _cur_m is not None else F.lit(True)).alias("cur"),
        # CA retirement / group-plans section tokens (bare "/retirement" dropped -> excludes PH)
        hp.rlike(r"group-retirement|group-plans|regimes-collectif|retraite").alias("ret"),
        noise.alias("noise"),
        F.when(hp.rlike(r"/ca/fr/|/fr/|/fr-ca/"), "fr")
         .when(hp.rlike(r"/ca/en/|/en/|/en-ca/"), "en").otherwise("other").alias("lang"),
        (~host.rlike(r"(^|\.)manulife\.com$")).alias("alt"))
    if CACHE_SAMPLE:
        tag = tag.persist()

    addable = F.col("ret") & ~F.col("cur") & ~F.col("noise")   # real CA retirement, not yet in scope
    g = tag.agg(
        F.count("*").alias("n"),
        F.sum(F.col("cur").cast("int")).alias("cur"),
        F.sum(F.col("ret").cast("int")).alias("ret"),
        F.sum((F.col("ret") & ~F.col("noise")).cast("int")).alias("ret_clean"),
        F.sum(addable.cast("int")).alias("xret"),
        F.sum((addable & (F.col("lang") == "fr")).cast("int")).alias("xfr"),
        F.sum((addable & F.col("alt")).cast("int")).alias("xalt"),
        F.sum((F.col("ret") & F.col("noise")).cast("int")).alias("noise"),
    ).collect()[0]
    ret_clean = g["ret_clean"] or 0
    top_add = [{"host_path": x["host_path"], "hits": x["n"],
                "lang": x["lang"], "alt_domain": bool(x["alt"])}
               for x in (tag.filter(addable)
                            .groupBy("host_path", "lang", "alt").agg(F.count("*").alias("n"))
                            .orderBy(F.desc("n")).limit(TOP_N).collect())]

    emit("url_scope_audit", {
        "note": ("rsid-only window; breakdown on coalesce(page_url, post_page_url); host/path "
                 "only; noise = Adobe AEM author hosts + non-CA /ph/ paths, excluded from addable."),
        "scope_col_recommended": "coalesce(page_url, post_page_url)",
        "reconciliation": reconciliation,
        "breakdown": {
            "nonblank_url_rows": g["n"],
            "current_english_filter_rows": g["cur"],
            "retirement_related_rows": g["ret"],
            "retirement_related_excl_noise_rows": ret_clean,
            "addable_retirement_rows": g["xret"],
            "addable_pct_of_retirement": round(100.0 * (g["xret"] or 0) / max(ret_clean, 1), 3),
            "addable_french_rows": g["xfr"],
            "addable_alt_domain_rows": g["xalt"],
            "noise_rows_excluded": g["noise"],
        },
        "top_addable_retirement_host_paths": top_add,
    })
    display(spark.createDataFrame(
        top_add or [{"host_path": "(none)", "hits": 0, "lang": "-", "alt_domain": False}]))
    if CACHE_SAMPLE:
        pop.unpersist(); tag.unpersist()

run_section("S4b", s4b_url_scope_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S4c — Multi-URL-column & pagename retirement audit
# MAGIC Answers the manager's follow-up: `post_page_url` may not be the best column, and the data
# MAGIC holds many other "retirement" URLs. S4b compared only `post_page_url` vs `page_url`; S4c
# MAGIC widens the lens to **all five candidate URL columns** — `first_hit_page_url`, `page_url`,
# MAGIC `post_page_url`, `visit_start_page_url`, `site_url` — plus **`pagename`**, and adds a
# MAGIC **window-wide, rsid-agnostic** retirement sweep so we can size how much retirement traffic
# MAGIC exists **beyond** the two known suites. All measured on the rsid-only window (like S4b),
# MAGIC host/path only, no raw query strings.

# COMMAND ----------

# S4c reuses S4b's helpers and widgets (rsid_list / url_scope_*). Two retirement matchers:
#   RET_STRICT  = section-level tokens (aligns with S4b 'addable'; bare "/retirement" dropped so
#                 Philippines /ph/retirement is excluded).
#   RET_BROAD   = the manager's literal "retirement" keyword (retirement|retraite).
# The recommended scope column stays coalesce(page_url, post_page_url) (S4b); per-column we also
# report how much retirement traffic each column would add BEYOND that coalesce.

URL_CANDIDATES = ("first_hit_page_url", "page_url", "post_page_url",
                  "visit_start_page_url", "site_url")
RET_STRICT = r"group-retirement|group-plans|regimes-collectif|retraite"
RET_BROAD  = r"retirement|retraite"

def s4c_url_column_audit():
    ensure_frames()
    _resolve_scope_cols(DF)
    raw_window = DF.filter(F.to_date(DATE_EXPR) >= F.lit(WINDOW_START))
    rsid_cond = (F.lower(F.trim(F.col(RSID_COL).cast("string"))).isin(RSID_LIST)
                 if (RSID_COL and RSID_LIST) else F.lit(True))

    present = [c for c in URL_CANDIDATES if pick_col(raw_window, c)]
    if not present:
        emit("url_column_audit", {"error": "no candidate URL column in source"})
        return

    def hp(colexpr):   # host + path; scheme and query/fragment removed (privacy: no raw query)
        u = F.regexp_replace(F.lower(colexpr.cast("string")), r"^https?://", "")
        return F.regexp_extract(u, r"^([^?#]*)", 1)

    pop = raw_window.filter(rsid_cond)
    if CACHE_SAMPLE:
        pop = pop.persist()

    # recommended scope column = coalesce(page_url, post_page_url); blanks nulled so coalesce falls
    # through (Adobe uses empty strings, not NULLs). Guard when neither column exists.
    coal_cols = [c for c in ("page_url", "post_page_url") if c in present]
    coal_ret = None
    if coal_cols:
        complete = F.coalesce(*[F.when(nonblank(c), F.col(c)) for c in coal_cols])
        coal_ret = hp(complete).rlike(RET_STRICT)

    # ---- 1) per-URL-column stats ----
    exprs = [F.count("*").alias("rows")]
    for c in present:
        h = hp(F.col(c))
        exprs += [F.sum(F.when(nonblank(c), 1).otherwise(0)).alias(c + "_nb"),
                  F.approx_count_distinct(h).alias(c + "_dist"),
                  F.sum(h.rlike(RET_STRICT).cast("int")).alias(c + "_rs"),
                  F.sum(h.rlike(RET_BROAD).cast("int")).alias(c + "_rb")]
        if URL_INCLUDE:
            exprs += [F.sum(like_any(h, URL_INCLUDE).cast("int")).alias(c + "_cur")]
        if coal_ret is not None:
            exprs += [F.sum((h.rlike(RET_STRICT) & ~coal_ret).cast("int")).alias(c + "_missed")]
    rd = pop.agg(*exprs).collect()[0].asDict()
    total = rd["rows"] or 0
    per_col = {}
    for c in present:
        nb = rd[c + "_nb"] or 0
        per_col[c] = {
            "blank_pct": round(100.0 * (total - nb) / max(total, 1), 3),
            "approx_distinct": rd[c + "_dist"],
            "rows_matching_current_filter": (rd.get(c + "_cur") if URL_INCLUDE else None),
            "rows_matching_retirement_strict": rd[c + "_rs"],
            "rows_matching_retirement_broad": rd[c + "_rb"],
            "retirement_rows_beyond_coalesce": rd.get(c + "_missed"),
        }

    # ---- 2) pagename audit (pagename values are non-sensitive; S9 prints them) ----
    pn_col = pick_col(raw_window, "pagename", "post_pagename")
    pagename = {"present": False}
    if pn_col:
        pn = F.lower(F.col(pn_col).cast("string"))
        pr = pop.agg(
            F.sum(F.when(nonblank(pn_col), 1).otherwise(0)).alias("nb"),
            F.approx_count_distinct(pn).alias("dist"),
            F.sum(pn.rlike(RET_STRICT).cast("int")).alias("rs"),
            F.sum(pn.rlike(RET_BROAD).cast("int")).alias("rb"),
        ).collect()[0]
        top_pn = [{"pagename": x[pn_col], "hits": x["n"]}
                  for x in (pop.filter(nonblank(pn_col) & pn.rlike(RET_BROAD))
                               .groupBy(pn_col).agg(F.count("*").alias("n"))
                               .orderBy(F.desc("n")).limit(TOP_N).collect())]
        pagename = {
            "present": True, "col": pn_col,
            "blank_pct": round(100.0 * (total - (pr["nb"] or 0)) / max(total, 1), 3),
            "approx_distinct": pr["dist"],
            "rows_matching_retirement_strict": pr["rs"],
            "rows_matching_retirement_broad": pr["rb"],
            "top_retirement_pagenames": top_pn,
        }

    # ---- 3) window-wide, rsid-agnostic retirement sweep (which suites carry retirement?) ----
    sweep = {"available": False}
    if coal_ret is not None and RSID_COL:
        grp = (raw_window.filter(hp(complete).rlike(RET_STRICT))
                         .groupBy(RSID_COL).agg(F.count("*").alias("n")).collect())
        total_ret = sum((row["n"] or 0) for row in grp)
        top_rsids = [{"rsid": row[RSID_COL], "hits": row["n"],
                      "pct_of_window_retirement": round(100.0 * (row["n"] or 0) / max(total_ret, 1), 3)}
                     for row in sorted(grp, key=lambda x: -(x["n"] or 0))[:TOP_N]]
        sweep = {"available": True,
                 "basis": "full profiling window, all rsids, retirement_strict on coalesce(page_url,post_page_url)",
                 "total_retirement_rows_window": total_ret,
                 "current_scope_rsids": RSID_LIST or None,
                 "top_rsids_by_retirement_hits": top_rsids}

    emit("url_column_audit", {
        "note": ("rsid-only window for per-column + pagename; host/path only (no raw query); "
                 "retirement_strict = group-retirement|group-plans|regimes-collectif|retraite "
                 "(section tokens, excludes PH /retirement); retirement_broad = literal "
                 "retirement|retraite; rsid_sweep is window-wide across all suites."),
        "rsid_scope": {"rsid_col": RSID_COL, "rsid_list": RSID_LIST or None,
                       "url_scope_mode": URL_SCOPE_MODE, "url_include": URL_INCLUDE or None,
                       "rsid_only_rows": total},
        "recommended_scope_col": ("coalesce(" + ", ".join(coal_cols) + ")") if coal_cols else None,
        "columns_present": present,
        "per_url_column": per_col,
        "pagename": pagename,
        "rsid_retirement_sweep": sweep,
    })
    display(spark.createDataFrame([
        {"url_column": c, **per_col[c]} for c in present]))
    if CACHE_SAMPLE:
        pop.unpersist()

run_section("S4c", s4c_url_column_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S5 — Population census (~1,198 columns)
# MAGIC Which columns are actually populated? Batched non-blank counts on the sample,
# MAGIC then approx-distinct only for live columns. Never prints values.

# COMMAND ----------

CENSUS = {}   # col -> {"dtype":..., "pop_pct":..., "apx_distinct":...}; reused by S7/S9/S10
CORE_MIN_PCT = 99.0   # "core" = reliably populated (~always present); usable as a stable series

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
        "basis": "sample", "sample_rows": SAMPLE_ROWS,
        "n_total_cols": len(all_cols),
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
# MAGIC Event-ID frequency table (the raw material for the 12 post_event_list metric-registry
# MAGIC slots) + events-per-hit distribution. IDs need the event lookup / data dictionary to
# MAGIC name; standard commerce IDs labeled inline.

# COMMAND ----------

ADOBE_STD_EVENTS = {
    "1": "purchase", "2": "product_view", "10": "cart_open", "11": "checkout",
    "12": "cart_add", "13": "cart_remove", "14": "cart_view", "20": "campaign_view",
}

def _load_event_lookup(path):
    """Best-effort load of an event-ID -> label TSV (id<TAB>label per line). Returns {}
    if the path is blank or unreadable — the notebook then falls back to the inline map
    plus the Instance-of-eVar formula. Works from a Databricks Repo/Git folder or local."""
    if not path:
        return {}
    for cand in (path, "/Workspace/" + path.lstrip("/"), "./" + path):
        try:
            with open(cand, "r", encoding="utf-8") as fh:
                out = {}
                for line in fh:
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) >= 2 and parts[0].strip():
                        out[parts[0].strip()] = parts[1].strip()
                if out:
                    print(f"event lookup: loaded {len(out)} ids from {cand}")
                    return out
        except OSError:
            continue
    print(f"event lookup: {path} not readable — using inline map + eVar-instance formula")
    return {}

EVENT_LOOKUP = _load_event_lookup(EVENT_LOOKUP_PATH)

def decode_event(eid):
    """Resolve an Adobe post_event_list numeric ID to a label. Order: loaded TSV ->
    inline standard events -> 'Instance of eVarN' formula (100-199 -> eVar1-100,
    10000-10099 -> eVar101-200) -> unknown. Formula ranges verified against event.tsv."""
    e = str(eid)
    if e in EVENT_LOOKUP:
        return EVENT_LOOKUP[e]
    if e in ADOBE_STD_EVENTS:
        return ADOBE_STD_EVENTS[e]
    try:
        n = int(e)
    except (TypeError, ValueError):
        return "unknown — resolve via event lookup / data dictionary"
    if 100 <= n <= 199:
        return f"Instance of eVar{n - 99}"
    if 10000 <= n <= 10099:
        return f"Instance of eVar{n - 9899}"
    return "unknown — resolve via event lookup / data dictionary"

TOP_EVENT_IDS = []   # filled here; used by S8

def s6_event_decode():
    global TOP_EVENT_IDS
    ensure_frames()
    ev_col = pick_col(DF_S, "post_event_list", "event_list")
    if not ev_col:
        emit("event_decode", {"error": "no post_event_list/event_list column"})
        return

    events_arr = F.filter(
        F.transform(F.split(F.col(ev_col), ","), lambda x: F.trim(x)),
        lambda x: x != "",
    )
    base = DF_S.select(F.when(nonblank(ev_col), events_arr)
                        .otherwise(F.array().cast("array<string>")).alias("ev"))

    per_hit = base.agg(
        F.count("*").alias("hits"),
        F.sum(F.when(F.size("ev") > 0, 1).otherwise(0)).alias("hits_with_events"),
        F.expr("percentile_approx(size(ev), array(0.5, 0.95))").alias("pcts"),
        F.max(F.size("ev")).alias("max_events"),
    ).collect()[0]

    with_ev = base.filter(F.size("ev") > 0)
    # instances (every occurrence) vs hit-presence (array_distinct)
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
    freq = (inst.join(pres, "event_id", "outer")
                .orderBy(F.desc("hits_with")).limit(40).collect())

    hits = max(per_hit["hits"], 1)
    event_freq = []
    for r in freq:
        eid = r["event_id"]
        event_freq.append({
            "event_id": eid,
            "label": decode_event(eid),
            "hits_with_pct": round(100.0 * (r["hits_with"] or 0) / hits, 3),
            "instances": r["instances"],
            "has_value_pct": round(100.0 * (r["with_value"] or 0) / r["instances"], 2) if r["instances"] else None,
            "val_mean": r["val_mean"], "val_max": r["val_max"],
        })
    TOP_EVENT_IDS = [e["event_id"] for e in event_freq[:TOP_EVENTS_K]]

    emit("event_decode", {
        "basis": "sample", "source_col": ev_col, "sample_hits": per_hit["hits"],
        "pct_hits_with_events": round(100.0 * per_hit["hits_with_events"] / hits, 2),
        "events_per_hit_p50_p95": list(per_hit["pcts"]) if per_hit["pcts"] else None,
        "events_per_hit_max": per_hit["max_events"],
        "event_freq": event_freq,
    })

run_section("S6", s6_event_decode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S7 — Live eVars / props / campaign
# MAGIC Shape + RAW top-value distributions for every live custom dimension
# MAGIC (feeds the post_eVar registry slots and the synthetic generator; ADR-0007 §5).

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
            F.expr(f"percentile_approx(length({c}), 0.5)").alias("len_p50"),
            F.max(F.length(c)).alias("len_max"),
            F.avg(F.length(c)).alias("len_avg"),
            F.avg(F.when(F.col(c).cast("string").startswith("http"), 1.0).otherwise(0.0)).alias("url_frac"),
        ).collect()[0]
        top = (DF_S.filter(nonblank(c)).groupBy(c).count()
                   .orderBy(F.desc("count")).limit(TOP_N).collect())
        pop_rows = max(SAMPLE_ROWS * CENSUS[c]["pop_pct"] / 100.0, 1)
        out.append({
            "col": c,
            "label": dim_label(c),
            "pop_pct": CENSUS[c]["pop_pct"],
            "apx_distinct": CENSUS[c]["apx_distinct"],
            "len": {"p50": stats["len_p50"], "avg": stats["len_avg"], "max": stats["len_max"]},
            "looks_like_url": (stats["url_frac"] or 0) > 0.5,
            "free_text": (stats["len_avg"] or 0) > 80,
            # Raw values for every custom dim, identifiers included (ADR-0007 §5):
            # eVar/prop contents are business semantics (form steps, plan codes, tool
            # names) and are the whole point of profiling custom dims.
            "top": [{"v": str(r[c]), "len": len(str(r[c])),
                     "pct": round(100.0 * r["count"] / pop_rows, 2)} for r in top],
            "mode": "raw",
        })
    emit("live_custom_dims", {"basis": "sample", "n_live": len(live_all), "n_core": n_core, "dims": out})

run_section("S7", s7_live_custom_dims)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S8 — Time-series pack (exact, on the full window)
# MAGIC Daily hits/visits/visitors/clean-hits + per-day series for the top-K event IDs +
# MAGIC 7×24 day-of-week × hour profile. This is what the darts/pyod model design consumes
# MAGIC (weekly seasonality, lag-7/28 autocorrelation, volatility, level shifts).

# COMMAND ----------

TS_DAILY_PDF = None   # kept for the chart cell below

def s8_time_series():
    global TS_DAILY_PDF
    ensure_frames()
    vis_hi = pick_col(DF_W, "post_visid_high", "visid_high")
    vis_lo = pick_col(DF_W, "post_visid_low", "visid_low")
    visit_num = pick_col(DF_W, "visit_num")
    excl = pick_col(DF_W, "exclude_hit")

    aggs = [F.count("*").alias("hits")]
    if vis_hi and vis_lo and visit_num:
        aggs.append(F.approx_count_distinct(
            F.concat_ws(":", vis_hi, vis_lo, visit_num)).alias("visits"))
        aggs.append(F.approx_count_distinct(
            F.concat_ws(":", vis_hi, vis_lo)).alias("visitors"))
    if excl:
        aggs.append(F.sum(F.when(F.coalesce(F.expr(f"try_cast({excl} as int)"), F.lit(0)) == 0, 1)
                          .otherwise(0)).alias("clean_hits"))

    daily = (DF_W.groupBy(F.to_date(DATE_EXPR).alias("d")).agg(*aggs).orderBy("d").collect())
    cols = ["hits", "visits", "visitors", "clean_hits"]
    series = {c: [] for c in cols}
    dates = []
    for r in daily:
        if r["d"] is None:
            continue
        dates.append(r["d"])
        rd = r.asDict()
        for c in cols:
            series[c].append(rd.get(c))

    csv_daily = [",".join([str(d)] + [str(series[c][i]) if series[c][i] is not None else ""
                                      for c in cols]) for i, d in enumerate(dates)]

    # ---- per-day series for top-K event IDs (exact hit-presence counts) ----
    csv_events, ev_cols = [], []
    ev_col = pick_col(DF_W, "post_event_list", "event_list")
    if ev_col and TOP_EVENT_IDS:
        ev_daily = (DF_W.filter(nonblank(ev_col))
                    .select(F.to_date(DATE_EXPR).alias("d"),
                            F.explode(F.array_distinct(F.transform(
                                F.filter(F.transform(F.split(F.col(ev_col), ","), lambda x: F.trim(x)),
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

    # ---- hourly 7x24 profile over the last HOURLY_DAYS days ----
    hour_matrix = None
    if dates:
        h_start = dates[-1] - datetime.timedelta(days=HOURLY_DAYS)
        hourly = (DF_W.filter(F.to_date(DATE_EXPR) >= F.lit(h_start))
                  .select(F.to_date(DATE_EXPR).alias("d"), F.hour(DATE_EXPR).alias("h"))
                  .groupBy("d", "h").count()
                  .groupBy(F.dayofweek("d").alias("dow"), "h")
                  .agg(F.avg("count").alias("mean_hits")).collect())
        # dayofweek: 1=Sunday..7=Saturday -> reorder rows to Mon..Sun
        mat = [[0] * 24 for _ in range(7)]
        for r in hourly:
            mat[(r["dow"] + 5) % 7][r["h"]] = round(r["mean_hits"], 1)
        hour_matrix = mat

    # ---- driver-side stats ----
    profiles = {}
    try:
        import pandas as pd
        s = pd.Series(series["hits"], index=pd.to_datetime([str(d) for d in dates]))
        overall = s.mean()
        dow_idx = (s.groupby(s.index.dayofweek).mean() / overall).round(3)
        roll = s.rolling(7, center=True).median()
        shift_scores = (roll / roll.shift(7)).apply(
            lambda x: abs(math.log(x)) if x and x > 0 else 0)
        top_shifts = shift_scores.nlargest(5)
        profiles = {
            "cv": round(float(s.std() / overall), 4) if overall else None,
            "autocorr_lag7": round(float(s.autocorr(7)), 4) if len(s) > 14 else None,
            "autocorr_lag28": round(float(s.autocorr(28)), 4) if len(s) > 56 else None,
            "dow_index_mon_to_sun": [float(dow_idx.get(i, float("nan"))) for i in range(7)],
            "level_shift_candidates": [
                {"date": str(d.date()), "abs_log_ratio_wow": round(float(v), 3)}
                for d, v in top_shifts.items() if v > math.log(1.3)],
        }
        TS_DAILY_PDF = s.reset_index()
    except Exception as e:
        profiles = {"error": f"pandas stats failed: {type(e).__name__}: {str(e)[:150]}"}

    emit("ts_daily", {
        "basis": "exact_window",
        "csv_header": "date," + ",".join(cols),
        "csv": csv_daily[-MAX_CSV_LINES:],
        "visits_visitors_note": "approx_count_distinct (~5% rsd)" if vis_hi else "visid columns missing",
    })
    if csv_events:
        emit("ts_events", {
            "basis": "exact_window (hits containing event, not instances)",
            "csv_header": "date," + ",".join("ev" + e for e in ev_cols),
            "csv": csv_events[-MAX_CSV_LINES:],
        })
    emit("ts_profiles", {
        "hour_matrix_rows_mon_to_sun_cols_0_23h": hour_matrix,
        "hourly_days": HOURLY_DAYS,
        **profiles,
    })

run_section("S8", s8_time_series)

# COMMAND ----------

# chart for your own inspection
if TS_DAILY_PDF is not None:
    display(spark.createDataFrame(TS_DAILY_PDF.astype(str)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## S9 — Dimension candidates
# MAGIC Cardinality + top values for EVERY populated dimension (census-driven, not a fixed
# MAGIC allow-list; eVars/props are covered in S7). All values print raw, identifiers included
# MAGIC (ADR-0007 §5); URL/pagename values keep the full path, query strings raw by default.

# COMMAND ----------

def s9_dimensions():
    ensure_frames()
    # Census-driven: every populated column gets top-values, not a fixed allow-list
    # (ADR-0007 §5, comprehensive coverage). eVar/prop/campaign columns already carry
    # their values in S7, so skip them here to avoid duplication; everything else that
    # is populated is fair game. MAX_PROFILED_COLS / sample_fraction / top_n bound cost.
    KNOWN_DIMS = [
        "pagename", "post_pagename", "page_url", "post_page_url", "referrer",
        "ref_domain", "ref_type", "geo_country", "geo_region", "geo_city",
        "browser", "os", "connection_type", "language", "hit_source",
        "exclude_hit", "duplicate_purchase", "new_visit", "post_page_event",
        "va_closer_id",
    ]
    _s7_re = re.compile(r"post_evar\d+$|evar\d+$|post_prop\d+$|prop\d+$|^post_campaign$|^campaign$")
    cols_present = set(DF_S.columns)
    ordered = [c for c in KNOWN_DIMS if c in CENSUS and c in cols_present]
    extra = sorted((c for c in CENSUS
                    if c not in KNOWN_DIMS and c in cols_present and not _s7_re.match(c)),
                   key=lambda c: -CENSUS[c]["pop_pct"])
    dim_candidates = (ordered + extra)[:MAX_PROFILED_COLS]

    # Numeric Adobe lookup-ID dimensions: values are integer codes; decode needs the
    # feed's browser/os/languages/countries lookup tables (not shipped in this repo).
    LOOKUP_ID_DIMS = {"browser", "os", "language", "connection_type",
                      "geo_country", "geo_region", "geo_dma", "color", "javascript"}

    out = []
    for c in dim_candidates:
        is_url = ("url" in c) or c in ("referrer", "post_referrer")
        top = (DF_S.filter(nonblank(c)).groupBy(c).count()
                   .orderBy(F.desc("count")).limit(TOP_N * (3 if is_url else 1)).collect())
        pop_rows = max(SAMPLE_ROWS * CENSUS[c]["pop_pct"] / 100.0, 1)
        if is_url or c in ("pagename", "post_pagename"):
            # Full URL/pagename. Query strings profile RAW by default; the
            # strip_url_query widget strips them (they can carry session tokens, though
            # login/auth hosts are already excluded from scope).
            top_vals = [{"v": maybe_strip(r[c]), "pct": round(100.0 * r["count"] / pop_rows, 2)}
                        for r in top[:TOP_N]]
            mode = "raw, query-stripped" if STRIP_URL_QUERY else "raw"
        else:
            # Every other populated column, identifiers included (ADR-0007 §5).
            top_vals = [{"v": str(r[c]), "pct": round(100.0 * r["count"] / pop_rows, 2)}
                        for r in top[:TOP_N]]
            mode = "raw"
        out.append({"dim": c, "mode": mode, "label": dim_label(c),
                    "coverage_pct": CENSUS[c]["pop_pct"],
                    "apx_distinct": CENSUS[c]["apx_distinct"],
                    "top": top_vals,
                    "note": ("numeric lookup-ID code" if c in LOOKUP_ID_DIMS else "")})
    emit("dim_candidates", {"basis": "sample", "n_dims": len(out), "dims": out})

run_section("S9", s9_dimensions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S10 — Data-quality baseline
# MAGIC Bot-filter distributions (exclude_hit × hit_source), clock skew, duplicate rate
# MAGIC (exact, one recent day), late-arrival evidence if a load-timestamp column exists.

# COMMAND ----------

def s10_dq_baseline():
    ensure_frames()
    key_cols = [c for c in ["date_time", "hit_time_gmt", "visit_num", "visit_page_num",
                            "post_event_list", "pagename", "page_url", "exclude_hit",
                            "hit_source"] if c in set(DF_S.columns)]
    key_nulls = {c: round(100.0 - CENSUS.get(c, {}).get("pop_pct", 0.0), 3) if c in CENSUS
                 else 100.0 for c in key_cols}

    # exclude_hit x hit_source (bot filtering rules)
    dist = []
    if pick_col(DF_S, "exclude_hit") and pick_col(DF_S, "hit_source"):
        dist = [{"exclude_hit": str(r["exclude_hit"]), "hit_source": str(r["hit_source"]),
                 "pct": round(100.0 * r["count"] / max(SAMPLE_ROWS, 1), 3)}
                for r in (DF_S.groupBy("exclude_hit", "hit_source").count()
                              .orderBy(F.desc("count")).limit(20).collect())]

    # clock skew: date_time (local) vs hit_time_gmt (epoch) -> tz offset + stragglers
    skew = None
    if pick_col(DF_S, "hit_time_gmt"):
        skew_row = (DF_S.filter(nonblank("hit_time_gmt"))
                    .select((F.unix_timestamp(DATE_EXPR)
                             - F.col("hit_time_gmt").cast("long")).alias("skew_s"))
                    .agg(F.expr("percentile_approx(skew_s, array(0.05, 0.5, 0.95))")
                         .alias("p")).collect()[0])
        skew = {"p5_p50_p95_seconds": list(skew_row["p"]) if skew_row["p"] else None,
                "note": "constant offset = timezone of date_time; spread = clock skew"}

    # duplicates: exact on the most recent complete day
    dup = None
    vis_hi = pick_col(DF_S, "post_visid_high", "visid_high")
    vis_lo = pick_col(DF_S, "post_visid_low", "visid_low")
    seq = pick_col(DF_S, "visit_page_num", "hit_time_gmt")
    ca_days = [d for d, ca in DAILY_ROWS if ca > 0]
    if ca_days and vis_hi and vis_lo and pick_col(DF_S, "visit_num") and seq:
        check_day = ca_days[-2] if len(ca_days) >= 2 else ca_days[-1]
        day_df = DF_CA.filter(F.to_date(DATE_EXPR) == F.lit(check_day))
        total = day_df.count()
        distinct = day_df.select(vis_hi, vis_lo, "visit_num", seq).distinct().count()
        dup = {"day": str(check_day), "rows": total, "distinct_keys": distinct,
               "dup_pct": round(100.0 * (total - distinct) / max(total, 1), 4),
               "key": f"{vis_hi},{vis_lo},visit_num,{seq}", "basis": "exact_one_day"}

    # late-arrival: look for a load/ingest timestamp column
    load_cols = [c for c in DF_S.columns
                 if re.search(r"(load|ingest|etl|insert|_created|processed).*(ts|time|date)|_ts$",
                              c, re.IGNORECASE)]
    late = {"load_timestamp_cols_found": load_cols[:10]}
    if load_cols:
        lc = load_cols[0]
        try:
            late_row = (DF_S.filter(nonblank(lc))
                        .select(F.datediff(F.to_date(qcol(lc).cast("timestamp")),
                                           F.to_date(DATE_EXPR)).alias("lag_days"))
                        .agg(F.expr("percentile_approx(lag_days, array(0.5, 0.95, 0.99))")
                             .alias("p")).collect()[0])
            late["lag_days_p50_p95_p99"] = list(late_row["p"]) if late_row["p"] else None
            late["col_used"] = lc
        except Exception as e:
            late["error"] = f"{type(e).__name__}: {str(e)[:150]}"
    else:
        late["note"] = "no load-timestamp column; use S2 write cadence as arrival evidence"

    emit("dq_baseline", {
        "basis": "sample (dup check exact on one day)",
        "key_col_null_blank_pct": key_nulls,
        "exclude_hit_x_hit_source_pct": dist,
        "clock_skew": skew,
        "duplicates": dup,
        "late_arrival": late,
    })

run_section("S10", s10_dq_baseline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S11 — Identity evidence (ADR-0007)
# MAGIC Shape-only stats for every identity column: validates doc-11 §3 findings
# MAGIC (cust_visid all-null? userid cardinality-1?) on the GWAM table. No values printed.

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
            F.approx_count_distinct(F.col(c)).alias("apx_distinct"),
            F.min(F.when(nonblank(c), F.length(F.col(c).cast("string")))).alias("len_min"),
            F.avg(F.when(nonblank(c), F.length(F.col(c).cast("string")))).alias("len_avg"),
            F.max(F.when(nonblank(c), F.length(F.col(c).cast("string")))).alias("len_max"),
        ).collect()[0]
        out.append({"col": c,
                    "null_blank_pct": round(100.0 * r["null_blank_frac"], 3),
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
        "note": "identity-column cardinality/null evidence; raw identifier values are in "
                "S7/S9 per ADR-0007 §5 (full-raw)",
        "columns": out, "flags": flags, "daily_ratios": ratios,
    })

run_section("S11", s11_identity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S12 — Synthesis spec (master paste-back artifact)
# MAGIC Consolidates all prior sections into one machine-readable spec for the synthetic
# MAGIC data generator. Tolerant of skipped sections.

# COMMAND ----------

def s12_synthesis_spec():
    expected = ["uc_discovery", "delta_meta", "daily_volume", "window_frame",
                "population_census", "event_decode", "live_custom_dims",
                "ts_daily", "ts_events", "ts_profiles", "dim_candidates",
                "dq_baseline", "identity_evidence"]
    missing = [s for s in expected if s not in RESULTS]

    census = RESULTS.get("population_census", {})
    live_dims = {d["col"]: d for d in RESULTS.get("live_custom_dims", {}).get("dims", [])}
    schema_spec = []
    for entry in census.get("populated", []):
        col = entry["col"]
        spec = {"col": col, "dtype": entry.get("dtype"),
                "pop_pct": entry.get("pop_pct"), "apx_distinct": entry.get("apx_distinct"),
                "label": dim_label(col)}
        if col in live_dims:
            spec["len"] = live_dims[col].get("len")
            spec["top_values"] = live_dims[col].get("top")
        schema_spec.append(spec)

    dv = RESULTS.get("daily_volume", {})
    prof = RESULTS.get("ts_profiles", {})
    _, scope_meta = scope_condition(DF) if DF is not None else (None, None)
    emit("synthesis_spec", {
        "meta": {
            "table": TABLE_FQN,
            "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "scope": {"rsid_list": RSID_LIST or None, "url_scope_mode": URL_SCOPE_MODE,
                      "url_include": URL_INCLUDE or None, "url_exclude": URL_EXCLUDE or None,
                      "login_host_exclude": LOGIN_EXCLUDE or None,
                      "rsid_col": (scope_meta or {}).get("rsid_col"),
                      "url_col": (scope_meta or {}).get("url_col"),
                      "ca_share_pct": dv.get("ca_share_pct")},
            "window": {"start": str(WINDOW_START), "end": str(WINDOW_END),
                       "months": WINDOW_MONTHS},
            "sample_fraction": SAMPLE_FRACTION, "sample_rows": SAMPLE_ROWS,
            "sections_missing": missing, "sections_skipped": SKIPPED,
        },
        "volume": {
            "total_rows": dv.get("total_rows_ca"),
            "date_min": dv.get("ca_date_min"), "date_max": dv.get("ca_date_max"),
            "monthly_totals": dv.get("monthly_totals_ca"),
            "dow_mean_hits_mon_to_sun": dv.get("dow_mean_ca_hits_mon_to_sun"),
            "missing_days": dv.get("n_ca_days_missing"),
            "cv": prof.get("cv"),
            "autocorr_lag7": prof.get("autocorr_lag7"),
            "autocorr_lag28": prof.get("autocorr_lag28"),
            "dow_index": prof.get("dow_index_mon_to_sun"),
            "hour_matrix": prof.get("hour_matrix_rows_mon_to_sun_cols_0_23h"),
            "level_shifts": prof.get("level_shift_candidates"),
        },
        "series_ref": "daily values in the ts_daily / ts_events shareable blocks",
        "schema": schema_spec,
        "events": RESULTS.get("event_decode", {}),
        "dims": RESULTS.get("dim_candidates", {}).get("dims"),
        "dq": RESULTS.get("dq_baseline", {}),
        "identity": RESULTS.get("identity_evidence", {}),
    })

run_section("S12", s12_synthesis_spec)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run manifest — integrity check for the export
# MAGIC Byte length + sha1 of every shareable section, computed from the exact JSON that
# MAGIC was printed. Databricks caps very large per-cell stdout; re-hash any pasted or
# MAGIC exported block and compare here to prove nothing was truncated.

# COMMAND ----------

def s_run_manifest():
    sections = {}
    for sid, payload in RESULTS.items():
        body = json.dumps(payload, separators=(",", ":"), default=str)
        # Plain sha1 now that the emit scrubber no longer redacts hex-id runs
        # (ADR-0007 §5 full-raw). Re-hash any pasted block and compare to prove nothing
        # was truncated.
        sections[sid] = {"bytes": len(body),
                         "sha1": hashlib.sha1(body.encode("utf-8")).hexdigest()}
    emit("run_manifest", {"sections": sections, "n_sections": len(sections),
                          "skipped": SKIPPED})

run_section("run_manifest", s_run_manifest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done — how to hand back the results
# MAGIC
# MAGIC **Primary: export the run notebook itself.** After a full Run All, use
# MAGIC `File → Export → IPython Notebook (.ipynb)` and drop the file in the repo. Every
# MAGIC `===== BEGIN SHAREABLE: <id> =====` block is captured in the cell outputs, so nothing
# MAGIC has to be copied by hand. Export the interactive-charts notebook the same way — its
# MAGIC `chart:<id>` blocks carry the aggregate data behind each panel.
# MAGIC
# MAGIC **Verify nothing was truncated.** The final `run_manifest` block (and `chart:manifest`
# MAGIC in the charts notebook) lists the byte length + sha1 of every section; re-hash an
# MAGIC exported block and compare. If a block is cut mid-JSON, re-run just that section (each
# MAGIC `run_section` is independent) or lower the `max_csv_lines` widget.
# MAGIC
# MAGIC **Fallback — copy blocks by hand** (if you can't export), in this priority order:
# MAGIC
# MAGIC 1. `synthesis_spec` (the consolidated master — if you only send one thing, send this)
# MAGIC 2. `ts_daily` + `ts_events` (daily series for model design)
# MAGIC 3. `event_decode` + `live_custom_dims` + `population_census` (metric-registry seeding)
# MAGIC 4. `daily_volume` + `delta_meta` (volume/cadence evidence)
# MAGIC 5. `dim_candidates` + `dq_baseline` + `identity_evidence` + `uc_discovery` + `window_frame`
# MAGIC
# MAGIC Nothing in these blocks contains raw identifier values, full URLs with query strings,
# MAGIC or unmasked high-cardinality values (ADR-0007). If a block looks like it leaks anything,
# MAGIC don't paste it — flag it instead so the notebook can be tightened.
# MAGIC
# MAGIC **Scope reminder:** everything except `delta_meta` and the `total_hits` column of
# MAGIC `daily_volume` describes the CA-Retirement subset. First sanity check on any run:
# MAGIC `window_frame.filter.both_match` must be > 0 and `top_rsids` must list BOTH expected
# MAGIC suites (`manugrs`, `manulifeglobalprod`) — if not, fix the `rsid_list` /
# MAGIC `url_scope_*` widgets. `rsid_breakdown` must also show a non-zero row count for each.
