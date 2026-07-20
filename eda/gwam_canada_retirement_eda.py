# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — GWAM Canada Retirement EDA
# MAGIC
# MAGIC **Purpose.** Read-only exploratory profiling of the Adobe Analytics hit-level table
# MAGIC (`gwam_prod_catalog.inv_typed_common.adobe_hit_data`, provisional) to:
# MAGIC
# MAGIC **Scope.** The table holds ALL GWAM Adobe data. CA Retirement is the subset with
# MAGIC `rsid = 'manulifeglobalprod'` AND page URL containing
# MAGIC `manulife.com/ca/en/personal/group-plans/group-retirement` (both are widgets).
# MAGIC All profiling sections (S4–S11) and the synthesis spec describe this subset;
# MAGIC S3 reports total-table vs subset daily volume side by side.
# MAGIC 1. Fill evidence gaps: volume, history depth, load cadence, schema population census.
# MAGIC 2. Discover real metric candidates (post_event_list event IDs, live eVars/props).
# MAGIC 3. Capture time-series shape (seasonality, volatility) for anomaly-model design.
# MAGIC 4. Produce a machine-readable **synthesis spec** for generating synthetic data.
# MAGIC
# MAGIC **Data visibility (ADR-0007 §5).** Business data profiles **raw and in full** — eVars,
# MAGIC props, events, URLs, pagenames, campaigns, referrers. Comprehensive EDA needs real
# MAGIC values, and this feed carries no person-level identifier (`cust_visid` wholly NULL,
# MAGIC `userid` a single constant — ADR-0007 Context §2).
# MAGIC
# MAGIC The one exception is direct device/network identifiers (visitor IDs, cookies, IPs,
# MAGIC geo_zip, user_agent), reported shape-only (null %, cardinality, length stats) — because
# MAGIC their raw values carry no analytical signal, not because EDA is restricted. URL query
# MAGIC strings are stripped (session tokens live there); paths and hosts print in full.
# MAGIC
# MAGIC **How to run.** Attach to a cluster (DBR 13+ recommended), review the widgets that
# MAGIC appear at the top after running the CONFIG cell, then Run All. Each section prints a
# MAGIC `===== BEGIN SHAREABLE: <id> =====` block — copy those blocks back. Sections are
# MAGIC independent: a failure prints `SKIPPED` and the run continues.

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
dbutils.widgets.text("top_n", "15", "5. Top-N cap for value lists")
dbutils.widgets.text("hourly_days", "35", "6. Days for hourly profile")
dbutils.widgets.text("max_csv_lines", "450", "7. Max CSV lines per shareable block")
dbutils.widgets.text("top_events_k", "6", "8. Top-K events for daily series")
dbutils.widgets.text("cache_sample", "false", "9. Persist sample df (true/false)")
dbutils.widgets.text("rsid_filter", "manulifeglobalprod", "10. rsid filter (empty = off)")
dbutils.widgets.text("url_filter", "manulife.com/ca/en/personal/group-plans/group-retirement", "11. URL contains filter (empty = off)")

TABLE_FQN      = dbutils.widgets.get("table_fqn").strip()
WINDOW_MONTHS  = int(dbutils.widgets.get("window_months"))
SAMPLE_FRACTION = float(dbutils.widgets.get("sample_fraction"))
COL_BATCH_SIZE = int(dbutils.widgets.get("col_batch_size"))
TOP_N          = int(dbutils.widgets.get("top_n"))
HOURLY_DAYS    = int(dbutils.widgets.get("hourly_days"))
MAX_CSV_LINES  = int(dbutils.widgets.get("max_csv_lines"))
TOP_EVENTS_K   = int(dbutils.widgets.get("top_events_k"))
CACHE_SAMPLE   = dbutils.widgets.get("cache_sample").strip().lower() == "true"
RSID_FILTER    = dbutils.widgets.get("rsid_filter").strip().lower()
URL_FILTER     = dbutils.widgets.get("url_filter").strip().lower()

# ------------------------------------------------------- privacy constants ----
# ADR-0007 §5 (analysis-time visibility). EDA runs inside the governed Databricks
# workspace against a feed that carries NO person-level identifier: cust_visid /
# post_cust_visid are wholly NULL and userid is a single constant account value
# (cardinality 1) — confirmed with the data owner 2026-07-04, ADR-0007 Context §2.
#
# Default is therefore RAW: business dimensions (eVars, props, events, URLs,
# pagenames, campaigns, referrers, search terms) profile at full fidelity. Masking
# them to sha1 tokens destroyed the analytical signal without protecting a person —
# prior runs emitted whole sections of `<masked:xxxxxxxx>` that nobody could read.
#
# The residual exception is direct device/network identifiers, reported SHAPE ONLY
# (null %, cardinality, length stats). The reason is analytical, not bureaucratic:
# their raw values carry no signal. What EDA needs from `mcvisid` is "412k distinct,
# 0.2% null" — never a specific cookie value. Shape-only costs nothing here while
# keeping re-identifiable values out of blocks that get copied out of the workspace.
DIRECT_IDENTIFIERS = {
    # visitor/device identifiers -> HMAC-pseudonymized in the pipeline (ADR-0007 §2)
    "mcvisid", "visid_high", "visid_low", "post_visid_high", "post_visid_low",
    "cust_visid", "post_cust_visid", "cookies", "post_cookies", "persistent_cookie",
    # network addresses
    "ip", "ip2", "ipv6",
    # fine geo + device fingerprint — quasi-identifiers, re-identifying in combination
    "geo_zip", "post_zip", "zip", "user_agent",
}

def is_sensitive(col_name):
    """True only for direct/quasi identifiers. Everything else profiles raw.

    Deliberately an exact-match set, not a regex: the previous pattern net matched
    `guid|token|mcid|aamid|zip$|social` and swept in business columns that were never
    identifiers, which is a large part of why so much output came back masked.
    """
    return col_name.lower() in DIRECT_IDENTIFIERS

# ------------------------------------------------------------ emit helpers ----
RESULTS = {}   # section_id -> payload (drives S12 consolidation)
SKIPPED = {}   # section_id -> reason

# Last-resort net for values that should never appear in an analytics dimension at
# all. Deliberately minimal now: the previous version also redacted any 10+ digit run
# and any 24+ hex run, which silently destroyed hit counts, epoch timestamps, order
# IDs and event codes — and truncated at 160 chars, cutting long URLs mid-path.
_SCRUB_PATTERNS = [
    (re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"), "<redacted:email>"),
    (re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"), "<redacted:ipv4>"),
]
MAX_EMIT_STR = 2000

def _scrub_str(s):
    if len(s) > MAX_EMIT_STR:
        s = s[:MAX_EMIT_STR] + "...<trunc>"
    for pat, repl in _SCRUB_PATTERNS:
        s = pat.sub(repl, s)
    return s

def _scrub(obj):
    """Walk a payload: truncate strings, redact email/IP/long-ID lookalikes, round floats."""
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
def mask(v):
    """SHA1-truncated token; matches new_data/generated_data_profile.json format."""
    return "<masked:" + hashlib.sha1(str(v).encode("utf-8")).hexdigest()[:8] + ">"

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

def _resolve_scope_cols(df):
    global RSID_COL, URL_COL
    RSID_COL = pick_col(df, "rsid", "report_suite", "reportsuite", "reportsuiteid", "post_rsid")
    URL_COL  = pick_col(df, "post_page_url", "page_url")

def scope_condition(df):
    """CA-Retirement subset selector. Returns (Column|None, meta).

    rsid == RSID_FILTER (case-insensitive) AND page URL CONTAINS URL_FILTER.
    Either widget empty -> that condition is dropped. Missing schema column ->
    that condition is dropped and flagged in meta (so the run doesn't silently
    profile the wrong population). NOTE: the URL 'contains' test excludes hits
    with a blank page_url; that share is measured in S4 filter diagnostics.
    """
    if RSID_COL is None and URL_COL is None:
        _resolve_scope_cols(df)
    conds, active, missing = [], [], []
    if RSID_FILTER:
        if RSID_COL:
            conds.append(F.lower(F.trim(F.col(RSID_COL).cast("string"))) == F.lit(RSID_FILTER))
            active.append(f"rsid[{RSID_COL}]=={RSID_FILTER}")
        else:
            missing.append("rsid (no report-suite column found)")
    if URL_FILTER:
        if URL_COL:
            conds.append(F.lower(F.col(URL_COL).cast("string")).contains(URL_FILTER))
            active.append(f"url[{URL_COL}] contains {URL_FILTER}")
        else:
            missing.append("url (no page_url column found)")
    cond = None
    for c in conds:
        cond = c if cond is None else (cond & c)
    meta = {"rsid_col": RSID_COL, "url_col": URL_COL,
            "rsid_filter": RSID_FILTER or None, "url_filter": URL_FILTER or None,
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
      f"batch={COL_BATCH_SIZE} top_n={TOP_N} shape_only_cols={len(DIRECT_IDENTIFIERS)}")
print(f"Scope filter: rsid={RSID_FILTER or '(off)'} url_contains={URL_FILTER or '(off)'}")

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
    rsid_cond = (F.lower(F.trim(F.col(RSID_COL).cast("string"))) == F.lit(RSID_FILTER)
                 if (RSID_COL and RSID_FILTER) else F.lit(False))
    url_cond = (F.lower(F.col(URL_COL).cast("string")).contains(URL_FILTER)
                if (URL_COL and URL_FILTER) else F.lit(False))
    url_blank_cond = (~nonblank(URL_COL)) if URL_COL else F.lit(False)
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

    both = diag["both"] or 0
    warning = None
    if both == 0:
        warning = ("SCOPE FILTER MATCHED 0 ROWS in the window. Downstream sections would "
                   "profile an empty frame. Check window_frame.filter.top_rsids for the "
                   "actual rsid value/casing and adjust the rsid_filter / url_filter widgets.")
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
    rsid_cond = (F.lower(F.trim(F.col(RSID_COL).cast("string"))) == F.lit(RSID_FILTER)
                 if (RSID_COL and RSID_FILTER) else F.lit(True))
    url_cols = [c for c in ("post_page_url", "page_url") if pick_col(raw_window, c)]
    if not url_cols:
        emit("url_scope_audit", {"error": "no post_page_url / page_url column in source"})
        return
    cur = (URL_FILTER or "").lower()

    pop = raw_window.filter(rsid_cond).select(*url_cols)
    if CACHE_SAMPLE:
        pop = pop.persist()

    def host_path(c):  # host + path; scheme and query/fragment removed
        u = F.regexp_replace(F.lower(F.col(c).cast("string")), r"^https?://", "")
        return F.regexp_extract(u, r"^([^?#]*)", 1)

    def matches(c):    # null-safe: blank/null URL -> False (never NULL), so ~matches() works
        return F.coalesce(host_path(c).contains(cur), F.lit(False)) if cur else F.lit(True)

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
    reconciliation = {"rsid_only_rows": total, "current_url_filter": URL_FILTER or None}
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
    tag = pop.filter(hp != F.lit("")).select(
        F.regexp_extract(hp, r"^([^/]+(?:/[^/]+){0,4})", 1).alias("host_path"),
        (hp.contains(cur) if cur else F.lit(True)).alias("cur"),
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

# S4c reuses S4b's helpers and widgets (rsid_filter / url_filter). Two retirement matchers:
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
    rsid_cond = (F.lower(F.trim(F.col(RSID_COL).cast("string"))) == F.lit(RSID_FILTER)
                 if (RSID_COL and RSID_FILTER) else F.lit(True))
    cur = (URL_FILTER or "").lower()

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
        if cur:
            exprs += [F.sum(F.coalesce(h.contains(cur), F.lit(False)).cast("int")).alias(c + "_cur")]
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
            "rows_matching_current_filter": (rd.get(c + "_cur") if cur else None),
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
                 "current_scope_rsid": RSID_FILTER or None,
                 "top_rsids_by_retirement_hits": top_rsids}

    emit("url_column_audit", {
        "note": ("rsid-only window for per-column + pagename; host/path only (no raw query); "
                 "retirement_strict = group-retirement|group-plans|regimes-collectif|retraite "
                 "(section tokens, excludes PH /retirement); retirement_broad = literal "
                 "retirement|retraite; rsid_sweep is window-wide across all suites."),
        "rsid_scope": {"rsid_col": RSID_COL, "rsid_filter": RSID_FILTER or None,
                       "url_filter": URL_FILTER or None, "rsid_only_rows": total},
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
        "populated": [{"col": c, **v} for c, v in ranked[:120]],
        "populated_names_beyond_top120": [c for c, _ in ranked[120:]],
        "sparse_cols": sparse[:40],
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
    "12": "cart_add", "13": "cart_remove", "14": "cart_view",
}
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
            "label": ADOBE_STD_EVENTS.get(eid, "unknown — resolve via event lookup / data dictionary"),
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
# MAGIC Shape + masked top-value distributions for the live custom dimensions
# MAGIC (feeds the 8 post_eVar registry slots and the synthetic generator).

# COMMAND ----------

def s7_live_custom_dims():
    ensure_frames()
    live_all = [c for c in CENSUS
                if re.match(r"post_evar\d+$|evar\d+$|post_prop\d+$|prop\d+$|^post_campaign$|^campaign$", c)]
    live = sorted(live_all, key=lambda c: -CENSUS[c]["pop_pct"])[:25]
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
            "pop_pct": CENSUS[c]["pop_pct"],
            "apx_distinct": CENSUS[c]["apx_distinct"],
            "len": {"p50": stats["len_p50"], "avg": stats["len_avg"], "max": stats["len_max"]},
            "looks_like_url": (stats["url_frac"] or 0) > 0.5,
            "free_text": (stats["len_avg"] or 0) > 80,
            # Raw values: eVar/prop contents are business semantics (form steps, plan
            # codes, tool names) and are the whole point of profiling custom dims.
            # Identifier-shaped columns still fall back to shape-only.
            "top": ([] if is_sensitive(c) else
                    [{"v": str(r[c]), "len": len(str(r[c])),
                      "pct": round(100.0 * r["count"] / pop_rows, 2)} for r in top]),
            "mode": "shape_only (direct identifier)" if is_sensitive(c) else "raw",
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
# MAGIC Cardinality + top values of candidate slicing dimensions. Allowlisted dims print raw
# MAGIC (lookup IDs / country codes); pagename & URL values are query-stripped but keep the
# MAGIC full path; direct identifiers are shape-only.

# COMMAND ----------

def s9_dimensions():
    ensure_frames()
    dim_candidates = [c for c in [
        "pagename", "post_pagename", "page_url", "post_page_url", "referrer",
        "ref_domain", "ref_type", "geo_country", "geo_region", "geo_city",
        "browser", "os", "connection_type", "language", "hit_source",
        "exclude_hit", "duplicate_purchase", "new_visit", "post_page_event",
        "va_closer_id",
    ] if c in set(DF_S.columns) and c in CENSUS]

    out = []
    for c in dim_candidates:
        is_url = c in ("page_url", "post_page_url", "referrer")
        top = (DF_S.filter(nonblank(c)).groupBy(c).count()
                   .orderBy(F.desc("count")).limit(TOP_N * (3 if is_url else 1)).collect())
        pop_rows = max(SAMPLE_ROWS * CENSUS[c]["pop_pct"] / 100.0, 1)
        if is_url:
            # Full query-stripped URL. The previous domain|depth|seg1 reduction
            # collapsed every Canadian page into one bucket, which made the URL
            # dimensions useless for scope work. Query strings stay stripped —
            # they are the one part of a URL that carries session tokens.
            top_vals = [{"v": strip_query(r[c]), "pct": round(100.0 * r["count"] / pop_rows, 2)}
                        for r in top[:TOP_N]]
            mode = "raw, query-stripped"
        elif c in ("pagename", "post_pagename"):
            top_vals = [{"v": strip_query(r[c]), "pct": round(100.0 * r["count"] / pop_rows, 2)}
                        for r in top[:TOP_N]]
            mode = "raw, query-stripped"
        elif is_sensitive(c):
            # Direct/quasi identifier: cardinality and null rate are the analytical
            # facts; individual values are not. See DIRECT_IDENTIFIERS above.
            top_vals = []
            mode = "shape_only (direct identifier)"
        else:
            top_vals = [{"v": str(r[c]), "pct": round(100.0 * r["count"] / pop_rows, 2)}
                        for r in top[:TOP_N]]
            mode = "raw"
        out.append({"dim": c, "mode": mode,
                    "coverage_pct": CENSUS[c]["pop_pct"],
                    "apx_distinct": CENSUS[c]["apx_distinct"],
                    "top": top_vals})
    emit("dim_candidates", {"basis": "sample", "dims": out})

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
        "basis": "sample", "note": "shape only per ADR-0007 — no identifier values",
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
                "sensitive_shape_only": is_sensitive(col)}
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
            "scope": {"rsid": RSID_FILTER or None, "url_contains": URL_FILTER or None,
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
# MAGIC exported block and compare here to prove nothing was truncated. The sha1 is printed
# MAGIC dash-grouped in 8-hex chunks (a bare 40-hex digest would be redacted as a hex-ID by
# MAGIC the privacy scrubber) — strip the dashes before comparing.

# COMMAND ----------

def s_run_manifest():
    sections = {}
    for sid, payload in RESULTS.items():
        body = json.dumps(payload, separators=(",", ":"), default=str)
        digest = hashlib.sha1(body.encode("utf-8")).hexdigest()
        # dash-grouped: a bare 40-hex digest matches the _SCRUB_PATTERNS hex-id
        # rule and emit() would print it as <redacted:hexid>; strip '-' to compare.
        sections[sid] = {"bytes": len(body),
                         "sha1": "-".join(digest[i:i + 8] for i in range(0, 40, 8))}
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
# MAGIC `window_frame.filter.both_match` must be > 0 and `top_rsids` must list the expected
# MAGIC `manulifeglobalprod` value — if not, fix the `rsid_filter` / `url_filter` widgets.
