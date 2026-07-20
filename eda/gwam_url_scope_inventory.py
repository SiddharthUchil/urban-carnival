# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — GWAM Canada-Retirement URL Scope Inventory
# MAGIC
# MAGIC **Purpose.** Answer the question no run has answered yet: *what URLs actually exist for
# MAGIC Canada Retirement?* This notebook scans **both** report suites — `manugrs` (legacy) and
# MAGIC `manulifeglobalprod` (current) — over full history with **no URL filter applied**, and
# MAGIC lands the complete distinct-URL inventory in Delta plus a scannable rollup.
# MAGIC
# MAGIC **Why.** Every scope decision so far inherits `databricks/conf/settings.py`
# MAGIC (`SCOPE_URL_MODE = "en_only"`, `SCOPE_SUITE_MODE = "current_only"`). Three facts make that
# MAGIC worth re-deriving from evidence:
# MAGIC
# MAGIC 1. `post_page_url` — the column the shipped filter runs on — is ~37% blank on
# MAGIC    `manulifeglobalprod` and ~48% blank on `manugrs`. `page_url` is ~0% blank.
# MAGIC 2. The two suites are on **different domains**: legacy `manulifeim.com/group-retirement/ca/en`
# MAGIC    vs current `manulife.com/ca/en/personal/group-plans/group-retirement`.
# MAGIC 3. They hand off at **2026-02-01** — `manugrs` ran Jan 2024 → Jan 2026 then collapsed,
# MAGIC    `manulifeglobalprod` starts Feb 2026. A short window sees only one side of the cutover.
# MAGIC
# MAGIC **Filters are TAGGED, never applied.** Today's scope patterns become boolean columns so you
# MAGIC can measure what they capture *and what they miss*. Noise (AEM authoring hosts, `/ph/`) is
# MAGIC likewise tagged, not dropped.
# MAGIC
# MAGIC **Data visibility (ADR-0007 §5).** Hosts and paths print in full — that is the deliverable.
# MAGIC Query strings and fragments are stripped before grouping, since session tokens live there
# MAGIC and they would also shatter the grouping. ID-like path segments generalize to `{id}`, which
# MAGIC is what makes the rollup readable. No visitor, IP, cookie, or user-agent column is projected
# MAGIC at all — this notebook has no need for them.
# MAGIC
# MAGIC **How to run.** Attach a cluster, run the S0 CONFIG cell to materialize widgets, then:
# MAGIC 1. **Dry run first** — set `dry_run_month` to e.g. `2025-06` and `write_delta=false`.
# MAGIC    Confirms the whole notebook end-to-end for a fraction of the cost.
# MAGIC 2. **Full run** — clear `dry_run_month`, set `target_catalog`, `write_delta=true`, Run All.
# MAGIC
# MAGIC Each section prints a `===== BEGIN SHAREABLE: <id> =====` block. Sections are independent:
# MAGIC a failure prints `SKIPPED` and the run continues.

# COMMAND ----------

# MAGIC %md
# MAGIC ## S0 — Config, constants, helpers

# COMMAND ----------

import json
import re
import math
import datetime
import traceback

from pyspark.sql import functions as F

# ---------------------------------------------------------------- widgets ----
dbutils.widgets.text("table_fqn", "gwam_prod_catalog.inv_typed_common.adobe_hit_data", "1. Source table (catalog.schema.table)")
dbutils.widgets.text("rsids", "manugrs,manulifeglobalprod", "2. rsids to scan (comma-separated)")
dbutils.widgets.text("start_date", "2024-01-01", "3. Scan start (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "4. Scan end (YYYY-MM-DD, empty = today)")
dbutils.widgets.text("dry_run_month", "", "5. Dry run: single month YYYY-MM (empty = off)")
dbutils.widgets.text("target_catalog", "__SET_ME__", "6. Target catalog for Delta output")
dbutils.widgets.text("target_schema", "gmai_pulse_bronze", "7. Target schema for Delta output")
dbutils.widgets.text("write_delta", "true", "8. Write Delta tables (true/false)")
dbutils.widgets.text("min_hits", "1", "9. Drop inventory rows with fewer hits than this")
dbutils.widgets.text("top_n", "200", "10. Top-N cap for printed lists")
dbutils.widgets.text("prefix_depth", "5", "11. Path segments in the rollup prefix")

TABLE_FQN      = dbutils.widgets.get("table_fqn").strip()
RSIDS          = [r.strip().lower() for r in dbutils.widgets.get("rsids").split(",") if r.strip()]
START_DATE     = dbutils.widgets.get("start_date").strip()
END_DATE       = dbutils.widgets.get("end_date").strip()
DRY_RUN_MONTH  = dbutils.widgets.get("dry_run_month").strip()
TARGET_CATALOG = dbutils.widgets.get("target_catalog").strip()
TARGET_SCHEMA  = dbutils.widgets.get("target_schema").strip()
WRITE_DELTA    = dbutils.widgets.get("write_delta").strip().lower() == "true"
MIN_HITS       = int(dbutils.widgets.get("min_hits"))
TOP_N          = int(dbutils.widgets.get("top_n"))
# 5 segments is the meaningful default: the current scope root only becomes distinguishable
# at /ca/en/personal/group-plans/group-retirement, so a shallower prefix would lump all
# Canadian personal traffic into /ca/en/personal. Legacy roots are shorter and unaffected.
PREFIX_DEPTH   = max(1, int(dbutils.widgets.get("prefix_depth")))

PARTITION_COL = "process_date"
RUN_DATE = datetime.date.today().isoformat()

# A dry run overrides the window with a single month — validate before the full scan.
if DRY_RUN_MONTH:
    _y, _m = (int(x) for x in DRY_RUN_MONTH.split("-"))
    START_DATE = datetime.date(_y, _m, 1).isoformat()
    _nm = datetime.date(_y + (_m == 12), (_m % 12) + 1, 1)
    END_DATE = (_nm - datetime.timedelta(days=1)).isoformat()
elif not END_DATE:
    END_DATE = datetime.date.today().isoformat()

# Writes are skipped unless a real catalog was supplied — the notebook stays fully
# runnable read-only, so a permissions gap degrades to print-only instead of failing.
CAN_WRITE = WRITE_DELTA and TARGET_CATALOG and TARGET_CATALOG != "__SET_ME__"

INVENTORY_FQN = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.url_scope_inventory"
ROLLUP_FQN    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.url_scope_rollup"

# ------------------------------------------------------- scope constants ----
# Mirrors databricks/conf/settings.py — KEEP IN SYNC. These are TAGGED here, not applied;
# the point of this notebook is to measure what they capture and what they miss.
CUR_EN_ONLY_LIKE = "manulife.com/ca/en/personal/group-plans/group-retirement"
BROAD_LIKE       = ["/group-retirement", "/group-plans", "/regimes-collectifs"]
LEGACY_LIKE      = ["manulifeim.com/group-retirement/ca/en"]
NOISE_LIKE       = ["adobeaemcloud.com", "/ph/"]

# EDA S4c regexes. Bare `/retirement` is deliberately excluded from STRICT so the
# Philippines site (`/ph/retirement`) does not leak into the retirement population.
RET_STRICT = r"group-retirement|group-plans|regimes-collectif|retraite"
RET_BROAD  = r"retirement|retraite"

# URL candidates, in coalesce priority order. page_url leads because it is ~0% blank
# while post_page_url is 37-48% blank (doc-14 §3) — the reverse of the shipped pipeline.
URL_CANDIDATES = ("page_url", "post_page_url", "first_hit_page_url",
                  "visit_start_page_url", "site_url")

# --------------------------------------------------------------- helpers ----
# Vendored verbatim from eda/gwam_canada_retirement_eda.py (L115-190, L226) — that file is a
# Databricks notebook, not an importable module. Keep in sync so the SHAREABLE protocol,
# and especially the privacy scrubber, stay identical across the EDA notebooks.
RESULTS = {}   # section_id -> payload
SKIPPED = {}   # section_id -> reason

# Last-resort net for values that should never appear in an analytics dimension at
# all. Deliberately minimal: redacting long digit/hex runs would destroy the hit
# counts and date strings this notebook exists to report, and a 160-char cap would
# truncate the long URLs that are its entire deliverable.
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

def qcol(col_name):
    """F.col with backtick quoting — the schema has dotted column names
    (mobileappperformanceappid.*) that unquoted F.col parses as struct access."""
    return F.col("`" + col_name.replace("`", "``") + "`")

def nonblank(col_name):
    """Adobe feeds use empty strings, not NULLs."""
    c = qcol(col_name)
    return c.isNotNull() & (F.trim(c.cast("string")) != "")

def pick_col(df, *candidates):
    """First candidate column present in the schema, else None."""
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def pct(num, den):
    return round(100.0 * num / den, 4) if den else None

def coverage(rows_total, rows_emitted, hits_emitted, hits_total):
    """Truncation is always visible — never a silent top-N."""
    return {"rows_total": rows_total, "rows_emitted": rows_emitted,
            "truncated": rows_total > rows_emitted,
            "hits_pct_covered_by_emitted": pct(hits_emitted, hits_total)}

print(f"window: {START_DATE} .. {END_DATE}" + (f"  (DRY RUN {DRY_RUN_MONTH})" if DRY_RUN_MONTH else ""))
print(f"rsids : {RSIDS}")
print(f"write : {'YES -> ' + INVENTORY_FQN if CAN_WRITE else 'NO (print-only)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## S1 — Schema probe (no scan)
# MAGIC Resolves the rsid column and which URL candidates actually exist. Metadata only.

# COMMAND ----------

SRC = spark.table(TABLE_FQN)

RSID_COL = pick_col(SRC, "rsid", "report_suite", "reportsuite", "reportsuiteid", "post_rsid")
PRESENT_URL_COLS = [c for c in URL_CANDIDATES if c in set(SRC.columns)]
COAL_ORDER = [c for c in URL_CANDIDATES if c in PRESENT_URL_COLS]

if RSID_COL is None:
    raise ValueError("No report-suite column found — cannot scope by rsid.")
if not PRESENT_URL_COLS:
    raise ValueError(f"None of {URL_CANDIDATES} exist in {TABLE_FQN} — nothing to inventory.")
if PARTITION_COL not in SRC.columns:
    raise ValueError(f"Partition column {PARTITION_COL} missing — refusing an unpruned full scan.")

PART_TYPE = dict(SRC.dtypes)[PARTITION_COL]

def _part_pred():
    """Predicate honoring the real partition dtype so Delta prunes partitions.
    Same pattern as databricks/src/01_bronze_ingest.py:49-54. A string 'YYYY-MM-DD'
    partition compares correctly lexically, so one predicate covers both dtypes."""
    lo, hi = F.lit(START_DATE), F.lit(END_DATE)
    if PART_TYPE == "date":
        lo, hi = lo.cast("date"), hi.cast("date")
    return (F.col(PARTITION_COL) >= lo) & (F.col(PARTITION_COL) <= hi)

run_section("s1_schema_probe", lambda: emit("s1_schema_probe", {
    "table": TABLE_FQN,
    "rsid_col": RSID_COL,
    "partition_col": PARTITION_COL,
    "partition_dtype": PART_TYPE,
    "url_candidates_declared": list(URL_CANDIDATES),
    "url_candidates_present": PRESENT_URL_COLS,
    "url_candidates_missing": [c for c in URL_CANDIDATES if c not in PRESENT_URL_COLS],
    "coalesce_order": COAL_ORDER,
    "window": {"start": START_DATE, "end": END_DATE, "dry_run_month": DRY_RUN_MONTH or None},
}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## S2 — Suite presence probe (cheap: 2 columns)
# MAGIC Projects only `process_date` + rsid, so columnar storage never reads the wide URL strings.
# MAGIC Answers "does `manugrs` exist in this table at all?" **before** S3 spends real compute —
# MAGIC and confirms the 2026-02-01 cutover.

# COMMAND ----------

RSID_NORM = F.lower(F.trim(qcol(RSID_COL).cast("string")))

def _s2():
    probe = (SRC
             .filter(_part_pred())
             .filter(RSID_NORM.isin(RSIDS))
             .select(RSID_NORM.alias("rsid"), F.col(PARTITION_COL).alias("pd")))

    by_rsid = {r["rsid"]: r.asDict() for r in probe.groupBy("rsid").agg(
        F.count("*").alias("hits"),
        F.countDistinct("pd").alias("active_days"),
        F.min("pd").cast("string").alias("first_day"),
        F.max("pd").cast("string").alias("last_day"),
    ).collect()}

    monthly = (probe
               .groupBy("rsid", F.substring(F.col("pd").cast("string"), 1, 7).alias("month"))
               .agg(F.count("*").alias("hits"))
               .orderBy("month", "rsid")
               .collect())

    emit("s2_suite_presence", {
        "per_rsid": {r: by_rsid.get(r, {"hits": 0, "note": "ABSENT from source table in this window"})
                     for r in RSIDS},
        "monthly_hits": [{"month": r["month"], "rsid": r["rsid"], "hits": r["hits"]} for r in monthly],
        "expected_cutover": "2026-02-01 (manugrs collapses as manulifeglobalprod begins)",
    })

run_section("s2_suite_presence", _s2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S3 — Build the inventory (the one expensive scan)
# MAGIC Partition prune → rsid filter → projection prune → normalize → aggregate.
# MAGIC No URL predicate. No `collect()` of raw rows.

# COMMAND ----------

BASE = (SRC
        .filter(_part_pred())                              # 1. partition pruning FIRST
        .filter(RSID_NORM.isin(RSIDS))                     # 2. rsid only — no URL filter
        .select(RSID_NORM.alias("rsid"),                   # 3. projection pruning
                F.col(PARTITION_COL).alias("pd"),
                *[qcol(c).alias(c) for c in PRESENT_URL_COLS]))

# --- normalization -------------------------------------------------------------
# Blank-guarded coalesce: Adobe writes empty strings, not NULLs, so a plain coalesce
# would happily return "" from page_url and never reach post_page_url.
_complete = F.coalesce(*[F.when(nonblank(c), qcol(c)) for c in COAL_ORDER])

_u  = F.lower(F.trim(_complete.cast("string")))
_u  = F.regexp_replace(_u, r"^[a-z]+://", "")              # drop scheme
_u  = F.regexp_replace(_u, r"^www\.", "")                  # drop www.
_hp = F.regexp_extract(_u, r"^([^?#]*)", 1)                # drop query + fragment (PII)
_hp = F.regexp_replace(_hp, r"/+$", "")                    # drop trailing slash

_host = F.regexp_extract(_hp, r"^([^/]+)", 1)
_path = F.when(_hp.rlike(r"^[^/]+/"),
               F.concat(F.lit("/"), F.regexp_extract(_hp, r"^[^/]+/(.*)$", 1))
               ).otherwise(F.lit("/"))

# Rows where every URL candidate was blank get their own bucket rather than being dropped,
# so the blank share stays visible in the output.
_is_blank = _hp.isNull() | (_hp == "")
HOST = F.when(_is_blank, F.lit("<blank>")).otherwise(_host)
PATH = F.when(_is_blank, F.lit("<blank>")).otherwise(_path)

# ID generalization: collapses /member/12345 into /member/{id}. Both a privacy control
# and what makes the rollup readable — otherwise leaf IDs explode into singleton rows.
_pg = PATH
_pg = F.regexp_replace(_pg, r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?=/|$)", "/{id}")
_pg = F.regexp_replace(_pg, r"/[0-9a-f]{16,}(?=/|$)", "/{id}")
_pg = F.regexp_replace(_pg, r"/\d{4,}(?=/|$)", "/{id}")
PATH_GEN = _pg

def _seg(n):
    """n-th path segment of the generalized path ('' when absent)."""
    return F.regexp_extract(PATH_GEN, r"^" + ("/[^/]*" * (n - 1)) + r"/([^/]*)", 1)

_segs = [_seg(n) for n in range(1, PREFIX_DEPTH + 1)]
PATH_PREFIX = F.concat(
    F.lit("/"), _segs[0],
    *[F.when(s != "", F.concat(F.lit("/"), s)).otherwise(F.lit("")) for s in _segs[1:]],
)

# --- tags (measured, never applied as filters) ---------------------------------
def _any_contains(col, needles):
    cond = F.lit(False)
    for n in needles:
        cond = cond | col.contains(n)
    return cond

TAGS = {
    "lang": (F.when(PATH_GEN.rlike(r"(^|/)fr(/|$)") | PATH_GEN.rlike(r"regimes-collectif|retraite|particuliers|conseillers"), "fr")
              .when(PATH_GEN.rlike(r"(^|/)en(/|$)") | PATH_GEN.rlike(r"group-retirement|group-plans"), "en")
              .otherwise("unknown")),
    "matches_current_en_only": _hp.contains(CUR_EN_ONLY_LIKE),
    "matches_broad":           _any_contains(_hp, BROAD_LIKE),
    "matches_legacy":          _any_contains(_hp, LEGACY_LIKE),
    "is_noise":                _any_contains(_hp, NOISE_LIKE),
    "ret_strict":              _hp.rlike(RET_STRICT),
    "ret_broad":               _hp.rlike(RET_BROAD),
    "env": (F.when(HOST.contains("adobeaemcloud.com"), "aem")
             .when(HOST.rlike(r"preview|stage|staging|author|uat|dev\."), "nonprod")
             .otherwise("prod")),
}

GRAIN = ["rsid", "host", "path", "path_prefix"] + list(TAGS.keys())

NORM = BASE.select(
    F.col("rsid"), F.col("pd"),
    HOST.alias("host"), PATH.alias("path"), PATH_PREFIX.alias("path_prefix"),
    *[expr.alias(name) for name, expr in TAGS.items()],
    # Per-source-column population, carried through the same scan so blank rates
    # come free instead of costing a second pass over the source table.
    *[F.when(nonblank(c), 1).otherwise(0).alias(f"src_{c}_nb") for c in PRESENT_URL_COLS],
)

INV = (NORM.groupBy(*GRAIN)
       .agg(F.count("*").alias("hits"),
            F.countDistinct("pd").alias("active_days"),
            F.min("pd").cast("string").alias("first_seen"),
            F.max("pd").cast("string").alias("last_seen"),
            *[F.sum(f"src_{c}_nb").alias(f"src_{c}_nb") for c in PRESENT_URL_COLS])
       .filter(F.col("hits") >= F.lit(MIN_HITS))
       .withColumn("run_date", F.lit(RUN_DATE)))

# The rollup is derived from INV (already small), so caching INV once turns the
# rollup, the summary, and both writes into a single pass over the source table.
INV = INV.persist()
INV_ROWS = INV.count()
print(f"inventory rows: {INV_ROWS:,}  (min_hits={MIN_HITS})")

ROLLUP = (INV.groupBy("rsid", "host", "path_prefix", "lang", "env",
                      "matches_current_en_only", "matches_broad", "matches_legacy",
                      "is_noise", "ret_strict", "ret_broad")
          .agg(F.sum("hits").alias("hits"),
               F.countDistinct("path").alias("distinct_paths"),
               F.min("first_seen").alias("first_seen"),
               F.max("last_seen").alias("last_seen"))
          .withColumn("run_date", F.lit(RUN_DATE)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## S4 — Write Delta tables
# MAGIC Partitioned by `run_date` with `replaceWhere`, so re-running the same day is idempotent
# MAGIC while earlier runs are retained. Skipped entirely when `target_catalog` is `__SET_ME__`.
# MAGIC A schema change needs the table dropped first (`replaceWhere` and `overwriteSchema`
# MAGIC cannot be combined).

# COMMAND ----------

def _write(df, fqn):
    (df.write.format("delta")
       .mode("overwrite")
       .option("replaceWhere", f"run_date = '{RUN_DATE}'")
       .partitionBy("run_date")
       .saveAsTable(fqn))
    return spark.table(fqn).filter(F.col("run_date") == RUN_DATE).count()

def _s4():
    if not CAN_WRITE:
        emit("s4_delta_write", {"written": False,
                                "reason": "target_catalog is __SET_ME__ or write_delta=false",
                                "note": "notebook ran print-only; all figures below are still valid"})
        return
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
    n_inv = _write(INV, INVENTORY_FQN)
    n_rol = _write(ROLLUP, ROLLUP_FQN)
    emit("s4_delta_write", {"written": True, "run_date": RUN_DATE,
                            "inventory": {"table": INVENTORY_FQN, "rows": n_inv},
                            "rollup": {"table": ROLLUP_FQN, "rows": n_rol}})

run_section("s4_delta_write", _s4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S5 — Inventory summary + URL column availability

# COMMAND ----------

def _s5():
    per_rsid = INV.groupBy("rsid").agg(
        F.sum("hits").alias("hits"),
        F.count("*").alias("distinct_host_path"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.min("first_seen").alias("first_seen"),
        F.max("last_seen").alias("last_seen"),
        F.sum(F.when(F.col("host") == "<blank>", F.col("hits")).otherwise(0)).alias("blank_url_hits"),
        *[F.sum(f"src_{c}_nb").alias(f"src_{c}_nb") for c in PRESENT_URL_COLS],
    ).collect()

    out = {}
    for r in per_rsid:
        d = r.asDict()
        hits = d["hits"]
        out[d["rsid"]] = {
            "hits": hits,
            "distinct_host_path": d["distinct_host_path"],
            "distinct_hosts": d["distinct_hosts"],
            "date_range": [d["first_seen"], d["last_seen"]],
            "blank_url_hits": d["blank_url_hits"],
            "blank_url_pct": pct(d["blank_url_hits"], hits),
            # The headline check: post_page_url should read ~37% blank on manulifeglobalprod
            # and ~48% on manugrs, while page_url reads ~0%.
            "blank_pct_by_source_col": {c: pct(hits - d[f"src_{c}_nb"], hits) for c in PRESENT_URL_COLS},
        }

    hosts = (INV.groupBy("rsid", "host").agg(F.sum("hits").alias("hits"))
             .orderBy(F.col("hits").desc()).limit(TOP_N).collect())
    total_hits = sum(v["hits"] for v in out.values())

    emit("s5_inventory_summary", {
        "per_rsid": out,
        "top_hosts": [{"rsid": r["rsid"], "host": r["host"], "hits": r["hits"]} for r in hosts],
        "top_hosts_coverage": coverage(INV.select("rsid", "host").distinct().count(),
                                       len(hosts), sum(r["hits"] for r in hosts), total_hits),
    })

run_section("s5_inventory_summary", _s5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S6 — Path-prefix rollup
# MAGIC The scannable view: host + first `prefix_depth` path segments, IDs generalized.

# COMMAND ----------

def _s6():
    total_hits = ROLLUP.agg(F.sum("hits")).collect()[0][0] or 0
    rows_total = ROLLUP.count()
    top = ROLLUP.orderBy(F.col("hits").desc()).limit(TOP_N).collect()
    emit("s6_url_scope_rollup", {
        "rows": [{"rsid": r["rsid"], "host": r["host"], "path_prefix": r["path_prefix"],
                  "hits": r["hits"], "distinct_paths": r["distinct_paths"],
                  "lang": r["lang"], "env": r["env"],
                  "first_seen": r["first_seen"], "last_seen": r["last_seen"],
                  "cur": r["matches_current_en_only"], "broad": r["matches_broad"],
                  "legacy": r["matches_legacy"], "noise": r["is_noise"]}
                 for r in top],
        "coverage": coverage(rows_total, len(top), sum(r["hits"] for r in top), total_hits),
    })

run_section("s6_url_scope_rollup", _s6)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S7 — Scope filter comparison
# MAGIC **The decision section.** For each shipped filter × rsid: what it captures, and — the number
# MAGIC that matters — what falls outside *every* filter but still looks like retirement traffic.

# COMMAND ----------

def _s7():
    flags = ["matches_current_en_only", "matches_broad", "matches_legacy",
             "is_noise", "ret_strict", "ret_broad"]
    uncovered_cond = (F.col("ret_strict") & ~F.col("is_noise")
                      & ~F.col("matches_current_en_only") & ~F.col("matches_legacy"))

    agg = INV.groupBy("rsid").agg(
        F.sum("hits").alias("hits"),
        F.count("*").alias("urls"),
        *[F.sum(F.when(F.col(f), F.col("hits")).otherwise(0)).alias(f"{f}_hits") for f in flags],
        *[F.sum(F.when(F.col(f), 1).otherwise(0)).alias(f"{f}_urls") for f in flags],
        # Retirement-looking traffic that no shipped filter would ingest today.
        F.sum(F.when(uncovered_cond, F.col("hits")).otherwise(0)).alias("uncovered_ret_hits"),
        F.sum(F.when(uncovered_cond, 1).otherwise(0)).alias("uncovered_ret_urls"),
    ).collect()

    per_rsid = {}
    for r in agg:
        d = r.asDict()
        per_rsid[d["rsid"]] = {
            "hits": d["hits"], "urls": d["urls"],
            "by_filter": {f: {"hits": d[f"{f}_hits"], "urls": d[f"{f}_urls"],
                              "hits_pct": pct(d[f"{f}_hits"], d["hits"])} for f in flags},
            "uncovered_retirement_hits": d["uncovered_ret_hits"],
            "uncovered_retirement_urls": d["uncovered_ret_urls"],
            "uncovered_retirement_pct": pct(d["uncovered_ret_hits"], d["hits"]),
        }

    uncovered = (ROLLUP.filter(uncovered_cond)
                 .orderBy(F.col("hits").desc()).limit(TOP_N).collect())

    emit("s7_scope_filter_comparison", {
        "filters_evaluated": {
            "current_en_only": CUR_EN_ONLY_LIKE,
            "broad": BROAD_LIKE,
            "legacy": LEGACY_LIKE,
            "noise_excluded_by_pipeline": NOISE_LIKE,
        },
        "per_rsid": per_rsid,
        "top_uncovered_retirement_prefixes": [
            {"rsid": r["rsid"], "host": r["host"], "path_prefix": r["path_prefix"],
             "hits": r["hits"], "lang": r["lang"], "first_seen": r["first_seen"],
             "last_seen": r["last_seen"]} for r in uncovered],
        "reading_note": "uncovered = matches the retirement regex, is not noise, and is captured by "
                        "NEITHER the current en_only filter NOR the legacy filter. These are the "
                        "candidate additions to scope.",
    })

run_section("s7_scope_filter_comparison", _s7)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S8 — Run manifest

# COMMAND ----------

run_section("s8_run_manifest", lambda: emit("s8_run_manifest", {
    "run_date": RUN_DATE,
    "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
    "source_table": TABLE_FQN,
    "window": {"start": START_DATE, "end": END_DATE, "dry_run_month": DRY_RUN_MONTH or None},
    "rsids": RSIDS,
    "resolved": {"rsid_col": RSID_COL, "url_cols_present": PRESENT_URL_COLS,
                 "coalesce_order": COAL_ORDER, "partition_col": PARTITION_COL},
    "min_hits": MIN_HITS, "top_n": TOP_N, "prefix_depth": PREFIX_DEPTH,
    "inventory_rows": INV_ROWS,
    "delta_written": CAN_WRITE,
    "tables": {"inventory": INVENTORY_FQN if CAN_WRITE else None,
               "rollup": ROLLUP_FQN if CAN_WRITE else None},
    "sections_emitted": sorted(RESULTS.keys()),
    "sections_skipped": SKIPPED,
}))

INV.unpersist()
