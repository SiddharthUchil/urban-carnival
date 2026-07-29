# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — GWAM Canada Retirement: multi-channel discovery probe
# MAGIC
# MAGIC **Purpose.** The business SME sent a four-channel alerting scope for Canada Retirement
# MAGIC (Public Website / Web Member / Mobile / ManulifeID) defined by **report suite + segment
# MAGIC field**, not by URL. Three of the four report suites appear nowhere in this repo, and the
# MAGIC segment fields (`evar105`, "v185", a pagename group) have never been profiled. This probe
# MAGIC answers everything about that table that can be answered **from data**, so the questions we
# MAGIC put to the SME are only the ones that genuinely need a business ruling.
# MAGIC
# MAGIC Companion to [19-gwam-channel-readiness.md](../research/claude/19-gwam-channel-readiness.md)
# MAGIC and [20-gwam-sme-questions.md](../research/claude/20-gwam-sme-questions.md). Precedent:
# MAGIC CoverMe ran a discovery probe before its main EDA.
# MAGIC
# MAGIC **This is NOT the GWAM EDA.** `gwam_canada_retirement_eda.py` filters to two rsids and a URL
# MAGIC include-list; that filter would drop mobile-app hits entirely (app hits carry no page URL)
# MAGIC and it has no eVar105/eVar185 census. This probe is deliberately **unscoped**: it reads the
# MAGIC whole table and reports per-rsid, so a suite we do not yet know about still shows up.
# MAGIC
# MAGIC **Sections.**
# MAGIC
# MAGIC | id | Question it settles |
# MAGIC |---|---|
# MAGIC | `C1 rsid_census` | Do `manucustomer.prod`, a GRS+ suite, and a GBRS-mobile suite exist here? Enough history each? |
# MAGIC | `C2 web_vs_app` | Which suites are app (no URL) vs web — i.e. can a URL-based scope express them at all? |
# MAGIC | `C3 evar105_census` | Is `ca-retirement :  : GWAM` a literal value? What delimiter? How big is segment-scope vs today's URL scope? |
# MAGIC | `C4 platform_census` | Is `MPS Member` a value of eVar185 or eVar110? (resolves the long-open Platform conflict) |
# MAGIC | `C5 error_fields` | Is an Errors metric implementable, and from which field? |
# MAGIC | `C6 signin_fields` | Can a sign-in completion ratio be built, and from what? |
# MAGIC | `C7 event_census` | The `post_event_list` id space of each suite. |
# MAGIC | `C8 pagename_census` | Is "Canada Retirement App Pages v2" translatable into a pagename predicate? |
# MAGIC | `C9 manulifeid_split` | Does ANY field separate retirement sign-ins from other ManulifeID sign-ins? (the SME's own open item) |
# MAGIC | `C10 marketing_fields` | What could "ideally non-marketing" mean operationally? |
# MAGIC
# MAGIC **What this probe canNOT settle** — deliberately out of reach of any query, and therefore
# MAGIC still SME questions: whether 1/0 in the table means in/out of scope; whether the 2026-07-20
# MAGIC login-exclusion rule (D8) is superseded; the business definition of "non-marketing"; the
# MAGIC numerator/denominator of "Sign in % rate completion"; the friendly-name → rsid mapping for
# MAGIC "GRS+" if no candidate in C1 is recognisable; and whether the "Manulife Financial" Adobe
# MAGIC instance is the same feed (an Adobe-admin question).
# MAGIC
# MAGIC **How to run.** Databricks → Workspace → Import → File → select this `.py` (it imports as a
# MAGIC notebook — the file is in Databricks "source" format). Attach to any cluster with Unity
# MAGIC Catalog access (DBR 13+). Run the **C0 config cell** once so the widgets appear, then
# MAGIC **Run All**. C1 is the one full-history scan but touches only `rsid` + the partition column;
# MAGIC everything else runs inside `window_days`.
# MAGIC
# MAGIC **What to paste back.** Every section prints a `===== BEGIN SHAREABLE: <id> =====` block —
# MAGIC copy those verbatim, including any `----- part i of N -----` continuations.
# MAGIC
# MAGIC ⚠ **Check the manifest.** The final `run_manifest` block carries a `skipped` map. A section
# MAGIC that throws prints `===== SKIPPED: <id> | <reason> =====` and the run *continues* — that is
# MAGIC how a missing section went unnoticed for days on CoverMe (doc-17 E1). `skipped` must be `{}`;
# MAGIC if it isn't, paste the SKIPPED lines back too. Top-N caps are kept deliberately small because
# MAGIC Databricks silently truncates large stdout payloads mid-JSON (doc-16 §0.5).

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0 — Config, constants, helpers

# COMMAND ----------

import json
import math
import datetime
import traceback

from pyspark.sql import functions as F

# ---------------------------------------------------------------- widgets ----
dbutils.widgets.text("table_fqn", "gwam_prod_catalog.inv_typed_common.adobe_hit_data", "1. Table (catalog.schema.table)")
dbutils.widgets.text("window_days", "90", "2. Profiling window (days back from latest data)")
dbutils.widgets.text("top_n", "25", "3. Top-N cap for value lists")
dbutils.widgets.text("rsid_focus",
                     "manulifeglobalprod,manufingbrsmobileapp.prod,manucustomer.prod,manugrs",
                     "4. rsids for per-suite deep dives (comma-sep; unknown names are reported, not fatal)")
dbutils.widgets.text("sme_suite_hints", "grs,gbrs,mobileapp,manucustomer,mps,globalprod",
                     "5. Substrings to flag as candidate SME suites in C1")

TABLE_FQN   = dbutils.widgets.get("table_fqn").strip()
WINDOW_DAYS = int(dbutils.widgets.get("window_days"))
TOP_N       = int(dbutils.widgets.get("top_n"))


def _csv(widget):
    return [p.strip() for p in dbutils.widgets.get(widget).split(",") if p.strip()]


RSID_FOCUS = [r.lower() for r in _csv("rsid_focus")]
SUITE_HINTS = [h.lower() for h in _csv("sme_suite_hints")]

# The SME's scope table, encoded so the emitted payloads can be read against it directly.
# Values are the SME's own labels, verbatim -- resolving them to real rsids is C1's job.
SME_CHANNELS = {
    "public_website": {"instance": "Manulife", "suite_label": "Manulife Global Prod",
                       "segment": "Brand (evar105) = 'ca-retirement :  : GWAM'"},
    "web_member":     {"instance": "Manulife", "suite_label": "GRS+",
                       "segment": "Platform - v185 = 'MPS Member'"},
    "mobile":         {"instance": "Manulife Financial", "suite_label": "GBRS Mobile App - Production",
                       "segment": "Canada Retirement App Pages v2"},
    "manulifeid":     {"instance": "Manulife", "suite_label": "manucustomer.prod",
                       "segment": None},  # SME: "Not sure how to seperate Retirement from other ManulifeID signins"
}

# Today's shipped pipeline scope, for the C3 segment-vs-URL comparison (conf/settings.py).
PIPELINE_RSID = "manulifeglobalprod"
URL_SCOPE_BROAD = ["%/group-retirement%", "%/group-plans%", "%/regimes-collectifs%"]
URL_SCOPE_EXCLUDE = ["%adobeaemcloud.com%", "%/ph/%"]

# Candidate delimiters for the eVar105 "Brand | Line of Business | Segment" triple. The docs
# describe the SHAPE, not the separator; the SME wrote "ca-retirement :  : GWAM". C3 measures
# which of these actually splits the values, rather than assuming one.
DELIM_CANDIDATES = [" : ", ":", " | ", "|", " - ", ","]

RESULTS = {}
SKIPPED = {}
MAX_EMIT_STR = 2000


def _scrub_str(s):
    if len(s) > MAX_EMIT_STR:
        s = s[:MAX_EMIT_STR] + "...<trunc>"
    return s


def _scrub(obj):
    """Display hygiene only -- truncate over-long strings, round floats. No redaction
    (ADR-0007 §5 full-raw revision 2026-07-23)."""
    if isinstance(obj, dict):
        return {(_scrub_str(k) if isinstance(k, str) else k): _scrub(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub(v) for v in obj]
    if isinstance(obj, str):
        return _scrub_str(obj)
    if isinstance(obj, float):
        return round(obj, 6) if math.isfinite(obj) else None
    return obj


def emit(section_id, payload):
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
def qcol(name):
    """F.col with backtick quoting -- the schema has dotted column names
    (mobileappperformanceappid.*) that unquoted F.col parses as struct access."""
    return F.col("`" + name.replace("`", "``") + "`")


def nonblank(name):
    """Adobe feeds write empty strings, not NULLs."""
    c = qcol(name)
    return c.isNotNull() & (F.trim(c.cast("string")) != "")


def sql_col(name):
    return "`" + name.replace("`", "``") + "`"


def sql_regex_literal(delim):
    """A literal delimiter, as a SQL string holding a regex.

    Two escapes, both required. Spark's split() takes a REGEX, not a literal, so an
    unescaped '|' would mean alternation and split on nothing; then the SQL string literal
    itself processes backslashes, so each one has to survive as a pair."""
    escaped = "".join("\\" + ch if ch in r".^$*+?()[]{}|\\" else ch for ch in delim)
    return escaped.replace("\\", "\\\\").replace("'", "\\'")


def split_part(sql_expr, delim, idx):
    """try_element_at(split(expr, delim), idx), built as SQL.

    Two deliberate choices. (1) try_element_at, never element_at: Databricks runs ANSI mode
    and a short split throws ArrayIndexOutOfBounds mid-section -- the CoverMe E1 defect that
    silently killed a whole section. (2) via F.expr rather than F.try_element_at, because the
    Python binding only exists in PySpark 3.5+ and these notebooks target DBR 13+; this is the
    form the other EDA scripts already use."""
    return F.expr(f"try_element_at(split({sql_expr}, '{sql_regex_literal(delim)}'), {idx})")


base = spark.table(TABLE_FQN)
AVAILABLE = {c.lower() for c in base.columns}


def have(name):
    return name.lower() in AVAILABLE


def pick(*names):
    """First column that actually exists. Adobe feeds carry both `evarN` and `post_evarN`
    and which one is populated varies by suite -- never assume."""
    for n in names:
        if have(n):
            return n
    return None


def evar(n):
    """Prefer post_ (the processed value Workspace reports on), fall back to raw."""
    return pick(f"post_evar{n}", f"evar{n}")


def nonblank_rate(name):
    return F.avg(F.when(nonblank(name), F.lit(1.0)).otherwise(F.lit(0.0)))


def top_values(df, name, n=None, extra_filter=None):
    """Top-N non-blank values of one column with counts. Returns [] if the column is absent."""
    if not name or not have(name):
        return []
    n = n or TOP_N
    d = df.filter(nonblank(name))
    if extra_filter is not None:
        d = d.filter(extra_filter)
    rows = (d.groupBy(qcol(name).cast("string").alias("value"))
             .count().orderBy(F.desc("count")).limit(n).collect())
    return [{"value": r["value"], "count": int(r["count"])} for r in rows]


def per_rsid_rates(df, cols):
    """Per-rsid non-blank rate + row count for a set of columns. Absent columns are
    reported as null rather than dropped, so the payload documents what the feed lacks."""
    present = [(label, name) for label, name in cols if name and have(name)]
    aggs = [F.count(F.lit(1)).alias("rows")]
    aggs += [nonblank_rate(name).alias(label) for label, name in present]
    rows = df.groupBy(F.col("rsid").alias("rsid")).agg(*aggs).orderBy(F.desc("rows")).collect()
    present_labels = {label for label, _ in present}
    out = []
    for r in rows:
        rec = {"rsid": r["rsid"], "rows": int(r["rows"])}
        for label, _ in cols:
            rec[label] = float(r[label]) if label in present_labels and r[label] is not None else None
        out.append(rec)
    return out


def like_any(col, patterns):
    expr = F.lit(False)
    for p in patterns:
        expr = expr | col.like(p.lower())
    return expr


# ---- window frame -------------------------------------------------------------
# Derived from the data's own latest partition, so the probe needs no hardcoded dates.
_max_pd = base.agg(F.max("process_date").alias("m")).collect()[0]["m"]
MAX_DATE = str(_max_pd)[:10]
START_DATE = (datetime.date.fromisoformat(MAX_DATE) - datetime.timedelta(days=WINDOW_DAYS)).isoformat()
WIN = base.filter((F.col("process_date") >= F.lit(START_DATE)) & (F.col("process_date") <= F.lit(MAX_DATE)))

EVAR105, EVAR185, EVAR110 = evar(105), evar(185), evar(110)
EVAR103 = evar(103)
ERROR_EVARS = {f"evar{n}": evar(n) for n in (181, 182, 183, 184)}
SIGNIN_EVARS = {f"evar{n}": evar(n) for n in (122, 135)}

print(json.dumps({
    "table": TABLE_FQN, "n_columns": len(base.columns),
    "max_process_date": MAX_DATE, "window": [START_DATE, MAX_DATE], "window_days": WINDOW_DAYS,
    "resolved_columns": {
        "evar105": EVAR105, "evar185": EVAR185, "evar110": EVAR110, "evar103": EVAR103,
        "error_evars": ERROR_EVARS, "signin_evars": SIGNIN_EVARS,
        "page_url": have("page_url"), "post_page_url": have("post_page_url"),
        "mobileappid": have("mobileappid"), "post_event_list": have("post_event_list"),
    },
}, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C1 — rsid census (full history, unfiltered)
# MAGIC
# MAGIC Settles: do the SME's three unknown report suites exist in this table, and does each carry
# MAGIC enough history to fit a baseline (the standing ≥30-day / ideally ≥90-day gate)? Full-history
# MAGIC scan, but it touches only `rsid` and the partition column.

# COMMAND ----------

def c1_rsid_census():
    rows = (base.groupBy(F.col("rsid").alias("rsid"))
                .agg(F.count(F.lit(1)).alias("rows"),
                     F.min("process_date").alias("first_day"),
                     F.max("process_date").alias("last_day"),
                     F.countDistinct("process_date").alias("n_days"))
                .orderBy(F.desc("rows")).collect())
    total = sum(int(r["rows"]) for r in rows) or 1
    census = []
    for r in rows:
        rsid = (r["rsid"] or "")
        hints = [h for h in SUITE_HINTS if h in rsid.lower()]
        census.append({
            "rsid": rsid, "rows": int(r["rows"]),
            "share_pct": round(100.0 * int(r["rows"]) / total, 4),
            "first_day": str(r["first_day"])[:10], "last_day": str(r["last_day"])[:10],
            "n_days": int(r["n_days"]),
            "clears_30d_gate": int(r["n_days"]) >= 30,
            "clears_90d_gate": int(r["n_days"]) >= 90,
            "sme_name_hints": hints,
        })
    found = {r["rsid"] for r in census}
    emit("rsid_census", {
        "n_rsids": len(census), "total_rows": total, "census": census,
        "sme_channel_lookup": {
            k: {"suite_label": v["suite_label"],
                "exact_rsid_present": v["suite_label"] in found,
                "hint_matches": [c["rsid"] for c in census
                                 if any(h in c["rsid"].lower()
                                        for h in SUITE_HINTS if h in v["suite_label"].lower().replace(" ", ""))][:10]}
            for k, v in SME_CHANNELS.items()},
        "focus_rsids_present": {r: (r in {f.lower() for f in found}) for r in RSID_FOCUS},
        "note": ("An rsid absent here is NOT proof the suite does not exist -- it may sit in a "
                 "different Adobe instance/feed. That distinction is an Adobe-admin question, "
                 "not a data one."),
    })


run_section("rsid_census", c1_rsid_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C2 — web vs app discriminator, per rsid
# MAGIC
# MAGIC Settles: which suites carry page URLs at all. Every scope predicate in the pipeline is a SQL
# MAGIC `LIKE` on a URL column, so a suite with no URL **cannot be scoped the way we scope today** —
# MAGIC this is the query that proves the Mobile channel needs a different mechanism.

# COMMAND ----------

def c2_web_vs_app():
    url_cols = [("page_url", pick("page_url")), ("post_page_url", pick("post_page_url")),
                ("post_pagename", pick("post_pagename")), ("pagename", pick("pagename"))]
    app_cols = [("mobileappid", pick("mobileappid")),
                ("mobiledayssincelastuse", pick("mobiledayssincelastuse")),
                ("mobileinstalldate", pick("mobileinstalldate")),
                ("mobileosversion", pick("mobileosversion"))]
    rates = per_rsid_rates(WIN, url_cols + app_cols)

    # The single discriminator that matters: does a usable URL exist after the D4 coalesce?
    have_url = [n for _, n in url_cols[:2] if n]
    if have_url:
        coalesced = F.coalesce(*[F.when(nonblank(n), qcol(n)) for n in have_url])
        url_rate = (WIN.groupBy(F.col("rsid").alias("rsid"))
                       .agg(F.avg(F.when(coalesced.isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0)))
                             .alias("coalesced_url_rate")).collect())
        url_map = {r["rsid"]: float(r["coalesced_url_rate"]) for r in url_rate}
    else:
        url_map = {}

    for rec in rates:
        rec["coalesced_url_rate"] = url_map.get(rec["rsid"])
        r = rec["coalesced_url_rate"]
        rec["verdict"] = None if r is None else ("web" if r >= 0.90 else "app_or_mixed" if r <= 0.10 else "mixed")

    emit("web_vs_app", {
        "window": [START_DATE, MAX_DATE], "per_rsid": rates,
        "note": ("verdict=app_or_mixed means a URL LIKE filter cannot express that channel's scope. "
                 "Null rate = column absent from the feed, not zero population."),
    })


run_section("web_vs_app", c2_web_vs_app)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C3 — eVar105 census, delimiter, and segment-scope vs URL-scope sizing
# MAGIC
# MAGIC The highest-value section. Settles: (a) is `ca-retirement :  : GWAM` a literal value; (b) what
# MAGIC the real delimiter is, measured rather than assumed; (c) **how the segment-scoped population
# MAGIC compares to the population the pipeline ingests today** — i.e. the size of the re-baseline
# MAGIC that switching scope models would cause.

# COMMAND ----------

def c3_evar105_census():
    if not EVAR105:
        raise ValueError("no evar105/post_evar105 column in this feed")

    col = qcol(EVAR105).cast("string")
    scoped = WIN.filter(nonblank(EVAR105))

    # (b) measure the delimiter instead of trusting the doc's "Brand | LoB | Segment" shorthand.
    src = sql_col(EVAR105)
    delim_probe = {}
    for d in DELIM_CANDIDATES:
        n_parts = F.expr(f"size(split({src}, '{sql_regex_literal(d)}'))")
        row = scoped.agg(
            F.avg(n_parts.cast("double")).alias("avg_parts"),
            F.avg(F.when(n_parts == 3, F.lit(1.0)).otherwise(F.lit(0.0))).alias("pct_exactly_3"),
        ).collect()[0]
        delim_probe[d] = {"avg_parts": float(row["avg_parts"] or 0.0),
                          "pct_exactly_3": float(row["pct_exactly_3"] or 0.0)}
    best = max(delim_probe, key=lambda d: (delim_probe[d]["pct_exactly_3"], delim_probe[d]["avg_parts"]))

    brand = F.trim(split_part(src, best, 1))
    lob = F.trim(split_part(src, best, 2))
    segment = F.trim(split_part(src, best, 3))

    triples = (scoped.groupBy(F.col("rsid").alias("rsid"), brand.alias("brand"),
                              lob.alias("lob"), segment.alias("segment"))
                     .count().orderBy(F.desc("count")).limit(TOP_N * 4).collect())

    # (c) the re-baseline sizing: segment-scope vs today's shipped URL-scope, same window.
    is_ca_ret = F.lower(col).contains("ca-retirement")
    is_gwam = F.lower(col).contains("gwam")
    url_cols = [n for n in (pick("page_url"), pick("post_page_url")) if n]
    if url_cols:
        urlc = F.lower(F.coalesce(*[F.when(nonblank(n), qcol(n)) for n in url_cols]))
        in_url_scope = like_any(urlc, URL_SCOPE_BROAD) & ~like_any(urlc, URL_SCOPE_EXCLUDE)
    else:
        in_url_scope = F.lit(False)

    sizing = (WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
                 .agg(F.count(F.lit(1)).alias("rows"),
                      F.sum(F.when(is_ca_ret & is_gwam, 1).otherwise(0)).alias("segment_scope"),
                      F.sum(F.when(in_url_scope, 1).otherwise(0)).alias("url_scope_broad"),
                      F.sum(F.when(is_ca_ret & is_gwam & in_url_scope, 1).otherwise(0)).alias("both"),
                      F.sum(F.when(is_ca_ret & is_gwam & ~in_url_scope, 1).otherwise(0)).alias("segment_only"),
                      F.sum(F.when(~(is_ca_ret & is_gwam) & in_url_scope, 1).otherwise(0)).alias("url_only"),
                      )).collect()[0]

    per_rsid_match = (WIN.groupBy(F.col("rsid").alias("rsid"))
                         .agg(F.count(F.lit(1)).alias("rows"),
                              nonblank_rate(EVAR105).alias("evar105_populated"),
                              F.avg(F.when(is_ca_ret & is_gwam, 1.0).otherwise(0.0)).alias("ca_ret_gwam_rate"))
                         .orderBy(F.desc("rows")).limit(TOP_N).collect())

    emit("evar105_census", {
        "column": EVAR105, "window": [START_DATE, MAX_DATE],
        "sme_claimed_value": "ca-retirement :  : GWAM",
        "delimiter_probe": delim_probe, "chosen_delimiter": best,
        "top_values": top_values(WIN, EVAR105),
        "top_triples": [{"rsid": r["rsid"], "brand": r["brand"], "lob": r["lob"],
                         "segment": r["segment"], "count": int(r["count"])} for r in triples],
        "per_rsid": [{"rsid": r["rsid"], "rows": int(r["rows"]),
                      "evar105_populated": float(r["evar105_populated"] or 0.0),
                      "ca_ret_gwam_rate": float(r["ca_ret_gwam_rate"] or 0.0)} for r in per_rsid_match],
        "scope_sizing_on_pipeline_rsid": {
            "rsid": PIPELINE_RSID, "window_rows": int(sizing["rows"]),
            "segment_scope_rows": int(sizing["segment_scope"]),
            "url_scope_broad_rows": int(sizing["url_scope_broad"]),
            "in_both": int(sizing["both"]),
            "segment_only": int(sizing["segment_only"]),
            "url_only": int(sizing["url_only"]),
            "note": ("segment_only = traffic a segment-scoped pipeline would GAIN; url_only = traffic "
                     "it would LOSE. Both are re-baseline magnitude: conf/settings.py:25-31 requires a "
                     "full mode=backfill with gold truncated for any scope change."),
        },
    })


run_section("evar105_census", c3_evar105_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C4 — Platform census: eVar185 vs eVar110
# MAGIC
# MAGIC Settles the Web Member predicate AND a long-open spec conflict: doc-16 §3 lists **both**
# MAGIC eVar110 and eVar185 as "Platform" and flags the collision as unresolved (doc-15 §8 Q14). The
# MAGIC SME's "v185 = MPS Member" is Adobe shorthand for eVar185. Whichever column actually carries
# MAGIC `MPS Member` is the field of record.

# COMMAND ----------

def c4_platform_census():
    out = {"window": [START_DATE, MAX_DATE], "columns": {"evar185": EVAR185, "evar110": EVAR110},
           "sme_claimed_value": "MPS Member"}
    for label, name in (("evar185", EVAR185), ("evar110", EVAR110)):
        if not name:
            out[label] = {"present": False}
            continue
        mps = F.lower(qcol(name).cast("string")).contains("mps")
        rows = (WIN.groupBy(F.col("rsid").alias("rsid"))
                   .agg(F.count(F.lit(1)).alias("rows"),
                        nonblank_rate(name).alias("populated"),
                        F.avg(F.when(mps, 1.0).otherwise(0.0)).alias("mps_rate"))
                   .orderBy(F.desc("rows")).limit(TOP_N).collect())
        out[label] = {
            "present": True, "column": name,
            "top_values": top_values(WIN, name),
            "per_rsid": [{"rsid": r["rsid"], "rows": int(r["rows"]),
                          "populated": float(r["populated"] or 0.0),
                          "mps_rate": float(r["mps_rate"] or 0.0)} for r in rows],
        }
    emit("platform_census", out)


run_section("platform_census", c4_platform_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C5 — Error fields
# MAGIC
# MAGIC Settles whether an Errors metric is implementable at all. The repo has NO error metric; the
# MAGIC EDDL dictionary labels eVar181-184 as Error Code / Description / Type / Category and lists
# MAGIC `event173`, but nothing has ever been profiled or built.

# COMMAND ----------

def c5_error_fields():
    cols = [(k, v) for k, v in ERROR_EVARS.items()]
    out = {"window": [START_DATE, MAX_DATE], "columns": ERROR_EVARS,
           "per_rsid_population": per_rsid_rates(WIN, cols),
           "top_values": {k: top_values(WIN, v) for k, v in ERROR_EVARS.items() if v}}
    if have("post_event_list"):
        ev = F.col("post_event_list").cast("string")
        has173 = (ev == F.lit("173")) | ev.startswith("173,") | ev.contains(",173,") | ev.endswith(",173")
        rows = (WIN.groupBy(F.col("rsid").alias("rsid"))
                   .agg(F.count(F.lit(1)).alias("rows"),
                        F.avg(F.when(has173, 1.0).otherwise(0.0)).alias("event173_rate"))
                   .orderBy(F.desc("rows")).limit(TOP_N).collect())
        out["event173"] = [{"rsid": r["rsid"], "rows": int(r["rows"]),
                            "event173_rate": float(r["event173_rate"] or 0.0)} for r in rows]
    emit("error_fields", out)


run_section("error_fields", c5_error_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C6 — Sign-in fields
# MAGIC
# MAGIC Settles whether a "Sign in % rate completion" is buildable. The metric ENGINE already
# MAGIC supports it — `gold_lib.build_kpis_spark` has a `ratio` kind that divides one metric by
# MAGIC another (CoverMe's funnel uses it). What is missing is the inputs: does anything mark a
# MAGIC sign-in attempt vs a sign-in success?

# COMMAND ----------

def c6_signin_fields():
    cols = [(k, v) for k, v in SIGNIN_EVARS.items()]
    focus = WIN.filter(F.lower(F.col("rsid")).isin(RSID_FOCUS)) if RSID_FOCUS else WIN
    emit("signin_fields", {
        "window": [START_DATE, MAX_DATE], "columns": SIGNIN_EVARS,
        "per_rsid_population": per_rsid_rates(WIN, cols),
        "top_values_all_suites": {k: top_values(WIN, v) for k, v in SIGNIN_EVARS.items() if v},
        "top_values_focus_suites": {k: top_values(focus, v) for k, v in SIGNIN_EVARS.items() if v},
        "focus_rsids": RSID_FOCUS,
        "note": ("A usable completion ratio needs two distinguishable states (attempt vs success). "
                 "If evar122 'Login Step' carries ordered step values, those are the numerator and "
                 "denominator; if it does not, the ratio has to come from events (see event_census)."),
    })


run_section("signin_fields", c6_signin_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C7 — post_event_list id census, per rsid
# MAGIC
# MAGIC GWAM's 23 tracked event ids are all unresolved, and the registry's own `report_suite_caveat`
# MAGIC warns that CoverMe's ids do not transfer. The three new suites' event spaces are wholly
# MAGIC unknown — Errors and Sign-in may well be events rather than eVars.

# COMMAND ----------

def c7_event_census():
    if not have("post_event_list"):
        raise ValueError("no post_event_list column in this feed")
    ev = F.explode(F.split(F.col("post_event_list").cast("string"), ",")).alias("tok")
    exploded = (WIN.filter(nonblank("post_event_list"))
                   .select(F.col("rsid"), ev))
    # Adobe serializes as `id` or `id=value`; split_part keeps a malformed token from
    # throwing under ANSI mode.
    eid = F.trim(split_part("tok", "=", 1))
    per_rsid = (exploded.select("rsid", eid.alias("event_id"))
                        .filter(F.col("event_id").isNotNull() & (F.col("event_id") != ""))
                        .groupBy("rsid", "event_id").count()
                        .orderBy(F.desc("count")).limit(TOP_N * 12).collect())
    grouped = {}
    for r in per_rsid:
        grouped.setdefault(r["rsid"], []).append({"event_id": r["event_id"], "count": int(r["count"])})
    emit("event_census", {
        "window": [START_DATE, MAX_DATE],
        "per_rsid_top_events": {k: v[:TOP_N] for k, v in grouped.items()},
        "n_rsids_with_events": len(grouped),
        "note": "Ids only -- this feed has no event dictionary. Labels must come from the EDDL workbook or the SME.",
    })


run_section("event_census", c7_event_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C8 — Pagename census on the focus suites
# MAGIC
# MAGIC Settles whether "Canada Retirement App Pages v2" (the Mobile channel's segment) is
# MAGIC translatable into a pagename predicate we can implement, or whether we need the Adobe
# MAGIC segment definition from the SME.

# COMMAND ----------

def c8_pagename_census():
    name = pick("post_pagename", "pagename")
    if not name:
        raise ValueError("no post_pagename/pagename column in this feed")
    out = {"window": [START_DATE, MAX_DATE], "column": name, "per_rsid": {}}
    prefix = F.trim(split_part(f"cast({sql_col(name)} as string)", ":", 1))
    for rsid in RSID_FOCUS:
        d = WIN.filter(F.lower(F.col("rsid")) == F.lit(rsid))
        pref = (d.filter(nonblank(name)).groupBy(prefix.alias("prefix"))
                 .count().orderBy(F.desc("count")).limit(TOP_N).collect())
        out["per_rsid"][rsid] = {
            "top_pagenames": top_values(d, name),
            "top_prefixes": [{"prefix": r["prefix"], "count": int(r["count"])} for r in pref],
            "retirement_like": top_values(
                d, name, extra_filter=(F.lower(qcol(name).cast("string")).contains("retire")
                                       | F.lower(qcol(name).cast("string")).contains("ret:")
                                       | F.lower(qcol(name).cast("string")).contains("retraite"))),
        }
    emit("pagename_census", out)


run_section("pagename_census", c8_pagename_census)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C9 — Can retirement be separated from other ManulifeID sign-ins?
# MAGIC
# MAGIC This is the SME's OWN flagged unknown ("Not sure how to seperate Retirement from other
# MAGIC ManulifeID signins"). We answer it from data if we can, and confirm it as genuinely blocked
# MAGIC if we cannot. Every candidate discriminator gets profiled on the ManulifeID suite.

# COMMAND ----------

def c9_manulifeid_split():
    target = SME_CHANNELS["manulifeid"]["suite_label"]  # "manucustomer.prod"
    d = WIN.filter(F.lower(F.col("rsid")) == F.lit(target.lower()))
    n = d.count()
    out = {"window": [START_DATE, MAX_DATE], "rsid": target, "rows_in_window": n}
    if n == 0:
        out["verdict"] = ("rsid not present in this table in this window -- the ManulifeID channel "
                          "cannot be profiled here at all. This is a data-ACCESS question for the SME "
                          "/ Adobe admin, not a modelling one.")
        emit("manulifeid_split", out)
        return

    candidates = {"evar105_brand": EVAR105, "evar185_platform": EVAR185,
                  "evar110_platform": EVAR110, "evar103_site_type": EVAR103}
    out["candidate_discriminators"] = {
        label: {"column": name, "top_values": top_values(d, name)} if name else {"present": False}
        for label, name in candidates.items()}
    out["candidate_population"] = per_rsid_rates(d, list(candidates.items()))

    for label, name in (("referrer", pick("referrer", "post_referrer")),
                        ("page_url", pick("page_url", "post_page_url"))):
        if name:
            host = split_part(
                f"regexp_replace(lower(cast({sql_col(name)} as string)), '^https?://', '')", "/", 1)
            rows = (d.filter(nonblank(name)).groupBy(host.alias("host"))
                     .count().orderBy(F.desc("count")).limit(TOP_N).collect())
            out[f"top_{label}_hosts"] = [{"host": r["host"], "count": int(r["count"])} for r in rows]

    out["note"] = ("If no field above isolates a retirement subset, the honest answer to the SME is "
                   "that this channel cannot be scoped to Canada Retirement with the data as tagged "
                   "-- which is a tagging change, not something we can solve downstream.")
    emit("manulifeid_split", out)


run_section("manulifeid_split", c9_manulifeid_split)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C10 — Marketing discriminator candidates
# MAGIC
# MAGIC The SME wants Page Views / Visits / Visitors "ideally non-marketing". Nothing in the repo
# MAGIC defines marketing traffic for GWAM. This profiles what could carry that meaning, so the SME
# MAGIC question can offer concrete options instead of asking an open-ended one.

# COMMAND ----------

def c10_marketing_fields():
    cols = [("campaign", pick("post_campaign", "campaign")),
            ("ref_type", pick("post_ref_type", "ref_type")),
            ("referrer", pick("post_referrer", "referrer")),
            ("channel", pick("post_channel", "channel"))]
    emit("marketing_fields", {
        "window": [START_DATE, MAX_DATE],
        "columns": {k: v for k, v in cols},
        "per_rsid_population": per_rsid_rates(WIN, cols),
        "top_values": {k: top_values(WIN, v) for k, v in cols if v},
        "note": ("Campaign-tagged share is the most likely operational definition of 'marketing', "
                 "with ref_type as the fallback. The SME picks; this just bounds the options."),
    })


run_section("marketing_fields", c10_marketing_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run manifest
# MAGIC
# MAGIC ⚠ `skipped` MUST be `{}`. A non-empty map means a section threw and its questions are still
# MAGIC open — do not read the run as complete coverage.

# COMMAND ----------

def c_run_manifest():
    sections = sorted(RESULTS.keys())
    emit("run_manifest", {
        "notebook": "gwam_channel_discovery",
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "table": TABLE_FQN, "window": [START_DATE, MAX_DATE], "window_days": WINDOW_DAYS,
        "sections": sections, "n_sections": len(sections),
        "skipped": SKIPPED,
        "complete": len(SKIPPED) == 0,
        "sme_channels": SME_CHANNELS,
    })


run_section("run_manifest", c_run_manifest)
