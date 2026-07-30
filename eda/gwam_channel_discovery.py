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
# MAGIC **↺ 2026-07-29 — the alerting scope narrowed to the Public Website channel only** (SME
# MAGIC ruling, Abhisekh). The four-channel census sections `C1`-`C10` are KEPT: they are the
# MAGIC evidence base for the 14 registry entries now marked `deferred`, so a re-widening costs a
# MAGIC status flip rather than another discovery cycle. The same update answered the marketing
# MAGIC question ("marketing" = hits carrying the **CID** query-string parameter) and named three
# MAGIC anomaly signals of its own. That is what the two new sections are for: `C11` tests whether
# MAGIC the CID rule is expressible with the column we actually ingest, and `C12` profiles the
# MAGIC per-visit page-view distribution behind the new signals. `C3` also gains sizing for two
# MAGIC brand-tag variants the SME mentioned that appear nowhere in our data.
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
# MAGIC | `C3 evar105_census` | Is `ca-retirement :  : GWAM` a literal value? What delimiter? How big is segment-scope vs today's URL scope? Plus: how much traffic do the `wealth-ca` / `pvt-wealth` brand variants carry (Q3b)? |
# MAGIC | `C4 platform_census` | Is `MPS Member` a value of eVar185 or eVar110? (resolves the long-open Platform conflict) |
# MAGIC | `C5 error_fields` | Is an Errors metric implementable, and from which field? |
# MAGIC | `C6 signin_fields` | Can a sign-in completion ratio be built, and from what? |
# MAGIC | `C7 event_census` | The `post_event_list` id space of each suite. |
# MAGIC | `C8 pagename_census` | Is "Canada Retirement App Pages v2" translatable into a pagename predicate? |
# MAGIC | `C9 manulifeid_split` | Does ANY field separate retirement sign-ins from other ManulifeID sign-ins? (the SME's own open item) |
# MAGIC | `C10 marketing_fields` | What could "ideally non-marketing" mean operationally? |
# MAGIC | `C11 cid_vs_campaign` | The SME's marketing rule is "carries CID". Is `post_campaign` — the column we actually ingest — equivalent to the presence of a `cid=` query parameter? |
# MAGIC | `C12 visit_shape` | What does the per-visit page-view distribution look like: how many visits have zero page views (the "< 1" signal), and how stable is the mass at exactly 2 (the duplication signal)? Also: ECID vs visid-pair visitor counts. |
# MAGIC
# MAGIC **What this probe canNOT settle** — deliberately out of reach of any query, and therefore
# MAGIC still SME questions: whether the `wealth-ca` / `pvt-wealth` brand variants belong to Canada
# MAGIC Retirement (C3 only sizes them — Q3b); whether "page views" means hits or Adobe page views,
# MAGIC which C12 profiles under every available basis but cannot choose between; whether 1/0 in the
# MAGIC table means in/out of scope; whether the 2026-07-20 login-exclusion rule (D8) is superseded;
# MAGIC the numerator/denominator of "Sign in % rate completion"; the friendly-name → rsid mapping
# MAGIC for "GRS+" if no candidate in C1 is recognisable; and whether the "Manulife Financial" Adobe
# MAGIC instance is the same feed (an Adobe-admin question). The business definition of
# MAGIC "non-marketing" **was** on this list and is now answered (CID) — what remains is the
# MAGIC mechanical question C11 asks.
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
# MAGIC
# MAGIC **A clean run of this version prints 13 `BEGIN SHAREABLE` blocks and reports
# MAGIC `n_sections: 12`.** That is not an off-by-one bug: `run_manifest` counts `RESULTS` *before*
# MAGIC emitting itself, so the manifest total is always one less than the block count. The previous
# MAGIC 11-section version reported `n_sections: 10` the same way. Checking `n_sections == 13` will
# MAGIC look like a section vanished when nothing did. What to actually assert:
# MAGIC `13` blocks · `n_sections: 12` · `skipped == {}` · `complete: true`.
# MAGIC
# MAGIC `scripts/decode_databricks_export.py` checks all four against an exported `.html`/`.ipynb`,
# MAGIC re-hashes every block against the manifest, and flags stdout truncation — run it on the export
# MAGIC rather than eyeballing the counts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0 — Config, constants, helpers

# COMMAND ----------

import json
import math
import hashlib
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
# ↺ 2026-07-29: only public_website is in the alerting scope now. All four stay here so the
# census sections keep reporting per-suite evidence for the deferred registry entries.
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

# The SME's brand-tag examples, verbatim from the 2026-07-29 update. The first is the value we
# already know (a second form of it); the other two name lines of business that appear NOWHERE
# in our data or docs. C3 sizes all three so doc-20 Q3b -- are wealth-ca / pvt-wealth inside
# Canada Retirement or outside it? -- can be priced instead of guessed. The scope predicate
# stays a parts-match on (ca-retirement AND gwam) until she rules.
SME_BRAND_EXAMPLES = ["Manulife: GWAM: group-plans:ca-retirement",
                      "Manulife: GWAM: wealth-ca",
                      "Manulife: GWAM : pvt-wealth"]
BRAND_VARIANTS = ["wealth-ca", "pvt-wealth"]

# Q5 ANSWERED 2026-07-29: "marketing" = hits carrying the CID campaign identifier, the standard
# query-string parameter appended to marketing URLs. Applied to a LOWERCASED url; group 1 is the
# value. C11 uses this to test the rule against post_campaign, the column the pipeline actually
# ingests -- note the pipeline strips query strings by policy (ADR-0007), so a production CID
# rule needs an ADR amendment regardless of what C11 finds.
CID_REGEX = r"[?&]cid=([^&#]*)"

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
    # NULL col makes col.like(...) NULL and False|NULL stays NULL; coalesce to False
    # so ~like_any(...) is well-defined for rows with no URL (e.g. mobile-app hits).
    return F.coalesce(expr, F.lit(False))


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
    # (d) Q3b sizing for the 2026-07-29 brand variants. Substring counts only -- these are NOT
    # part of the predicate. If a variant overlaps ca-retirement it is already counted above;
    # an overlap near zero means adopting it would ADD that much traffic, not re-label it.
    variant_flags = {v: F.lower(col).contains(v) for v in BRAND_VARIANTS}
    url_cols = [n for n in (pick("page_url"), pick("post_page_url")) if n]
    if url_cols:
        urlc = F.lower(F.coalesce(*[F.when(nonblank(n), qcol(n)) for n in url_cols], F.lit("")))
        in_url_scope = like_any(urlc, URL_SCOPE_BROAD) & ~like_any(urlc, URL_SCOPE_EXCLUDE)
    else:
        in_url_scope = F.lit(False)

    variant_aggs = []
    for v in BRAND_VARIANTS:
        vf = variant_flags[v]
        key = v.replace("-", "_")
        variant_aggs.append(F.sum(F.when(vf & is_gwam, 1).otherwise(0)).alias(f"{key}_rows"))
        variant_aggs.append(
            F.sum(F.when(vf & is_ca_ret, 1).otherwise(0)).alias(f"{key}_x_ca_ret"))

    sizing = (WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
                 .agg(F.count(F.lit(1)).alias("rows"),
                      F.sum(F.when(is_ca_ret & is_gwam, 1).otherwise(0)).alias("segment_scope"),
                      F.sum(F.when(in_url_scope, 1).otherwise(0)).alias("url_scope_broad"),
                      F.sum(F.when(is_ca_ret & is_gwam & in_url_scope, 1).otherwise(0)).alias("both"),
                      F.sum(F.when(is_ca_ret & is_gwam & ~in_url_scope, 1).otherwise(0)).alias("segment_only"),
                      F.sum(F.when(~(is_ca_ret & is_gwam) & in_url_scope, 1).otherwise(0)).alias("url_only"),
                      *variant_aggs,
                      )).collect()[0]

    per_rsid_match = (WIN.groupBy(F.col("rsid").alias("rsid"))
                         .agg(F.count(F.lit(1)).alias("rows"),
                              nonblank_rate(EVAR105).alias("evar105_populated"),
                              F.avg(F.when(is_ca_ret & is_gwam, 1.0).otherwise(0.0)).alias("ca_ret_gwam_rate"))
                         .orderBy(F.desc("rows")).limit(TOP_N).collect())

    emit("evar105_census", {
        "column": EVAR105, "window": [START_DATE, MAX_DATE],
        "sme_claimed_value": "ca-retirement :  : GWAM",
        "sme_brand_examples": SME_BRAND_EXAMPLES,
        "delimiter_probe": delim_probe, "chosen_delimiter": best,
        "top_values_all_suites": top_values(WIN, EVAR105),
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
                     "full mode=backfill with gold truncated for any scope change. ↺ 2026-07-29: with "
                     "the other three channels deferred, this trade IS the whole segment-vs-URL "
                     "decision -- the 'URL cannot express mobile' argument no longer applies."),
        },
        "brand_variant_sizing": {
            "rsid": PIPELINE_RSID, "window_rows": int(sizing["rows"]),
            "ca_retirement_rows": int(sizing["segment_scope"]),
            "variants": {v: {"rows_with_gwam": int(sizing[f'{v.replace("-", "_")}_rows']),
                             "overlap_with_ca_retirement": int(sizing[f'{v.replace("-", "_")}_x_ca_ret'])}
                         for v in BRAND_VARIANTS},
            "note": ("Sizes doc-20 Q3b, it does not answer it: are wealth-ca / pvt-wealth inside "
                     "Canada Retirement? rows_with_gwam is what including a variant would add; a "
                     "near-zero overlap_with_ca_retirement confirms the variant is additive rather "
                     "than already counted. The predicate stays a parts-match on (ca-retirement AND "
                     "gwam) until the SME rules."),
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
            "top_values_all_suites": top_values(WIN, name),
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
           "top_values_all_suites": {k: top_values(WIN, v) for k, v in ERROR_EVARS.items() if v}}
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
        "top_values_all_suites": {k: top_values(WIN, v) for k, v in cols if v},
        "note": ("Campaign-tagged share is the most likely operational definition of 'marketing', "
                 "with ref_type as the fallback. The SME picks; this just bounds the options. "
                 "↺ 2026-07-29: she picked neither -- the rule is 'carries the CID query "
                 "parameter'. C11 tests whether campaign is the same thing."),
    })


run_section("marketing_fields", c10_marketing_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C11 — Is `post_campaign` the same thing as a `cid=` query parameter?
# MAGIC
# MAGIC The SME's marketing rule (2026-07-29) is **"carries CID"** — a query-string parameter. Our
# MAGIC pipeline ingests Adobe's `post_campaign` column and **strips query strings by policy**
# MAGIC (ADR-0007), so before anyone writes an exclusion rule we need to know whether the column we
# MAGIC have stands in for the parameter she named.
# MAGIC
# MAGIC Read the result knowing the two are *not* expected to match row-for-row: Adobe persists a
# MAGIC campaign value across the visit, while `cid=` appears only on the landing hit. So
# MAGIC `campaign_only >> cid_only` is normal and healthy. The two figures that matter are
# MAGIC **`cid_only` ≈ 0** (nothing carries CID that the column misses) and a high
# MAGIC **`equal_when_both / both`** (when both are present they agree). If either fails, the rule
# MAGIC cannot be implemented from `post_campaign` and needs the raw query string — which means an
# MAGIC ADR-0007 amendment, not just a settings change.
# MAGIC
# MAGIC **Privacy.** This section reads raw URLs transiently and emits **counts only** — never a
# MAGIC query-string value, never a URL. That matches the repo's posture (the EDA notebooks strip
# MAGIC query strings because session tokens live there) while still answering the question.

# COMMAND ----------

def c11_cid_vs_campaign():
    url_col = pick("page_url", "post_page_url")
    if not url_col:
        raise ValueError("no page_url/post_page_url column in this feed")
    camp_col = pick("post_campaign", "campaign")

    lurl = F.lower(qcol(url_col).cast("string"))
    cid = F.regexp_extract(F.coalesce(lurl, F.lit("")), CID_REGEX, 1)
    has_cid = F.length(cid) > 0
    if camp_col:
        camp = F.lower(F.trim(qcol(camp_col).cast("string")))
        has_camp = nonblank(camp_col)
    else:
        camp, has_camp = F.lit(""), F.lit(False)

    def xtab(df):
        r = df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(has_cid, 1).otherwise(0)).alias("cid_rows"),
            F.sum(F.when(has_camp, 1).otherwise(0)).alias("campaign_rows"),
            F.sum(F.when(has_cid & has_camp, 1).otherwise(0)).alias("both"),
            F.sum(F.when(has_cid & ~has_camp, 1).otherwise(0)).alias("cid_only"),
            F.sum(F.when(~has_cid & has_camp, 1).otherwise(0)).alias("campaign_only"),
            F.sum(F.when(has_cid & has_camp & (cid == camp), 1).otherwise(0)).alias("equal_when_both"),
        ).collect()[0]
        out = {k: int(r[k] or 0) for k in ("rows", "cid_rows", "campaign_rows", "both",
                                          "cid_only", "campaign_only", "equal_when_both")}
        out["agreement_when_both"] = (out["equal_when_both"] / out["both"]) if out["both"] else None
        return out

    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
    payload = {
        "window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID,
        "url_column": url_col, "campaign_column": camp_col,
        "cid_regex": CID_REGEX,
        "suite_all": xtab(d),
    }
    if EVAR105:
        c105 = F.lower(qcol(EVAR105).cast("string"))
        payload["segment_scope"] = xtab(
            d.filter(c105.contains("ca-retirement") & c105.contains("gwam")))
    payload["note"] = (
        "Tests the 2026-07-29 marketing rule (Q5: marketing = carries CID) against the column we "
        "ingest. EXPECTED asymmetry: post_campaign persists across a visit while cid= appears only "
        "on landing URLs, so campaign_only >> cid_only is normal. The real checks are cid_only ~ 0 "
        "and a high agreement_when_both. Counts only -- no query-string values or URLs are emitted "
        "(ADR-0007 posture). Even a clean result does not make the rule shippable: bronze projects "
        "post_page_url and strips query strings, so parsing cid= in production needs an ADR "
        "amendment (doc-16 backlog).")
    emit("cid_vs_campaign", payload)


run_section("cid_vs_campaign", c11_cid_vs_campaign)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C12 — Per-visit page-view distribution, and the ECID-vs-visid-pair visitor grain
# MAGIC
# MAGIC The SME named three anomaly signals on 2026-07-29: **unique ECID visitors**, **page views
# MAGIC per visit < 1**, and **pages consistently at exactly 2** as a duplication indicator. None of
# MAGIC the three exists as a declared metric today, and two of them are per-visit ratios we have
# MAGIC never measured. This section profiles the distribution first, so thresholds come from data.
# MAGIC
# MAGIC Three things worth knowing about how to read it:
# MAGIC
# MAGIC - **"< 1" is really a zero-page-view-visit detector.** A daily total of page views over
# MAGIC   visits can only fall below 1 if visits exist that contain no page view at all, so
# MAGIC   `share_pv_eq_0` is the direct measurement of her signal.
# MAGIC - **The "exactly 2" signal is about consistency, not level.** A stable point mass at 2 is
# MAGIC   what indicates duplication, so the daily series matters more than the window average —
# MAGIC   and no current scorer (robust-z, level-shift, ECOD, rules) expresses "unusually stable".
# MAGIC - **The page-view basis is still an open SME question** (doc-20 Q6). `pv_basis` in the
# MAGIC   payload records which definition was available; the fallbacks are deliberate, not silent.
# MAGIC
# MAGIC The `visitor_grain` block settles something else: gold counts visitors as
# MAGIC `countDistinct(mcvisid)` (the ECID, which is what the SME named) while the EDA notebooks
# MAGIC count the `post_visid` pair. Both are defensible; they are not equal, and until now nobody
# MAGIC had measured the gap.

# COMMAND ----------

def c12_visit_shape():
    key_cols = ["post_visid_high", "post_visid_low", "visit_num"]
    missing = [c for c in key_cols if not have(c)]
    if missing:
        raise ValueError(f"visit key columns missing from this feed: {missing}")

    # Page-view basis, best available. page_event == 0 is Adobe's own page-view marker; a
    # populated pagename is the weaker proxy; all-hits is the honest last resort. Whichever
    # is used is reported, because doc-20 Q6 has not been answered.
    #
    # try_cast, never cast: Databricks runs ANSI mode, so a single non-numeric page_event
    # value would throw and kill the section -- the CoverMe E1 defect. try_cast yields NULL
    # instead, which simply fails the == 0 test. Verified against a non-numeric row locally.
    pe = pick("post_page_event", "page_event")
    pn = pick("post_pagename", "pagename")
    if pe:
        pv_flag = F.expr(f"try_cast({sql_col(pe)} as int)") == F.lit(0)
        pv_basis = f"try_cast({pe} as int) == 0 (Adobe page-view marker)"
    elif pn:
        pv_flag, pv_basis = nonblank(pn), f"nonblank({pn}) (proxy -- no page_event column)"
    else:
        pv_flag, pv_basis = F.lit(True), "ALL HITS (no page_event and no pagename column)"

    # NULLs coalesced positionally so distinct visit keys cannot collide (the NULL_SAFE_KEYS
    # convention from cm_registry).
    key_exprs = [F.coalesce(qcol(c).cast("string"), F.lit("~null~")) for c in key_cols]
    key = [e.alias(c) for e, c in zip(key_exprs, key_cols)]
    bucket = (F.when(F.col("pvs") == 0, "0")
               .when(F.col("pvs") == 1, "1")
               .when(F.col("pvs") == 2, "2")
               .when(F.col("pvs") <= 5, "3-5")
               .otherwise("6+"))

    def per_visit(df):
        return df.groupBy(*key).agg(F.count(F.lit(1)).alias("hits"),
                                    F.sum(F.when(pv_flag, 1).otherwise(0)).alias("pvs"),
                                    F.min("process_date").alias("visit_date"))

    def shape(pv):
        agg = pv.agg(F.count(F.lit(1)).alias("visits"),
                     F.sum("pvs").alias("pvs"),
                     F.avg((F.col("pvs") == 2).cast("double")).alias("eq2"),
                     F.avg((F.col("pvs") == 0).cast("double")).alias("eq0")).collect()[0]
        n = int(agg["visits"] or 0)
        dist = {r["b"]: int(r["count"])
                for r in pv.groupBy(bucket.alias("b")).count().collect()}
        daily = (pv.groupBy("visit_date")
                   .agg(F.count(F.lit(1)).alias("visits"),
                        F.sum("pvs").alias("pvs"),
                        F.avg((F.col("pvs") == 2).cast("double")).alias("share_eq2"))
                   .orderBy("visit_date").collect())
        return {
            "visits": n,
            "pv_per_visit": (float(agg["pvs"] or 0) / n) if n else None,
            "share_pv_eq_0": float(agg["eq0"] or 0.0),
            "share_pv_eq_2": float(agg["eq2"] or 0.0),
            "bucket_dist": dist,
            "daily": [{"date": str(r["visit_date"])[:10], "visits": int(r["visits"]),
                       "pv_per_visit": (float(r["pvs"] or 0) / int(r["visits"])) if r["visits"] else None,
                       "share_eq2": float(r["share_eq2"] or 0.0)} for r in daily],
        }

    d = WIN.filter(F.col("rsid") == F.lit(PIPELINE_RSID))
    payload = {"window": [START_DATE, MAX_DATE], "rsid": PIPELINE_RSID,
               "visit_key": key_cols, "pv_basis": pv_basis,
               "suite_all": shape(per_visit(d))}
    if EVAR105:
        c105 = F.lower(qcol(EVAR105).cast("string"))
        payload["segment_scope"] = shape(per_visit(
            d.filter(c105.contains("ca-retirement") & c105.contains("gwam"))))

    ecid = pick("mcvisid", "post_mcvisid")
    if ecid:
        rows = (d.groupBy("process_date")
                 .agg(F.countDistinct(qcol(ecid)).alias("ecid"),
                      F.countDistinct(F.concat_ws(":", *key_exprs[:2])).alias("pair"))
                 .orderBy("process_date").collect())
        payload["visitor_grain"] = {
            "ecid_column": ecid,
            "daily": [{"date": str(r["process_date"])[:10], "ecid": int(r["ecid"]),
                       "pair": int(r["pair"])} for r in rows],
            "note": ("Quantifies a divergence nobody had measured: gold counts visitors as "
                     "countDistinct(mcvisid) (gold_lib.py:94) -- the ECID the SME named -- while "
                     "the EDA notebooks count the post_visid pair "
                     "(gwam_canada_retirement_eda.py:1320). Registry entry gwam_pw_visitors."),
        }
    else:
        payload["visitor_grain"] = {"ecid_column": None,
                                    "note": "no mcvisid column in this feed -- ECID grain unmeasurable"}

    payload["note"] = (
        "Profiles the SME's 2026-07-29 anomaly signals before they are declared. share_pv_eq_0 IS "
        "the 'page views per visit < 1' signal (registry gwam_pw_pv_per_visit): a daily ratio can "
        "only drop below 1 if zero-page-view visits exist. share_pv_eq_2 plus its daily series IS "
        "the duplication signal (registry gwam_pw_pv_per_visit_dup2), where the SME's concern is "
        "CONSISTENCY at 2 rather than the level -- read the daily spread, not the average. Both "
        "metrics are blocked on doc-19 G2 (SeriesSpec has no numerator/denominator) and doc-20 Q6 "
        "(which page-view basis is meant -- see pv_basis above).")
    emit("visit_shape", payload)


run_section("visit_shape", c12_visit_shape)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run manifest
# MAGIC
# MAGIC ⚠ `skipped` MUST be `{}`. A non-empty map means a section threw and its questions are still
# MAGIC open — do not read the run as complete coverage.

# COMMAND ----------

def c_run_manifest():
    # Byte length + sha1 of every shareable section, from the exact JSON that was
    # printed — the truncation guard the header warns about (doc-16 §0.5).
    sections = {}
    for sid, payload in RESULTS.items():
        body = json.dumps(payload, separators=(",", ":"), default=str)
        sections[sid] = {"bytes": len(body), "sha1": hashlib.sha1(body.encode("utf-8")).hexdigest()}
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
