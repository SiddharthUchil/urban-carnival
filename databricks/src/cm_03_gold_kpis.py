# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse CoverMe — Task 3/3: Gold KPI build
# MAGIC Registry-driven daily KPI series from detect/cm_registry (pinned to
# MAGIC metric-registry.yaml v0.4.0; CoverMe entries SME-confirmed at v0.3.0). Full rebuild
# MAGIC from silver each run — the matrix is tiny
# MAGIC (~1,200 days x 53 series). Metric semantics per Adobe datafeeds-calculate + the SME
# MAGIC rulings of 2026-07-27: 4-part visit key, visid-pair visitors, VISIT-DISTINCT event
# MAGIC counts, 0-safe funnel ratios, funnel x Product Category cube. Stored long:
# MAGIC `hit_date, metric_id, value`.
# MAGIC
# MAGIC Expected offset vs the EDA S6b figures: gold funnel counts read BELOW them
# MAGIC (visit-distinct post-eligibility-filter vs hit-presence pre-filter). Not a bug.

# COMMAND ----------
import common
common.setup_paths(dbutils)   # adds repo_root/detect so `cm_registry` imports

from conf.coverme_settings import resolve
import gold_lib
from cm_registry import (
    SERIES, EVENT_IDS, NEEDED_COLS, DATE_COL, VISIT_KEY_COLS, VISITOR_KEY_COLS,
    EVENT_BASIS, NULL_SAFE_KEYS, FUNNEL_EVENT_IDS, EVENT_METRICS, TOP_LANGUAGES, slug,
)

s = resolve(dbutils)
if not common.gate(dbutils):
    dbutils.notebook.exit("guard: no new data")

# COMMAND ----------
silver = spark.table(s.silver)                 # full history: gold is a full rebuild
wide = gold_lib.build_kpis_spark(
    silver, EVENT_IDS, SERIES,
    date_col=DATE_COL, needed_cols=NEEDED_COLS,
    visit_key_cols=VISIT_KEY_COLS, visitor_key_cols=VISITOR_KEY_COLS,
    event_basis=EVENT_BASIS, null_safe_keys=NULL_SAFE_KEYS)
long = gold_lib.melt_to_long(wide, date_col=DATE_COL)

# COMMAND ----------
(long.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(s.gold_kpi))

n_days = wide.count()
n_series = len(wide.columns) - 1
print(f"gold {s.gold_kpi}: {n_days} days x {n_series} series = {long.count()} long rows")

# COMMAND ----------
# Backfill-only sanity gates (EDA S12 first-run checks, rebased onto gold):
#   - all 5 funnel events fire somewhere in history (any silent zero needs a business
#     conversation before that series can be an anomaly KPI);
#   - both language buckets present (corroboration only -- the authoritative host check
#     runs in cm_01 at bronze where URLs exist; the retired insttrip host also derives
#     "en", so language alone cannot prove coverme.com made it through scope).
if s.mode == "backfill":
    from pyspark.sql import functions as F
    funnel_metrics = [EVENT_METRICS[eid][0] for eid in FUNNEL_EVENT_IDS]
    # Derived from the registry like the funnel ids above; "unknown" excluded — a
    # zero-total unknown bucket is healthy, not a dead series.
    lang_metrics = [f"language_share_{slug(v)}" for v in TOP_LANGUAGES if v != "unknown"]
    totals = {r["metric_id"]: r["total"]
              for r in (long.where(F.col("metric_id").isin(funnel_metrics + lang_metrics))
                        .groupBy("metric_id").agg(F.sum("value").alias("total")).collect())}
    dead = [m for m in funnel_metrics + lang_metrics if not totals.get(m, 0.0) > 0.0]
    if dead:
        raise ValueError(f"gold backfill sanity: series with zero total over full history: "
                         f"{dead}. EDA baseline says all of these fire.")
    print("backfill sanity: all funnel events fire, both language domains present")
