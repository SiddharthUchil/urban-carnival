"""Pure PySpark port of detect/kpis.build_kpis, factored out for unit testing against the
pandas implementation (tests/test_gold_parity.py). Requires pyspark; no dbutils.

Semantics mirror detect/kpis.py exactly:
  - one row per calendar day, gap-free from min(date) to max(date)
  - hits = row count; visits = distinct(high_low_visitnum); visitors = distinct(mcvisid)
  - event counts = per-day occurrences of each tracked id in post_event_list
  - event rate = count / hits; dim share = rows(dim==value) / hits; 0.0 where hits == 0
Tokens are assumed already bare (silver normalizes via silver_lib; synthetic data is bare).
"""
from __future__ import annotations

from pyspark.sql import functions as F

# Same columns detect/kpis.py reads (NEEDED_COLS).
NEEDED_COLS = [
    "process_date", "post_event_list", "post_pagename", "language",
    "mcvisid", "post_visid_high", "post_visid_low", "visit_num",
]


def build_kpis_spark(df, event_ids, series):
    """Return a wide DataFrame ``[process_date] + [metric_id...]`` matching build_kpis().

    ``event_ids`` and ``series`` come from detect/registry (dependency-injected so this
    module imports without the detect/ package on the path).
    """
    d = df.select(*NEEDED_COLS).withColumn("date", F.to_date("process_date"))

    # Gap-free calendar derived from the data itself (pure DataFrame ops -- no local
    # createDataFrame, so the lineage stays JVM-only).
    cal = (d.agg(F.min("date").alias("mn"), F.max("date").alias("mx"))
           .select(F.explode(F.sequence(F.col("mn"), F.col("mx"),
                                        F.expr("interval 1 day"))).alias("date")))

    hits = d.groupBy("date").count().withColumnRenamed("count", "hits_total")

    vkey = F.concat_ws(
        "_",
        F.col("post_visid_high").cast("string"),
        F.col("post_visid_low").cast("string"),
        F.col("visit_num").cast("string"),
    )
    visits = (d.withColumn("_vk", vkey).groupBy("date")
              .agg(F.countDistinct("_vk").alias("visits_total")))
    visitors = d.groupBy("date").agg(F.countDistinct("mcvisid").alias("visitors_total"))

    ev = (d.select("date", F.explode(F.split("post_event_list", ",")).alias("ev"))
          .where(F.col("ev").isNotNull() & (F.col("ev") != "") & F.col("ev").isin(event_ids)))
    evc = ev.groupBy("date").pivot("ev", event_ids).count()

    base = (cal.join(hits, "date", "left")
               .join(visits, "date", "left")
               .join(visitors, "date", "left")
               .join(evc, "date", "left"))
    count_cols = ["hits_total", "visits_total", "visitors_total"] + list(event_ids)
    base = base.fillna(0, subset=[c for c in count_cols if c in base.columns])

    # One left join per tracked dimension value (mirrors pandas' per-value groupby count).
    share_specs = [s for s in series if s.kind == "share"]
    for i, s in enumerate(share_specs):
        cnt_col = f"_share_cnt_{i}"
        cdf = (d.where(F.col(s.dim) == F.lit(s.dim_value))
                 .groupBy("date").count().withColumnRenamed("count", cnt_col))
        base = base.join(cdf, "date", "left").fillna(0, subset=[cnt_col])
    share_idx = {id(s): i for i, s in enumerate(share_specs)}

    out = [F.col("date").alias("process_date")]
    for s in series:
        if s.source == "hits":
            c = F.col("hits_total")
        elif s.source == "visits":
            c = F.col("visits_total")
        elif s.source == "visitors":
            c = F.col("visitors_total")
        elif s.source == "event" and s.kind == "count":
            c = F.col(f"`{s.event_id}`")
        elif s.source == "event" and s.kind == "rate":
            c = F.when(F.col("hits_total") > 0,
                       F.col(f"`{s.event_id}`") / F.col("hits_total")).otherwise(F.lit(0.0))
        elif s.kind == "share":
            cnt = F.col(f"_share_cnt_{share_idx[id(s)]}")
            c = F.when(F.col("hits_total") > 0, cnt / F.col("hits_total")).otherwise(F.lit(0.0))
        else:
            raise ValueError(f"unhandled series spec: {s}")
        out.append(c.alias(s.metric_id))

    return base.select(*out).orderBy("process_date")


def melt_to_long(wide):
    """Wide ``[process_date, metric...]`` -> long ``[process_date, metric_id, value:double]``."""
    metric_cols = [c for c in wide.columns if c != "process_date"]
    pairs = ", ".join([f"'{c}', cast(`{c}` as double)" for c in metric_cols])
    stack_expr = f"stack({len(metric_cols)}, {pairs}) as (metric_id, value)"
    return wide.select("process_date", F.expr(stack_expr))
