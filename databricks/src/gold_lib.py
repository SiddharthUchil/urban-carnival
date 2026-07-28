"""Pure PySpark port of detect/kpis.build_kpis, factored out for unit testing against the
pandas implementation (tests/test_gold_parity.py). Requires pyspark; no dbutils.

Default-argument semantics mirror detect/kpis.py exactly (the GWAM build):
  - one row per calendar day, gap-free from min(date) to max(date)
  - hits = row count; visits = distinct(high_low_visitnum); visitors = distinct(mcvisid)
  - event counts = per-day occurrences of each tracked id in post_event_list
  - event rate = count / hits; dim share = rows(dim==value) / hits; 0.0 where hits == 0
Tokens are assumed already bare (silver normalizes via silver_lib; synthetic data is bare).

The CoverMe build (detect/cm_registry, tests/test_cm_gold.py) opts into extensions via
keyword arguments -- a different date/partition column, the Adobe 4-part visit key, a
column-pair visitor key, visit-distinct event counting (SME ruling 2026-07-27), plus two
extra series kinds:
  - kind=ratio: numerator/denominator reference sibling metric_ids; 0.0 when the
    denominator is 0 (never NULL/inf)
  - source=event_dim: per-day event count segmented by a dimension bucket; specs with
    dim_value=None collect the "other" bucket (values -- including NULL/blank -- not
    claimed by any sibling spec on the same dim). A visit whose hits carry the same event
    under different dim values counts once per bucket, so bucket sums can exceed the flat
    visit-distinct event count.
"""
from __future__ import annotations

from pyspark.sql import functions as F

# Same columns detect/kpis.py reads (NEEDED_COLS).
NEEDED_COLS = [
    "process_date", "post_event_list", "post_pagename", "language",
    "mcvisid", "post_visid_high", "post_visid_low", "visit_num",
]

_OTHER = "__other__"   # event_dim bucket label for values not claimed by a spec


def _evd_key(spec):
    """Pivot column name for an event_dim spec."""
    return f"{spec.event_id}||{spec.dim_value if spec.dim_value is not None else _OTHER}"


def _key_expr(cols, null_safe):
    """Composite distinct-count key over ``cols``.

    concat_ws silently SKIPS NULL components, so ("h", NULL, "1") and ("h", "1", NULL)
    both collapse to "h_1" and an all-NULL tuple becomes a countable "". With
    ``null_safe`` a NULL component keeps its position via a sentinel, and a fully-NULL
    key becomes NULL so countDistinct skips it (matching single-column countDistinct
    semantics). The non-safe form is kept as the default for GWAM parity.
    """
    parts = [F.col(c).cast("string") for c in cols]
    if not null_safe:
        return F.concat_ws("_", *parts)
    key = F.concat_ws("_", *[F.coalesce(p, F.lit("␀")) for p in parts])
    all_null = parts[0].isNull()
    for p in parts[1:]:
        all_null = all_null & p.isNull()
    return F.when(all_null, F.lit(None).cast("string")).otherwise(key)


def build_kpis_spark(df, event_ids, series,
                     date_col="process_date",
                     needed_cols=None,
                     visit_key_cols=("post_visid_high", "post_visid_low", "visit_num"),
                     visitor_key_cols=None,
                     event_basis="hits",
                     null_safe_keys=False):
    """Return a wide DataFrame ``[date_col] + [metric_id...]`` matching build_kpis().

    ``event_ids`` and ``series`` come from a registry module (dependency-injected so this
    module imports without the detect/ package on the path). Defaults reproduce the GWAM
    build byte-for-byte; ``visitor_key_cols=None`` keeps the exact distinct(mcvisid)
    semantics (a concat key would turn NULLs into a countable empty string).
    ``event_basis="visits"`` counts distinct visit keys carrying the event instead of
    token occurrences. ``null_safe_keys`` opts the composite visit/visitor keys into the
    NULL-positional handling documented on ``_key_expr`` (CoverMe passes True; the GWAM
    default stays False for parity).
    """
    if event_basis not in ("hits", "visits"):
        raise ValueError(f"unknown event_basis: {event_basis!r}")
    d = df.select(*(needed_cols or NEEDED_COLS)).withColumn("date", F.to_date(date_col))

    # Gap-free calendar derived from the data itself (pure DataFrame ops -- no local
    # createDataFrame, so the lineage stays JVM-only).
    cal = (d.agg(F.min("date").alias("mn"), F.max("date").alias("mx"))
           .select(F.explode(F.sequence(F.col("mn"), F.col("mx"),
                                        F.expr("interval 1 day"))).alias("date")))

    hits = d.groupBy("date").count().withColumnRenamed("count", "hits_total")

    vkey = _key_expr(visit_key_cols, null_safe_keys)
    visits = (d.withColumn("_vk", vkey).groupBy("date")
              .agg(F.countDistinct("_vk").alias("visits_total")))
    if visitor_key_cols is None:
        visitors = d.groupBy("date").agg(F.countDistinct("mcvisid").alias("visitors_total"))
    else:
        vis_key = _key_expr(visitor_key_cols, null_safe_keys)
        visitors = (d.withColumn("_vr", vis_key).groupBy("date")
                    .agg(F.countDistinct("_vr").alias("visitors_total")))

    ev = (d.withColumn("_vk", vkey)
          .select("date", "_vk", F.explode(F.split("post_event_list", ",")).alias("ev"))
          .where(F.col("ev").isNotNull() & (F.col("ev") != "") & F.col("ev").isin(event_ids)))
    if event_basis == "visits":
        evc = ev.groupBy("date").pivot("ev", event_ids).agg(F.countDistinct("_vk"))
    else:
        evc = ev.groupBy("date").pivot("ev", event_ids).count()

    base = (cal.join(hits, "date", "left")
               .join(visits, "date", "left")
               .join(visitors, "date", "left")
               .join(evc, "date", "left"))
    count_cols = ["hits_total", "visits_total", "visitors_total"] + list(event_ids)
    base = base.fillna(0, subset=[c for c in count_cols if c in base.columns])

    # Event x dimension cube: one aggregation per distinct dim, pivoted on
    # "<event_id>||<bucket>". NULL/blank/off-list dim values land in the _OTHER bucket.
    evd_specs = [s for s in series if s.source == "event_dim"]
    by_dim = {}
    for s in evd_specs:
        by_dim.setdefault(s.dim, []).append(s)
    for dim, specs in by_dim.items():
        explicit = [s.dim_value for s in specs if s.dim_value is not None]
        eids = sorted({s.event_id for s in specs})
        keys = [_evd_key(s) for s in specs]
        bucket = F.when(F.col(dim).isin(explicit), F.col(dim)).otherwise(F.lit(_OTHER))
        evd = (d.withColumn("_vk", vkey).withColumn("_bkt", bucket)
               .select("date", "_vk", "_bkt",
                       F.explode(F.split("post_event_list", ",")).alias("ev"))
               .where(F.col("ev").isin(eids))
               .withColumn("_k", F.concat_ws("||", F.col("ev"), F.col("_bkt"))))
        agg = F.countDistinct("_vk") if event_basis == "visits" else F.count(F.lit(1))
        base = base.join(evd.groupBy("date").pivot("_k", keys).agg(agg), "date", "left")
        base = base.fillna(0, subset=keys)

    # One left join per tracked dimension value (mirrors pandas' per-value groupby count).
    share_specs = [s for s in series if s.kind == "share"]
    for i, s in enumerate(share_specs):
        cnt_col = f"_share_cnt_{i}"
        cdf = (d.where(F.col(s.dim) == F.lit(s.dim_value))
                 .groupBy("date").count().withColumnRenamed("count", cnt_col))
        base = base.join(cdf, "date", "left").fillna(0, subset=[cnt_col])
    share_idx = {id(s): i for i, s in enumerate(share_specs)}

    # Two passes: ratios reference sibling metric expressions, so build those first.
    exprs = {}
    for s in series:
        if s.kind == "ratio":
            continue
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
        elif s.source == "event_dim":
            c = F.col(f"`{_evd_key(s)}`")
        elif s.kind == "share":
            cnt = F.col(f"_share_cnt_{share_idx[id(s)]}")
            c = F.when(F.col("hits_total") > 0, cnt / F.col("hits_total")).otherwise(F.lit(0.0))
        else:
            raise ValueError(f"unhandled series spec: {s}")
        exprs[s.metric_id] = c
    for s in series:
        if s.kind != "ratio":
            continue
        if s.numerator not in exprs or s.denominator not in exprs:
            raise ValueError(f"ratio {s.metric_id} references unknown metric_ids: "
                             f"{s.numerator} / {s.denominator}")
        num, den = exprs[s.numerator], exprs[s.denominator]
        exprs[s.metric_id] = F.when(den > 0, num / den).otherwise(F.lit(0.0))

    out = [F.col("date").alias(date_col)]
    out += [exprs[s.metric_id].alias(s.metric_id) for s in series]
    return base.select(*out).orderBy(date_col)


def melt_to_long(wide, date_col="process_date"):
    """Wide ``[date_col, metric...]`` -> long ``[date_col, metric_id, value:double]``."""
    metric_cols = [c for c in wide.columns if c != date_col]
    pairs = ", ".join([f"'{c}', cast(`{c}` as double)" for c in metric_cols])
    stack_expr = f"stack({len(metric_cols)}, {pairs}) as (metric_id, value)"
    return wide.select(date_col, F.expr(stack_expr))
