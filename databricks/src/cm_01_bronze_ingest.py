# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse CoverMe — Task 1/3: Bronze ingest
# MAGIC Scoped, pruned mirror of the CoverMe Adobe hit table. Scope is **URL-only** (the
# MAGIC single-suite feed has no `rsid` column): the blank-guarded page_url-first coalesce
# MAGIC must match a brand-domain include pattern and none of the UAT/AEM excludes. Hit
# MAGIC eligibility (`exclude_hit` / `hit_source`) is deliberately NOT applied here — bronze
# MAGIC keeps raw scoped rows so the excluded volume stays observable; silver filters.
# MAGIC Idempotent `replaceWhere` partition overwrite with `OVERLAP_DAYS` (=5, late-arrival
# MAGIC p99) of trailing reprocessing.

# COMMAND ----------
from datetime import date, timedelta

import common
common.setup_paths(dbutils)

from pyspark.sql import functions as F
from conf.coverme_settings import (
    resolve, SOURCE_TABLE, PARTITION_COL,
    URL_SCOPE_INCLUDE, URL_SCOPE_EXCLUDE,
    BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, OVERLAP_DAYS,
)
from conf.coverme_bronze_columns import bronze_select, REQUIRED_SOURCE_COLUMNS
import cm_silver_lib as cml

s = resolve(dbutils)
if not common.gate(dbutils):
    dbutils.notebook.exit("guard: no new data")

# COMMAND ----------
# Target schemas (idempotent). Catalog itself must already exist and be writable.
for sch in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s.catalog}.{sch}")

# COMMAND ----------
src = spark.table(SOURCE_TABLE)
common.assert_source_columns(src.columns, REQUIRED_SOURCE_COLUMNS,   # schema contract, ADR-0006
                             conf_hint="databricks/conf/coverme_bronze_columns.py")
cols = bronze_select(src.columns)
print(f"projecting {len(cols)} columns into bronze")

# COMMAND ----------
# Ingest window: full backfill on first run / mode=backfill, else trailing overlap.
bronze_wm = common.read_watermark(spark, s.bronze, PARTITION_COL)
if s.mode == "backfill" or bronze_wm is None:
    start = s.start_date
else:
    start = (date.fromisoformat(bronze_wm) - timedelta(days=OVERLAP_DAYS)).isoformat()
print(f"ingest window: {PARTITION_COL} >= {start} (mode={s.mode}, bronze_wm={bronze_wm})")

# COMMAND ----------
# Predicate honoring the real partition dtype so Delta prunes partitions (hit_date is a
# typed date on this feed, but stay dtype-aware like the GWAM ingest).
pcol_type = dict(src.dtypes)[PARTITION_COL]
if pcol_type == "date":
    pred = F.col(PARTITION_COL) >= F.lit(start).cast("date")
else:
    pred = F.col(PARTITION_COL) >= F.lit(start)   # string 'YYYY-MM-DD' compares lexically

scoped = (src.where(pred)
             .where(cml.scope_expr(URL_SCOPE_INCLUDE, URL_SCOPE_EXCLUDE))
             .select(*cols))

# COMMAND ----------
writer = scoped.write.format("delta").partitionBy(PARTITION_COL)
if spark.catalog.tableExists(s.bronze):
    (writer.mode("overwrite")
           .option("replaceWhere", f"{PARTITION_COL} >= '{start}'")
           .saveAsTable(s.bronze))
else:
    writer.mode("overwrite").saveAsTable(s.bronze)

n = spark.table(s.bronze).where(pred).count()
common.set_task_value(dbutils, "process_from", start)
print(f"bronze {s.bronze}: {n} rows in window >= {start}")

# COMMAND ----------
# Backfill-only sanity (design spec): both production brand domains present in bronze.
# This check lives HERE, where URLs exist -- the gold language gate can't distinguish
# coverme.com from the retired insttrip host (both derive language "en").
if s.mode == "backfill":
    u = cml.url_expr()
    hosts = (spark.table(s.bronze).where(pred)
             .agg(F.sum(F.when(u.like("%coverme.com%"), 1).otherwise(0)).alias("coverme"),
                  F.sum(F.when(u.like("%pourmeproteger.com%"), 1).otherwise(0))
                   .alias("pourmeproteger"))
             .first())
    dead = [h for h in ("coverme", "pourmeproteger") if not (hosts[h] or 0) > 0]
    if dead:
        raise ValueError(f"bronze backfill sanity: zero rows for production host(s) {dead} "
                         "in the ingest window -- scope filter regression?")
    print(f"backfill sanity: coverme.com={hosts['coverme']} "
          f"pourmeproteger.com={hosts['pourmeproteger']}")
