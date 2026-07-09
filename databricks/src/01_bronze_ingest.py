# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — Task 1/4: Bronze ingest
# MAGIC Scoped, pruned, append-style mirror of the source Adobe hit table (plan D1/D2).
# MAGIC Applies the GWAM CA-retirement filter, keeps the populated/required columns (minus
# MAGIC sensitive ones), and writes an idempotent `replaceWhere` partition overwrite over a
# MAGIC `process_date` window with `OVERLAP_DAYS` of trailing reprocessing for late micro-batches.

# COMMAND ----------
from datetime import date, timedelta

import common
common.setup_paths(dbutils)

from pyspark.sql import functions as F
from conf.settings import (
    resolve, SOURCE_TABLE, PARTITION_COL, SCOPE_RSID, SCOPE_URL_LIKE,
    BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, OVERLAP_DAYS,
)
from conf.bronze_columns import bronze_select, REQUIRED_SOURCE_COLUMNS

s = resolve(dbutils)
if not common.gate(dbutils):
    dbutils.notebook.exit("guard: no new data")

# COMMAND ----------
# Target schemas (idempotent). Catalog itself must already exist and be writable.
for sch in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s.catalog}.{sch}")

# COMMAND ----------
src = spark.table(SOURCE_TABLE)
common.assert_source_columns(src.columns, REQUIRED_SOURCE_COLUMNS)   # schema contract, ADR-0006
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
# Predicate honoring the real partition dtype so Delta prunes partitions.
pcol_type = dict(src.dtypes)[PARTITION_COL]
if pcol_type == "date":
    pred = F.col(PARTITION_COL) >= F.lit(start).cast("date")
else:
    pred = F.col(PARTITION_COL) >= F.lit(start)   # string 'YYYY-MM-DD' compares lexically

scoped = (src.where(pred)
             .where(F.col("rsid") == F.lit(SCOPE_RSID))
             .where(F.col("post_page_url").like(SCOPE_URL_LIKE))
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
