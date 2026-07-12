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
    resolve, SOURCE_TABLE, PARTITION_COL, SCOPE_RSID,
    SCOPE_URL_MODE, SCOPE_URL_LIKE, SCOPE_URL_LIKE_BROAD, SCOPE_URL_LIKE_EXCLUDE,
    SCOPE_SUITE_MODE, LEGACY_SCOPE_RSID, LEGACY_SCOPE_URL_LIKE,
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

# URL scope. Default ("en_only") is the shipped English section root, applied to
# post_page_url byte-for-byte as before. "broad" matches SCOPE_URL_LIKE_BROAD on the COMPLETE
# url (coalesce(page_url, post_page_url) -- post_page_url is ~37% blank, EDA S4b) and subtracts
# SCOPE_URL_LIKE_EXCLUDE (Adobe AEM author hosts + non-CA /ph/ paths). Flip SCOPE_URL_MODE only
# after a re-profile -- it changes the ingested population and re-baselines downstream.
if SCOPE_URL_MODE == "broad":
    from functools import reduce
    _urlc = F.lower(F.coalesce(F.col("page_url"), F.col("post_page_url")).cast("string"))
    _incl = reduce(lambda acc, p: acc | _urlc.like(p.lower()), SCOPE_URL_LIKE_BROAD, F.lit(False))
    _excl = reduce(lambda acc, p: acc | _urlc.like(p.lower()), SCOPE_URL_LIKE_EXCLUDE, F.lit(False))
    url_scope = _incl & ~_excl
else:
    url_scope = F.col("post_page_url").like(SCOPE_URL_LIKE)

# Suite scope. "current_only" (default) keeps exactly the shipped population:
# rsid == SCOPE_RSID AND url_scope (two ANDed conditions, row-set-identical to the prior
# chained .where() calls). "with_legacy" ALSO unions the pre-Storefront suite `manugrs`
# (research/claude/14-manugrs-cross-suite-analysis.md) so its ~2.5 yr of history backfills.
# Legacy URL lives mostly in page_url (post_page_url ~48% blank on manugrs, EDA S4b), so its
# scope matches LEGACY_SCOPE_URL_LIKE on the COMPLETE url. Flip SCOPE_SUITE_MODE only after
# business sign-off -- it changes the ingested population and re-baselines downstream.
# NOTE: bronze projects post_page_url but not page_url (conf/bronze_columns.py); if legacy rows
# are ingested, adding page_url to the bronze projection is a candidate follow-up (deferred).
current_scope = (F.col("rsid") == F.lit(SCOPE_RSID)) & url_scope
if SCOPE_SUITE_MODE == "with_legacy":
    from functools import reduce
    _legurlc = F.lower(F.coalesce(F.col("page_url"), F.col("post_page_url")).cast("string"))
    _leg_incl = reduce(lambda acc, p: acc | _legurlc.like(p.lower()), LEGACY_SCOPE_URL_LIKE, F.lit(False))
    legacy_scope = (F.col("rsid") == F.lit(LEGACY_SCOPE_RSID)) & _leg_incl
    suite_scope = current_scope | legacy_scope
else:
    suite_scope = current_scope

scoped = (src.where(pred)
             .where(suite_scope)
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
