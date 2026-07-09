# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — Task 2/4: Silver conform
# MAGIC Detector-ready hit table: project the needed columns, derive `event_ts`, normalize
# MAGIC `post_event_list` to bare ids (plan D4), pseudonymize identity fields (ADR-0007), and
# MAGIC run DQ gates. Same `replaceWhere` window as bronze.

# COMMAND ----------
import common
common.setup_paths(dbutils)

from pyspark.sql import functions as F
from conf.settings import (
    resolve, PARTITION_COL, HMAC_SECRET_SCOPE, HMAC_SECRET_KEY, IDENTITY_COLS,
)
from conf.bronze_columns import SILVER_COLUMNS
import silver_lib as sl

s = resolve(dbutils)
if not common.gate(dbutils):
    dbutils.notebook.exit("guard: no new data")

start = dbutils.jobs.taskValues.get(taskKey="bronze", key="process_from",
                                    default=s.start_date, debugValue=s.start_date)
print("silver window >=", start)

# COMMAND ----------
# Identity HMAC key (fail loudly if unprovisioned -- privacy gate, see databricks/README.md).
try:
    hmac_key = dbutils.secrets.get(scope=HMAC_SECRET_SCOPE, key=HMAC_SECRET_KEY)
except Exception as e:
    raise RuntimeError(
        f"identity HMAC secret {HMAC_SECRET_SCOPE}/{HMAC_SECRET_KEY} not found. "
        "Provision it (databricks/README.md) before running silver."
    ) from e

# COMMAND ----------
pred = F.col(PARTITION_COL) >= F.lit(start)
b = spark.table(s.bronze).where(pred).select(*SILVER_COLUMNS)

conf = (b.withColumn("event_ts", sl.event_ts_expr())
          .withColumn("post_event_list", sl.normalize_event_list_expr("post_event_list"))
          .drop("date_time", "hit_time_gmt"))
for c in IDENTITY_COLS:
    conf = conf.withColumn(c, sl.pseudonymize_expr(c, hmac_key))
conf = conf.cache()

# COMMAND ----------
# DQ gates.
n = conf.count()
if n == 0:
    raise ValueError(f"silver DQ: 0 rows in window >= {start}")
null_ev = conf.where(F.col("post_event_list").isNull()).count()
ev_nonnull_frac = 1 - null_ev / n
vk_card = conf.select("post_visid_high", "post_visid_low").distinct().count()
print(f"DQ: rows={n} event_list_nonnull={ev_nonnull_frac:.4f} visid_pair_cardinality={vk_card}")

if ev_nonnull_frac < 0.95:
    raise ValueError(f"silver DQ: post_event_list non-null frac {ev_nonnull_frac:.3f} < 0.95")
if vk_card <= 1:
    print("WARNING: post_visid_high/low degenerate (cardinality<=1, matches EDA). "
          "visits_total collapses toward distinct(visit_num); the day-over-day change signal "
          "is still valid. Not switching the visit key silently (plan).")

# COMMAND ----------
writer = conf.write.format("delta").partitionBy(PARTITION_COL)
if spark.catalog.tableExists(s.silver):
    (writer.mode("overwrite")
           .option("replaceWhere", f"{PARTITION_COL} >= '{start}'")
           .saveAsTable(s.silver))
else:
    writer.mode("overwrite").saveAsTable(s.silver)
print(f"silver {s.silver}: {n} rows written for window >= {start}")
