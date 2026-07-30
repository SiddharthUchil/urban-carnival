# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse CoverMe — Task 2/3: Silver conform
# MAGIC KPI-ready hit table: apply the Workspace-parity eligibility filter (`exclude_hit = 0`,
# MAGIC `hit_source` not a data-source row — this feed has no `customer_perspective` column),
# MAGIC derive `event_ts` and the domain language (SME-approved 2026-07-29 — NOT eVar8), normalize
# MAGIC `post_event_list` to bare ids, pseudonymize identity fields (ADR-0007), and run DQ
# MAGIC gates. Same `replaceWhere` window as bronze.

# COMMAND ----------
import common
common.setup_paths(dbutils)

from pyspark.sql import functions as F
from conf.coverme_settings import (
    resolve, PARTITION_COL, ELIGIBLE_HIT_SOURCE_EXCLUDE,
    HMAC_SECRET_SCOPE, HMAC_SECRET_KEY, IDENTITY_COLS, TIMEZONE,
)
from conf.coverme_bronze_columns import SILVER_COLUMNS
import silver_lib as sl
import cm_silver_lib as cml

s = resolve(dbutils)
if not common.gate(dbutils):
    dbutils.notebook.exit("guard: no new data")

# date_time carries America/Toronto wall-clock; pin the session so timestamp casts stay
# deterministic across cluster timezone settings.
spark.conf.set("spark.sql.session.timeZone", TIMEZONE)

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
pred = F.col(PARTITION_COL) >= F.lit(start).cast("date")
b = spark.table(s.bronze).where(pred).select(*SILVER_COLUMNS)

n_raw = b.count()
elig = b.where(cml.eligible_expr(ELIGIBLE_HIT_SOURCE_EXCLUDE))

conf = (elig.withColumn("event_ts", cml.event_ts_expr())
            .withColumn("language", cml.lang_from_host_expr(cml.url_expr()))
            .withColumn("post_event_list", sl.normalize_event_list_expr("post_event_list"))
            .drop("date_time", "hit_time_gmt", "exclude_hit", "hit_source",
                  "page_url", "visit_start_page_url", "first_hit_page_url", "post_page_url"))
for c in IDENTITY_COLS:
    conf = conf.withColumn(c, sl.pseudonymize_expr(c, hmac_key))
conf = conf.cache()

# COMMAND ----------
# DQ gates.
n = conf.count()
if n == 0:
    raise ValueError(f"silver DQ: 0 rows in window >= {start}")
elig_share = n / max(n_raw, 1)
null_ev = conf.where(F.col("post_event_list").isNull()).count()
ev_nonnull_frac = 1 - null_ev / n
print(f"DQ: rows={n}/{n_raw} eligible_share={elig_share:.4f} "
      f"event_list_nonnull={ev_nonnull_frac:.4f}")

# CoverMe baseline: post_event_list 93.2% non-null (GWAM's 0.95 gate would false-fail);
# eligible share ~94.33% (exclude_hit 0 -> 94.33%, 14 -> 4.9%, 4 -> 0.78%).
if ev_nonnull_frac < 0.90:
    raise ValueError(f"silver DQ: post_event_list non-null frac {ev_nonnull_frac:.3f} < 0.90")
if not (0.90 <= elig_share <= 0.98):
    print(f"WARNING: eligible-hit share {elig_share:.3f} outside [0.90, 0.98] -- the "
          "exclude_hit/hit_source mix moved vs the EDA baseline. Investigate before "
          "trusting this window's KPIs.")

# COMMAND ----------
writer = conf.write.format("delta").partitionBy(PARTITION_COL)
if spark.catalog.tableExists(s.silver):
    (writer.mode("overwrite")
           .option("replaceWhere", f"{PARTITION_COL} >= '{start}'")
           .saveAsTable(s.silver))
else:
    writer.mode("overwrite").saveAsTable(s.silver)
print(f"silver {s.silver}: {n} rows written for window >= {start}")
