# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — Task 3/4: Gold KPI build
# MAGIC Registry-driven daily KPI series (plan D5). Full rebuild from silver each run — the
# MAGIC matrix is tiny (~157 days x 35 series), so a rebuild guarantees consistency with the
# MAGIC trailing-overlap reprocessing. Stored long: `process_date, metric_id, value`.
# MAGIC The build logic is a faithful PySpark port of `detect/kpis.build_kpis` (see gold_lib),
# MAGIC unit-tested for exact parity in `tests/test_gold_parity.py`.

# COMMAND ----------
import common
common.setup_paths(dbutils)   # adds repo_root/detect so `registry` imports

from conf.settings import resolve
import gold_lib
from registry import SERIES, EVENT_IDS

s = resolve(dbutils)
if not common.gate(dbutils):
    dbutils.notebook.exit("guard: no new data")

# COMMAND ----------
silver = spark.table(s.silver)                 # full history: gold is a full rebuild (D5)
wide = gold_lib.build_kpis_spark(silver, EVENT_IDS, SERIES)
long = gold_lib.melt_to_long(wide)

# COMMAND ----------
(long.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(s.gold_kpi))

n_days = wide.count()
n_series = len(wide.columns) - 1
print(f"gold {s.gold_kpi}: {n_days} days x {n_series} series = {long.count()} long rows")
