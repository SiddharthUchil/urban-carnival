# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — Task 0/4: Freshness guard
# MAGIC Compares the source's latest partition to what bronze already holds and publishes a
# MAGIC `new_data` task value. Downstream tasks early-exit when it is `false`, so the linear
# MAGIC job DAG needs no separate condition task. `mode=backfill` always proceeds.

# COMMAND ----------
import common
repo_root = common.setup_paths(dbutils)
from conf.settings import resolve, SOURCE_TABLE, PARTITION_COL

s = resolve(dbutils)
print("config:", s, "| repo_root:", repo_root)

# COMMAND ----------
src_wm = common.read_watermark(spark, SOURCE_TABLE, PARTITION_COL)   # latest source partition
bronze_wm = common.read_watermark(spark, s.bronze, PARTITION_COL)    # latest we have ingested
print(f"source watermark: {src_wm} | bronze watermark: {bronze_wm}")

if s.mode == "backfill":
    new_data = True
elif bronze_wm is None:
    new_data = True            # first ever run -> full ingest
elif src_wm is None:
    new_data = False           # source empty / unreadable -> nothing to do
else:
    new_data = src_wm > bronze_wm   # 'YYYY-MM-DD' lexicographic compare == date compare

common.set_task_value(dbutils, "new_data", "true" if new_data else "false")
common.set_task_value(dbutils, "src_watermark", src_wm or "")
common.set_task_value(dbutils, "bronze_watermark", bronze_wm or "")
print(f"new_data={new_data} (mode={s.mode})")

# COMMAND ----------
if not new_data:
    dbutils.notebook.exit("no new data -- skipping downstream tasks")
print("new data present -- proceeding to bronze ingest")
