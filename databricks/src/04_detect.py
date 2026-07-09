# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — Task 4/4: Anomaly detection
# MAGIC Reuses the locally-validated `detect/run.run_detection` **unchanged** (plan D6): exports
# MAGIC a slim hit-level parquet from silver to a UC Volume, scores it on the driver (univariate
# MAGIC + level-shift + pyod ECOD multivariate + operational rules), then MERGEs the findings
# MAGIC into `gold.anomalies` (17-col schema, preserving analyst `status`/`reconciled`) and
# MAGIC appends `gold.run_meta`. A parity guard (D7) asserts the pandas KPIs the detector scored
# MAGIC equal the gold table.

# COMMAND ----------
import json
import os
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import common
repo_root = common.setup_paths(dbutils)   # adds repo_root/detect for run/registry/kpis

# Shim the Windows-only OpenMP guard so `import run` does not pull numba/xgboost (absent on a
# stock cluster); keep only the reproducibility thread pin (plan D10).
os.environ.setdefault("OMP_NUM_THREADS", "1")
sys.modules.setdefault("_bootstrap", types.ModuleType("_bootstrap"))

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

from conf.settings import (
    resolve, DETECT_METHOD, DETECT_SEED, DOMAIN, GOLD_SCHEMA, SCRATCH_VOLUME,
)

s = resolve(dbutils)
if not common.gate(dbutils):
    dbutils.notebook.exit("guard: no new data")

# COMMAND ----------
# Detector input: slim hit-level parquet (the 12 columns run_detection needs = kpis
# NEEDED_COLS + rules RULE_DIMS), single file on a UC Volume.
from kpis import NEEDED_COLS
from registry import RULE_DIMS
export_cols = list(dict.fromkeys(NEEDED_COLS + RULE_DIMS))

spark.sql(f"CREATE VOLUME IF NOT EXISTS {s.catalog}.{GOLD_SCHEMA}.{SCRATCH_VOLUME}")
Path(s.scratch_dir).mkdir(parents=True, exist_ok=True)
slim_path = f"{s.scratch_dir}/hits.parquet"

local_pdf = spark.table(s.silver).select(*export_cols).toPandas()
local_pdf.to_parquet(slim_path, index=False)
print(f"exported {len(local_pdf)} rows x {len(export_cols)} cols -> {slim_path}")

# COMMAND ----------
from run import run_detection, COLS
kpis, anomalies, meta = run_detection(Path(slim_path), method=DETECT_METHOD, seed=DETECT_SEED)
print(f"{len(kpis)} days x {len(kpis.columns) - 1} series | {len(anomalies)} flagged metric-days")

# COMMAND ----------
# Parity guard (D7): the pandas KPIs the detector scored must equal the gold table.
gold_long = spark.table(s.gold_kpi).toPandas()
k = kpis.melt(id_vars="process_date", var_name="metric_id", value_name="value_pd")
k["process_date"] = pd.to_datetime(k["process_date"]).dt.date
gold_long["process_date"] = pd.to_datetime(gold_long["process_date"]).dt.date
merged = k.merge(gold_long, on=["process_date", "metric_id"], how="outer", indicator=True)
unmatched = int((merged["_merge"] != "both").sum())
maxdiff = float((merged["value_pd"] - merged["value"]).abs().max())
print(f"parity: unmatched={unmatched} max_abs_diff={maxdiff:.3e}")
assert unmatched == 0 and maxdiff <= 1e-6, \
    "gold vs pandas KPI parity failed -- investigate silver/gold drift"

# COMMAND ----------
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# anomalies -> gold.anomalies via MERGE on anomaly_id (analyst status/reconciled preserved).
if len(anomalies):
    a = anomalies.copy()
    a["date"] = pd.to_datetime(a["date"]).dt.date
    a["run_id"] = run_id

    num_cols = {"observed", "expected", "score", "threshold"}
    fields = []
    for c in COLS:
        if c == "date":
            fields.append(StructField("date", DateType()))
        elif c in num_cols:
            a[c] = pd.to_numeric(a[c], errors="coerce").astype(float)
            fields.append(StructField(c, DoubleType()))
        else:
            a[c] = a[c].map(lambda v: None if pd.isna(v) else str(v))
            fields.append(StructField(c, StringType()))
    fields.append(StructField("run_id", StringType()))
    schema = StructType(fields)

    sdf = spark.createDataFrame(a[list(COLS) + ["run_id"]], schema=schema)
    sdf.createOrReplaceTempView("_new_anoms")

    target = s.gold_anomalies
    if not spark.catalog.tableExists(target):
        spark.sql(f"CREATE TABLE {target} AS SELECT * FROM _new_anoms WHERE 1=0")

    updatable = [c for c in COLS if c not in ("anomaly_id", "status", "reconciled")] + ["run_id"]
    set_clause = ", ".join(f"t.`{c}` = s.`{c}`" for c in updatable)
    spark.sql(f"""
        MERGE INTO {target} t USING _new_anoms s ON t.anomaly_id = s.anomaly_id
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"merged {sdf.count()} anomalies into {target}")
else:
    print("no anomalies flagged this run")

# COMMAND ----------
# run_meta: append-only history keyed by run_id (FP denominators + per-detector counts).
meta_row = {
    "run_id": run_id,
    "domain": DOMAIN,
    "method": str(meta.get("method")),
    "seed": int(meta.get("seed")),
    "n_days": int(meta.get("n_days")),
    "date_min": str(kpis["process_date"].min())[:10],
    "date_max": str(kpis["process_date"].max())[:10],
    "n_anomalies": int(len(anomalies)),
    "counts_by_detector": json.dumps(meta.get("counts_by_detector", {})),
    "denominators": json.dumps(meta.get("denominators", {})),
}
(spark.createDataFrame([meta_row])
 .write.format("delta").mode("append").option("mergeSchema", "true")
 .saveAsTable(s.gold_runmeta))
print(f"run_meta appended: {run_id} ({meta_row['n_anomalies']} anomalies)")
