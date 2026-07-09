# GMAI-Pulse — Databricks pipeline (GWAM Canada Retirement)

Production medallion pipeline + scheduled anomaly detection for the GWAM Canada-Retirement
Adobe Analytics feed. Ports the locally-validated `detect/` prototype (5/5 injected scenarios
recalled, ≈2.3% business FP) onto Databricks without changing the detector code.

```
source (read-only)                bronze            silver              gold
gwam_prod_catalog                 scoped +          conformed +         registry KPI series
.inv_typed_common      ──▶        pruned    ──▶     pseudonymized ──▶   + anomalies + run_meta
.adobe_hit_data                   mirror            + DQ                       │
                                                                              ▼
                                                              detect/run_detection (pandas+pyod)
```

Job DAG (one Databricks Workflow): `freshness_guard → bronze → silver → gold → detect`,
scheduled daily **06:00 America/Toronto**, created **PAUSED**.

## Layout
| Path | Purpose |
|------|---------|
| `conf/settings.py` | Table names, scope filter, windowing, secret/volume names; `resolve(dbutils)` reads job params |
| `conf/bronze_columns.py` | Column policy: required set, 24 sensitive cols dropped, optional full-width `POPULATED_COLUMNS` |
| `src/common.py` | sys.path setup, watermark, schema contract, freshness gate |
| `src/silver_lib.py` | Pure transforms: event-list normalization, HMAC pseudonymization, `event_ts` |
| `src/gold_lib.py` | `build_kpis_spark` — PySpark port of `detect/kpis.build_kpis` (parity-tested) |
| `src/00_freshness_guard.py` … `04_detect.py` | The five job notebooks |
| `jobs/gmai_pulse_daily.json` | Databricks Jobs definition (cron, cluster, params) |

## Prerequisites
- A writable Unity Catalog you can create schemas in (`<catalog>.gmai_pulse_bronze|silver|gold`).
- `SELECT` on the source `gwam_prod_catalog.inv_typed_common.adobe_hit_data`.
- Databricks CLI v0.2+ configured (`databricks configure`), run from this Windows box or any host.
- DBR **16.4 LTS** (or ≥15.4 LTS) job cluster; only extra library is **`pyod==2.0.5`** (on the
  detect task). `darts`/`statsmodels` are **not** needed — the detector uses neither.

## Deploy

### 1. Get the repo into the workspace (so `detect/*.py` import as modules)
**Preferred — Git folder** (arbitrary `.py` are importable):
```bash
# push this repo to a remote, then in the workspace:
databricks repos create <git-url> gitHub --path /Repos/<you>/anomoly-detection
# or update an existing one:
databricks repos update /Repos/<you>/anomoly-detection --branch main
```
The notebooks auto-resolve `repo_root` from their own path, so leave the `repo_root` job
parameter blank when deployed under `/Repos/...`.

**Alternative — import just the notebooks** (then set `repo_root` explicitly): import the repo,
and because `workspace import-dir` turns `.py` into notebooks (not importable modules), you must
also sync `detect/`, `databricks/conf/`, `databricks/src/*.py` (the non-notebook libs) as
**workspace files** and pass `repo_root=/Workspace/<path-to-repo>` as a job parameter. The Git
folder route avoids this — prefer it.

### 2. Provision the identity HMAC secret (ADR-0007)
```bash
databricks secrets create-scope gmai_pulse
databricks secrets put-secret gmai_pulse identity_hmac_key   # paste a long random key
```
Silver fails fast if this is missing.

### 3. Create the job
```bash
# edit jobs/gmai_pulse_daily.json first: replace __SET_ME__ (repo owner in notebook paths),
# __SET_ME_NODE_TYPE__ (e.g. Standard_DS3_v2), and __ALERT_EMAIL__.
databricks jobs create --json @databricks/jobs/gmai_pulse_daily.json
```
Set the real catalog on the job (or per-run): `target_catalog=<your_catalog>`. To update an
existing job, use `databricks jobs reset --job-id <id> --json @databricks/jobs/gmai_pulse_daily.json`.

## Backfill (first load) and smoke test
Run once with `mode=backfill` to load all history (2026-02-01 → latest):
```bash
databricks jobs run-now --job-id <id> \
  --job-parameters target_catalog=<catalog>,mode=backfill,start_date=2026-02-01
```
Expected (per EDA):
- **bronze** ≈ 1,151,474 rows across 157 `process_date` partitions.
- **silver** same row count; DQ prints `event_list_nonnull≈1.0`; a `visid_pair_cardinality<=1`
  warning is expected (account-level ids per EDA) — not a failure.
- **gold** `kpi_daily` = 35 series × 157 days (5,495 long rows), no calendar gaps.
- **detect** parity guard prints `unmatched=0 max_abs_diff≈0`; `anomalies` + `run_meta` populated.
- Re-run the same window → identical bronze/gold counts (idempotent `replaceWhere`).

After ≥1 successful backfill, un-pause the schedule (UI, or set `pause_status: UNPAUSED` and
`jobs reset`). Daily runs use `mode=incremental` (default): the freshness guard no-ops when no
new `process_date` has landed; otherwise bronze reprocesses the trailing 3 days for late
micro-batches.

## Local verification (dev box)
The gold KPI build is unit-tested for exact parity with the pandas detector:
```bash
pip install pyspark==3.5.1            # dev-only; needs a JDK (11/17)
python -m pytest tests/test_gold_parity.py -q
```
On Windows set `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to your venv python and
`SPARK_LOCAL_IP=127.0.0.1` (the test fixture does this automatically). The existing
`tests/test_detect.py` continues to guard detector recall/FP (≤3%).

## Notes & out of scope (v1)
- **Widen bronze** for Phase-2 investigation by pasting the EDA populated-column census into
  `conf/bronze_columns.POPULATED_COLUMNS` (sensitive columns stay excluded).
- **Alert delivery** (anomalies → Databricks SQL Alert → webhook → Teams/email) is designed in
  `research/claude/02-solution-architecture.md` but not wired here — add a SQL Alert on
  `<catalog>.gmai_pulse_gold.anomalies` filtered to `severity in ('major','critical')`.
- Not included: DLT/Lakeflow expectations, hourly grain, CoverMe domain, MLflow registry,
  dev/prod split, Asset Bundles. The pseudonymization is a keyed SHA-256 (not RFC-2104 HMAC);
  swap `silver_lib.pseudonymize_expr` for a `hashlib.hmac` UDF if governance mandates strict HMAC.
