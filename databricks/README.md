# GMAI-Pulse — Databricks pipeline (GWAM Canada Retirement + CoverMe)

Production medallion pipeline + scheduled anomaly detection for the GWAM Canada-Retirement
Adobe Analytics feed. Ports the locally-validated `detect/` prototype (5/5 injected scenarios
recalled, ≈2.3% business FP) onto Databricks without changing the detector code.
A second domain, **CoverMe**, shares the libs with its own conf + notebooks + job
(bronze→gold only for now — see the CoverMe section below).

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
| `src/00_freshness_guard.py` … `04_detect.py` | The five GWAM job notebooks |
| `jobs/gmai_pulse_daily.json` | GWAM Databricks Jobs definition (cron, cluster, params) |
| `conf/coverme_settings.py` | CoverMe: source table, URL-only scope, eligibility, windowing; same widget contract |
| `conf/coverme_bronze_columns.py` | CoverMe column policy (no `rsid`/`customer_perspective` in this feed; flagged eVars mirrored) |
| `src/cm_silver_lib.py` | CoverMe pure transforms: page_url-first coalesce, URL scope, domain language, hit eligibility |
| `src/cm_00_freshness_guard.py` … `cm_03_gold_kpis.py` | The four CoverMe job notebooks (no detect task yet) |
| `jobs/coverme_pulse_daily.json` | CoverMe Databricks Jobs definition |
| `../detect/cm_registry.py` | CoverMe series registry, pinned to `research/claude/metric-registry.yaml` v0.3.0 |

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

## CoverMe pipeline (bronze → gold)
Second product domain, sharing `common.py`/`silver_lib.py`/`gold_lib.py`. DAG:
`freshness_guard → bronze → silver → gold` (4 tasks, **no detect yet** — the detector waits
for real gold baselines to calibrate thresholds). Job `gmai_pulse_coverme_daily`, same 06:00
America/Toronto cron, created **PAUSED**. Design spec:
`docs/superpowers/specs/2026-07-27-coverme-pipeline-port-design.md`.

Key differences vs GWAM (all EDA/SME-confirmed 2026-07-27):
- **URL-only scope** (single-suite feed, no `rsid` column): coverme.com + pourmeproteger.com,
  plus retired insttrip.manulife.com for baseline history only (dead since 2024-03-11), minus
  UAT/AEM noise. Matched on the blank-guarded **page_url-first** 4-way coalesce.
- **Hit eligibility at silver** (Analysis-Workspace parity per Adobe datafeeds-calculate):
  `exclude_hit = 0` and `hit_source not in (5,7,8,9)`; this feed has no `customer_perspective`
  column. Bronze keeps raw scoped rows. Baseline eligible share ≈ 94.33%.
- **Adobe metric keys**: visits = 4-part key (visid pair + visit_num + visit_start_time_gmt),
  visitors = visid pair (not mcvisid). **Event counts are visit-distinct** (SME ruling); the
  funnel is non-monotonic (saved-quote resume) — no within-visit sequencing anywhere.
- **Language is domain-derived** (en/fr/unknown from the URL host), not eVar8.
- Partition `hit_date`, `OVERLAP_DAYS=5` (late-arrival p99), backfill start **2023-02-28**.

Backfill (deployment gated on the PII/consent sign-off — readiness doc 17 item 9 — so the
schedule stays PAUSED until that clears):
```bash
databricks jobs run-now --job-id <id> \
  --job-parameters target_catalog=<catalog>,mode=backfill,start_date=2023-02-28
```
Expected (per the verified EDA manifest):
- **bronze** `adobe_hit_coverme` ≈ 57.7M rows across ~1,211 `hit_date` partitions (~30 known
  missing source days).
- **silver** `hits_conformed_coverme` ≈ 94% of bronze; DQ prints `event_list_nonnull≈0.93`
  (gate is 0.90 here, not GWAM's 0.95).
- **gold** `kpi_daily_coverme` = 53 series × days, no calendar gaps; backfill sanity gate
  asserts all 5 funnel events fire and both language domains are present.
- Gold funnel counts read **below** the EDA S6b totals (visit-distinct post-eligibility vs
  hit-presence pre-filter) — expected, not a bug. Compare **conversion ratios**, not raw totals.

## Local verification (dev box)
The gold KPI build is unit-tested for exact parity with the pandas detector, and the CoverMe
series kinds (visit-distinct counts, ratios, evar4 cube) have their own hand-computed suite:
```bash
pip install pyspark==3.5.1            # dev-only; needs a JDK (11/17)
python -m pytest tests/test_gold_parity.py tests/test_cm_gold.py tests/test_cm_silver.py -q
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
- Not included: DLT/Lakeflow expectations, hourly grain, the CoverMe **detect** task
  (bronze→gold shipped; thresholds wait for real baselines), MLflow registry,
  dev/prod split, Asset Bundles. The pseudonymization is a keyed SHA-256 (not RFC-2104 HMAC);
  swap `silver_lib.pseudonymize_expr` for a `hashlib.hmac` UDF if governance mandates strict HMAC.
