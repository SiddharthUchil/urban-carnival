# GMAI-Pulse — Databricks build runbook

Sequential instructions for standing up the medallion layers in `usdo_aa_catalog`.
**CoverMe first, then GWAM.** Bronze → silver → gold only; anomaly detection is out of scope.

`README.md` in this folder is the reference doc (what each thing *is*). This file is the
running order (what to *do*, in sequence). Where they disagree, this file is newer.

Environment this was written against:

| | |
|---|---|
| Catalog | `usdo_aa_catalog` |
| Schemas | `gmai_pulse_bronze` / `gmai_pulse_silver` / `gmai_pulse_gold` |
| Compute | **Global Marketing AI AA Team Compute** (DBR 18 LTS, Spark 4.1.0) |
| SQL warehouse | **`gmai-aab-pro-sql-warehouse`** |
| Repo | `github.com/JHDevOps/GMAI-PULSE-DATABRICKS` |

---

## Two things to know before you click anything

**1. The first notebook run always fails, and that is correct.** Widgets do not exist until
the code creates them. `resolve()` calls `dbutils.widgets.text("target_catalog", "__SET_ME__")`
and then immediately raises:

```
ValueError: target_catalog is unset. Set the 'target_catalog' job parameter (or notebook
widget) to a writable Unity Catalog...
```

That is the fail-fast guard doing its job (ADR-0006 — never write to a catalog nobody named).
The widgets are now visible at the top of the notebook. Fill them in and re-run. Every notebook
in this runbook behaves this way on its first execution.

**2. Interactive runs and job runs read `start_date` differently.** Under a job, silver
receives the window from bronze via task values. Interactively there are no task values, so
silver falls back to its **own `start_date` widget**. Set `start_date` to the *same value* on
bronze and silver, or silver will conform a different window than bronze just wrote.

---

## Step 1 — Get the code into the workspace

The libs (`gold_lib.py`, `settings.py`, `detect/*.py`) must import as real Python modules, which
is why this is a Git folder and not a notebook import.

1. Left sidebar → **Workspace**.
2. Navigate to where you want it (your user folder, or `Repos/`).
3. **Create → Git folder** (older workspaces: **Add → Repo**).
4. Git repository URL: `https://github.com/JHDevOps/GMAI-PULSE-DATABRICKS`
5. Git provider: **GitHub**. If it asks for credentials, you need a PAT with `repo` scope under
   **Settings → Linked accounts → Git integration**.
6. Create, then confirm the branch is the one you pushed.

**Note the full path** — you need it in Step 7. It will look like
`/Workspace/Users/<you>@manulife.com/GMAI-PULSE-DATABRICKS` or `/Repos/<you>/GMAI-PULSE-DATABRICKS`.
Either layout works: the notebooks derive their own repo root by splitting their path on
`/databricks/`.

## Step 2 — Prove the libs import (2 minutes, saves hours)

If this fails, nothing after it can work, and every later error will be a red herring.

1. **Workspace → Create → Notebook**, language Python.
2. Attach it to **Global Marketing AI AA Team Compute** (top-right cluster picker).
3. Run:

```python
import sys

# Your Step 1 path, e.g. "/Workspace/Repos/you/GMAI-PULSE-DATABRICKS"
REPO = "/Workspace/<your-git-folder-path>"

sys.path.insert(0, REPO + "/databricks/src")        # bootstrap: find common.py
import common
root = common.setup_paths(dbutils, override=REPO)   # override is required here -- see below
print("repo root:", root)

import gold_lib, silver_lib, cm_silver_lib          # the shared libs
from conf.coverme_settings import SOURCE_TABLE      # config as a package
from cm_registry import SERIES                      # detect/ on the path
print("gold_lib OK | source:", SOURCE_TABLE, "| CoverMe series:", len(SERIES))
```

**Expect:** `CoverMe series: 53` and the CoverMe source table name.

`override=REPO` is not optional in *this* notebook. `setup_paths` normally derives the repo root
by splitting the running notebook's own path on `/databricks/` — which works for the pipeline
notebooks because they live at `<repo>/databricks/src/…`, but a scratch notebook outside the repo
has no such path and silently falls back to `os.getcwd()` (`/databricks/driver`). That is the same
mechanism as the `repo_root` widget, so this also proves the widget works. The pipeline notebooks
in Steps 4–8 need no override — leave `repo_root` blank there.

**If `ModuleNotFoundError`:** `REPO` is wrong, or the repo was imported as notebooks rather than
as a Git folder (arbitrary `.py` files only import as modules from a Git folder or Workspace file).

## Step 3 — Preflight: access and namespace

Open **SQL Editor** (left sidebar) and select **`gmai-aab-pro-sql-warehouse`** in the warehouse
dropdown, top right.

**3a. Confirm you can read both sources.** These are two *separate* grants — CoverMe's is the
one that matters this week.

```sql
SELECT count(*) FROM csdo_prod_catalog.adobe_coverme_bronze.hit_data  WHERE hit_date     = current_date() - 3;
SELECT count(*) FROM gwam_prod_catalog.inv_typed_common.adobe_hit_data WHERE process_date = current_date() - 3;
```

A `PERMISSION_DENIED` here is a grant request, not a code problem. Stop and get it.

**3b. Create the schemas.** The bronze notebooks do this themselves, but running it now means a
permissions failure surfaces in two seconds instead of forty minutes into a backfill.

```sql
CREATE SCHEMA IF NOT EXISTS usdo_aa_catalog.gmai_pulse_bronze
  COMMENT 'GMAI-Pulse bronze — scoped, column-pruned mirrors of Adobe Analytics feeds';
CREATE SCHEMA IF NOT EXISTS usdo_aa_catalog.gmai_pulse_silver
  COMMENT 'GMAI-Pulse silver — conformed, KPI-ready hit tables';
CREATE SCHEMA IF NOT EXISTS usdo_aa_catalog.gmai_pulse_gold
  COMMENT 'GMAI-Pulse gold — daily KPI series from research/claude/metric-registry.yaml';

SHOW SCHEMAS IN usdo_aa_catalog LIKE 'gmai_pulse*';
```

**Expect:** three rows.

---

## Step 4 — CoverMe smoke run (one month)

**Do not skip this.** These notebooks have never executed on DBR 18 / Spark 4.1 — they were
written for DBR 16.4 / Spark 3.5. Finding an incompatibility is cheap on 22 days of data and
expensive on 1,211.

Open `databricks/src/cm_01_bronze_ingest` from the Git folder, attach to the team compute.

**4a.** Run the **first cell only** (`Shift+Enter`). It fails with the `target_catalog is unset`
`ValueError` — expected, see the note at the top. Widgets now appear.

**4b.** Set the widgets:

| Widget | Value |
|---|---|
| `target_catalog` | `usdo_aa_catalog` |
| `mode` | `backfill` |
| `start_date` | `2026-07-01` |
| `repo_root` | *(leave blank — auto-derived)* |
| `pseudonymize` | `false` |

**4c.** **Run all**.

**Expect:**
```
projecting <N> columns into bronze
ingest window: hit_date >= 2026-07-01 (mode=backfill, bronze_wm=None)
bronze usdo_aa_catalog.gmai_pulse_bronze.adobe_hit_coverme: <~800k> rows in window >= 2026-07-01
backfill sanity: coverme.com=<n> pourmeproteger.com=<n>
```
Both brand-domain counts must be non-zero — `mode=backfill` raises if either is zero, which is
the scope filter proving it still works.

**4d.** Open `cm_02_silver_conform`, same widgets and **the same `start_date`** (see note 2 at
the top — interactive silver uses its own widget, not bronze's). Run all.

**Expect:**
```
WARNING: pseudonymize=false -- ADR-0007 DEVIATION. These columns land in silver in the clear:
mcvisid, post_visid_high, post_visid_low. ...
DQ: rows=<n>/<n_raw> eligible_share=0.94xx event_list_nonnull=0.93xx
silver ...hits_conformed_coverme: <n> rows written for window >= 2026-07-01
```
`eligible_share` outside `[0.90, 0.98]` only prints a warning — note it, don't ignore it.

**4e.** Open `cm_03_gold_kpis`, same widgets, run all. Gold reads *all* of silver and fully
overwrites, so after the smoke gold holds July only. That is fine; Step 5 replaces it.

**Expect:** 53 series written, and the backfill sanity gate confirming all 5 funnel events fire
and both language domains are present.

> **If anything fails here, stop and fix it before Step 5.** The likely DBR 18 failure modes are
> ANSI-mode casts and Spark 4 API changes. The known ANSI hazards are already handled
> (`try_cast`, `try_element_at`), so a new one is genuinely new information — capture the full
> traceback.

## Step 5 — CoverMe full backfill

Same three notebooks, same order, **one widget changes**: `start_date` → `2023-02-28`.

Run `cm_01` → `cm_02` → `cm_03`, waiting for each to finish.

`replaceWhere hit_date >= '2023-02-28'` covers everything the smoke wrote, so the July rows are
replaced, not duplicated.

**Expect:**

| Layer | Table | Expected |
|---|---|---|
| bronze | `adobe_hit_coverme` | ≈ 57.7M rows, ~1,211 `hit_date` partitions |
| silver | `hits_conformed_coverme` | ≈ 94% of bronze, `event_list_nonnull` ≈ 0.93 |
| gold | `kpi_daily_coverme` | 53 series × days, long format |

This is the long one — a full scan of a 17.13 GB / 10,633-file source.

> ⚠ **Do not try to chunk this by year.** `replaceWhere hit_date >= start` has no upper bound, so
> a later run with a *newer* start date deletes everything above it. If it times out, scale the
> cluster or raise the timeout — do not split the window.

## Step 6 — Verify CoverMe

SQL Editor, warehouse `gmai-aab-pro-sql-warehouse`. Open
`databricks/sql/coverme_backfill_verify.sql`, and **replace the single `${target_catalog}` on
line 22** with `usdo_aa_catalog` (it appears once; the other mentions are comments). Run the
file and read down the `verdict` column.

- Every check should read `PASS`.
- **Investigate every `REVIEW`.** Check 4 in particular flags an eligible-hit share that drifted
  inside the warn band — the pipeline only *prints* that, so a run can succeed while the mix moved.
- **Record check 7b's output.** Those are the ~30 zero-filled feed-gap dates. Nothing in the
  pipeline knows about them, and a zero-filled gap is indistinguishable from a real zero-traffic
  day to anything reading gold. You need that list before anyone fits a baseline.

## Step 7 — Create the CoverMe job

This job has never existed in any workspace.

**7a.** Edit `databricks/jobs/coverme_pulse_daily.json` and replace four placeholders:

| Token | With | Where to get it |
|---|---|---|
| `__GIT_FOLDER_OWNER__` | your owner segment | From the Step 1 path — must match exactly |
| `__DBR_SPARK_VERSION__` | e.g. `18.4.x-scala2.13` | **Compute → your cluster → View JSON → copy `spark_version` verbatim.** Do not guess. |
| `__NODE_TYPE_ID__` | e.g. `Standard_DS3_v2` | Same View JSON, `node_type_id` |
| `__ALERT_EMAIL__` | your address | — |

If your Git folder is under `/Workspace/Users/...` rather than `/Repos/...`, fix the four
`notebook_path` values to match the real path.

**7b.** **Jobs & Pipelines → Create job**, then the **⋮ menu → Edit as JSON**. Paste the file. Save.

**7c.** **Run now** with `mode=incremental` (the default). The freshness guard should either
no-op ("no new data") or reprocess the trailing 5 days.

**7d.** Run it once more and confirm the row counts are unchanged — that is the idempotence check.

**7e.** Leave it **PAUSED** until you trust it. `pause_status` gates only the cron; `Run now`
works on a paused job. Un-pause from the job's Schedule panel when ready.

---

## Step 8 — GWAM backfill

Same shape, much smaller data (~1.15M rows). Notebooks `01_bronze_ingest` → `02_silver_conform`
→ `03_gold_kpis`. **Do not run `04_detect`** — out of scope, and it is the only task needing
`pyod`, which is unverified on DBR 18.

Widgets: `target_catalog=usdo_aa_catalog`, `mode=backfill`, `start_date=2026-02-01`,
`pseudonymize=false`.

**Expect** ≈1,151,474 bronze rows across ~157 `process_date` partitions, and **42 series** in
gold — not 35. 42 is the built count since registry v0.7.0; 35 is the *scored* count, and older
docs conflate them.

A `visid_pair_cardinality<=1` warning from silver is expected on this feed (account-level ids),
not a failure.

## Step 9 — Verify GWAM

Same as Step 6 with `databricks/sql/gwam_backfill_verify.sql` (`${target_catalog}` on line 26).
Nine checks. Three worth reading closely:

- **Check 1 settles an open question.** Docs disagree on whether `manulifeglobalprod` starts
  2026-02-01 or 2026-03-10 (doc-16 backlog #4). Whatever `first_day` reads *is* the answer —
  record it, and update `BACKFILL_START` if it is the later date.
- **Check 6** reports which privacy branch actually ran. It should say identity columns are RAW,
  because this build runs `pseudonymize=false`. That is the ADR-0007 deviation, made visible
  rather than assumed.
- **Check 9** asserts `page_views_total == hits_total`. That identity *is* the zero-re-baseline
  guarantee for SME question Q6. If it ever fails, `PAGE_VIEW_BASIS` moved and every page-view
  series needs a full backfill with gold truncated.

## Step 10 — Create the GWAM job

Same as Step 7 with `jobs/gmai_pulse_daily.json`.

> **The `detect` task is still in that JSON.** It is the real production DAG, so it was not
> deleted. For this build either remove that task before saving, or leave it and keep the
> schedule PAUSED until `pyod==2.0.5` is smoke-tested on DBR 18.

---

## When something fails

| Symptom | Cause | Fix |
|---|---|---|
| `ValueError: target_catalog is unset` | First run created the widgets | Expected. Fill them in, re-run |
| `ModuleNotFoundError: common` / `gold_lib` | Not a Git folder, or wrong path | Redo Step 1; verify with Step 2 |
| `RuntimeError: identity HMAC secret ... not found` | `pseudonymize` is not `false` | Set the widget to `false`, or provision the secret |
| `Source schema contract violation (ADR-0006)` | Upstream feed changed columns | Reconcile `conf/*_bronze_columns.py` — do not just drop the column |
| `PERMISSION_DENIED` on a source table | Missing `SELECT` grant | Step 3a. GWAM and CoverMe are separate grants |
| `bronze backfill sanity: zero rows for production host(s)` | Scope filter matched nothing | Real regression. Check the URL patterns before overriding |
| `silver DQ: post_event_list non-null frac < 0.90` | Feed quality moved | Do not lower the gate. Investigate the window |
| Silver row count ≠ bronze (GWAM only) | GWAM silver applies no eligibility filter | They must match. Something dropped rows |
| Backfill times out at 7200s | Full scan of a large source | Scale the cluster or raise the timeout. **Never split the window** |
