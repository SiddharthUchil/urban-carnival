# 16 — End-to-End Production Blueprint & Agent Guidance
## GMAI-Pulse · GWAM Canada-Retirement Anomaly Detection

**Status:** Authoritative guidance — written 2026-07-19 for execution by Claude Opus 4.8 (or any successor agent).
**Audience:** An implementation agent working in this repo. Read §0 before touching anything.
**Scope:** All phases — data exploration, EDA, eVars, Databricks medallion pipeline, jobs/orchestration, detection, AKS serving.

---

## 0. How to use this document (agent ground rules)

1. **This doc is the single entry point.** It links to the detailed docs; when this doc and an older doc conflict, this doc wins. Record any deliberate deviation as a new ADR in `research/claude/`.
2. **Standing decisions in §1 are settled.** Do not re-litigate them; implement them.
3. **Reading order for a fresh agent:** this doc → `research/claude/02-solution-architecture.md` → `research/claude/12-eda-findings-analysis.md` → `research/claude/14-manugrs-cross-suite-analysis.md` → `research/claude/15-consolidated-eda-report.md` → `databricks/README.md`.
4. **Repo conventions:** numbered docs in `research/claude/` (next free number: **17**); ADRs as `adr-XXXX-*.md`; every EDA notebook section emits a `SHAREABLE` block (§4.4); commits follow conventional-commit style visible in `git log`.
5. **Notebook handling gotchas (learned the hard way):**
   - The large `.ipynb` files can NOT be opened with Read/NotebookEdit (too big). Edit the paired `.py` export and re-splice, or use a scripted JSON splice on the `.ipynb`. When splicing cells by key, watch for **duplicate-key collisions** — always regenerate cell ids.
   - `eda/gwam_canada_retirement_eda_manugrs.ipynb` currently has **no `.py` export** — export it first (`jupyter nbconvert --to script`) before mining logic from it.
6. **Concurrent agents:** as of 2026-07-19 one agent is producing the definitive dual-rsid URL scope inventory, and another is retiring ADR-0007 masking controls. Do not duplicate or race that work — consume their outputs (§1 D3, D2).

---

## 1. Standing decisions (authoritative)

| ID | Decision | Detail |
|----|----------|--------|
| **D1** | **Exactly 2 EDA notebooks** | `eda/gwam_canada_retirement_eda.ipynb` (profiler) + `eda/gwam_canada_retirement_charts.ipynb` (interactive charts). Both cover **both rsids** (`manugrs` + `manulifeglobalprod`) and the full URL scope. All other EDA notebooks are retired/archived (§4.6). |
| **D2** | **No masking — full data visibility** | All masking/redaction is removed: `mask()`, `RAW_OK_DIMS` gating, sha1 self-redaction, identity suppression. Analysts and agents see data as-is, at all times, to get a complete picture. ADR-0007 is being retired by a separate agent; do **not** re-introduce masking helpers. Exports that leave the company still follow corporate data-handling policy. |
| **D3** | **URL scope is data-driven and widget-editable** | The definitive URL scope list comes from the dual-rsid full-history scope inventory run (in progress by another agent). Its result seeds the `url_scope_list` widget default; from then on scope changes are made **only via widgets**, never by editing code. |
| **D4** | **Scope/URL column rule** | Row filtering uses `post_page_url`; host/path breakdowns use `coalesce(page_url, post_page_url)` because `post_page_url` is ~37% blank ([gwam_canada_retirement_eda.py:665,722,759](../../eda/gwam_canada_retirement_eda.py)). This rule lives in ONE shared helper (§4.5), used everywhere. |
| **D5** | **One source of scope truth** | The EDA widget contract (§4.2) and the Databricks pipeline config (`databricks/conf/settings.py`) must resolve the same scope definition. Pipeline jobs take scope as job parameters with `settings.py` defaults (§5.2) — no more silent divergence between notebook widgets and pipeline constants. |
| **D6** | **AKS = model serving/scoring** | AKS hosts the anomaly-detection scoring service downstream of the Databricks gold layer (concrete phase, §7), aligned with `research/claude/13-global-serving-topology.md` and `adr-0008`. |
| **D7** | **Dual-rsid is permanent** | Legacy suite `manugrs` (traffic 2024-01 → cutover 2026-02-01) and current suite `manulifeglobalprod` are BOTH in scope for all history-aware analysis. rsid selection is a widget, defaulting to both. |

---

## 2. Project context

**Goal:** production-grade anomaly detection over Adobe Analytics clickstream for Manulife GWAM **Canada Retirement** (group-retirement web properties), surfacing volume/behavior anomalies (level shifts, drops, spikes) with investigation support.

**Data reality:**
- Two report suites: `manugrs` (legacy, active 2024-01 through cutover **2026-02-01**) and `manulifeglobalprod` (current). manugrs carries ~57% of total retirement traffic in the overlap analysis window; eVar overlap between suites is partial (12 shared / 50 current-only / 8 legacy-only — see doc-14).
- ~1,198 columns in the raw census; ~96 live eVars in manugrs.
- Event decoding is incomplete: custom events (10000+ range) resolve to "unknown" in `ADOBE_STD_EVENTS`; the `event.tsv` lookup (`new_data/event.tsv`) was never wired into the notebooks — see §8 backlog.
- French-language traffic is material (manugrs ~40.5% vs ~30.1% current suite) — URL scope must not silently drop `/fr/` paths.
- ⚠️ Earlier profiling assumed `eVar166`/`eVar169` are URL-type fields; the EDDL spec **contradicts this** (§3.4). Reconcile before building eVar-based rules.

**Repo layout (what exists today):**

| Path | Purpose |
|------|---------|
| `eda/` | Profiling + charts notebooks and `.py` exports; stakeholder README; consolidated HTML report |
| `databricks/` | Production medallion pipeline: `conf/settings.py`, `src/00_freshness_guard → 01_bronze_ingest → 02_silver_conform → 03_gold_kpis → 04_detect`, `jobs/gmai_pulse_daily.json` |
| `detect/` | Local detection library: `rules.py`, `univariate.py`, `multivariate.py`, `kpis.py`, `registry.py`, `evaluate.py`, `run.py` |
| `research/claude/` | Docs 01–16 + ADR-0001…0008 + `metric-registry.yaml` |
| `data/` | `EDDL_datalayer.xlsx`, synth clean/injected parquet + `known_events.json`, detect outputs |
| `new_data/` | `data_profile_summary.json`, profiling reports, `event.tsv` |
| `forresearchpurposes/` | Adobe data-dictionary / field-profile references |

---

## 3. eVar dictionary — `data/EDDL_datalayer.xlsx` (all 25 tabs)

**Authoritative tabs:** `Global Data Layer_GPMSS_IT` (91-row master spec) + the per-entity tabs (Page, User, Links, Transaction, Login, Registration, Internal Search, Form, Product, Video, Error, Tracking code, Download, Onetrust, Google ID) — these agree with each other. The `DKPIs` tab uses a **different/legacy numbering** and is treated as legacy (§3.5). Two Canada-Retirement-specific tabs: `EDDL ready for CAR` and `EDDL for CAR_WIP` (CAR = Canada Retirement). Narrative/template tabs (Tag Requirements, Measurement Plans, Core Tagging Script) contain no variable mappings.

### 3.1 eVar map (spec'd — number → meaning → source)

| eVar | Meaning | Source / notes |
|------|---------|----------------|
| eVar101 | Page Name | `web.webPageDetails.name` |
| eVar102 | Page Type | Non-Txn / Txn |
| eVar103 | Site Type | PWS / CWS / Sign-in hub |
| eVar104 | Content Type | Product / Campaign / Article |
| eVar105 | Brand \| Line of Business \| Segment | delimited multi-value; Segment ∈ CA / JH / GWAM / Asia |
| eVar106 | Country \| Region \| City | delimited multi-value |
| **eVar107** | **Full Page URL \| Domain \| Hash \| Query \| Path** | delimited, from `document.url`; paired with prop52 — **primary page-URL field** |
| eVar108 | User Agent | `navigator.userAgent` |
| eVar109 | Language | paired prop54 |
| eVar110 | Platform (MPS/SLX/OA/GA) | ⚠️ conflicts with eVar185 |
| eVar121 | Domain | Page tab only; ⚠️ overlaps eVar107 |
| eVar122 | Login Step | ⚠️ DKPIs says Error Description |
| eVar126 / **eVar127** | Download file label / **Download URL** | Download tab |
| eVar131–134 | Anonymous ID (ECID) / Primary / Secondary / Tertiary-Member Customer IDs | User/Global |
| eVar135 / eVar136 | Login Method (Biometric/MFA/Standard) / Email (hashed) | User |
| eVar137 | Age\|Gender\|Spouse Age\|Spouse Gender\|#Dependents\|Smoking | delimited multi-value |
| eVar138–145 | User Type / Sub-Type / Medallia UUID / Policy ID(s) / Transactions / Asset Tier / Nav History / New-Repeat (prop57) | User |
| eVar151–156, eVar168 | Txn Name / Type / ID / Step / Value / Option / Category | Transaction/Global |
| eVar161–163 | Search Results / Keywords (prop71) / Type (prop72) | Internal Search |
| eVar164–167 | Product Name / Qty / **Product ID (eVar166)** / Category | Product/Transaction |
| eVar171 / eVar172 / eVar174 | Product Name (productListItems) / Purchase ID / Product Category | Global/Product; dup of 164/167 |
| eVar181–184 | Error Code / Description / Type / Category | Error |
| eVar185 | Platform | Page tab; ⚠️ conflicts with eVar110 |
| eVar189 | Link Region | Links |
| eVar191 / eVar192 | Form Name / Form Step | Form |
| eVar193 / **eVar194** | Link Click Name (text) / **Click Href (link URL)** | Links |
| eVar195 | Video Name | Video |
| eVar196 | Registration Step | Registration |
| eVar199 / eVar200 | Google ID (GLID) / Onetrust Categories-ID | Google ID / Onetrust |
| eVar0 | Campaign ID | Tracking code tab, placeholder numbering |

### 3.2 Events (spec'd)

| Event | Meaning | Event | Meaning |
|-------|---------|-------|---------|
| event141 | Login Step | event161/162/163/180 | Form Start / Complete / Error / Step |
| event146 | Download click | event164 | Internal Search attempt (⚠️ Form tab reuses as Form Step) |
| event151/152/153/165 | Txn Start / Complete / Error / Step | event166 | Product Views (prodView) |
| event154/155/156 | Login Start / Complete / Error (⚠️ DKPIs remaps) | event167 | Purchase ID (⚠️ Global says event165) |
| event157/158/159/181 | Reg Start / Complete / Error / Step | event168–172, event174 | Video Start / Complete / 25% / 50% / 75% / Error |
| event160 | Link Click | event173 | Error event |

### 3.3 Props

prop51 Page Title · prop52 Page-URL parts (pairs eVar107) · prop53 Bot Detector (`navigator.webdriver`) · prop54 Language · prop55 Previous Page/Referrer · prop56 Page Nav Position · prop57 New/Repeat Visitor · prop71 Search Keywords · prop72 Search Type.

### 3.4 URL fields — correction to prior project assumption

Per this spec the URL-bearing variables are **eVar107 (+prop52)** full page URL parts, **eVar194** click href, **eVar127** download URL, **eVar121** domain. **eVar166 = Product ID (not a URL) and eVar169 does not exist** (event169 = Video Complete). Earlier production profiling tagged eVar166/169 as URL-type in `manugrs` — so either the deployed suites use a different variable map than this planning workbook, or the profiling mislabeled. **Backlog item #4 (§8): reconcile spec vs S7 live-eVar census per rsid before any eVar-based detection rule ships.** Note the workbook contains **no rsid names at all** (DKPIs' "Report Suite Mapping" column is blank) — it cannot arbitrate which suite implements which map.

### 3.5 Canada-Retirement applicability & conflicts

- `EDDL ready for CAR`: **CAR-applicable = Yes** → Page views, Errors, Transactions, User IDs, Business (LOB/Segment), Referrers, Timestamps, Technology, Downloads, Exits, Product (CWS), Policy IDs. **CAR-applicable = No** → Searches, Registrations, Logins, Campaign, Link tracking, Key-UI interaction. Use this to prioritize which eVars/events feed detection KPIs.
- Site scope model: **PWS** (public/prospect) vs **CWS** (customer — Plan Members / Plan Sponsors = group retirement). eVar105 Segment: Canada = CA, GWAM explicit.
- Known spec conflicts (flag, don't guess): Platform eVar110↔185 · Domain eVar107↔121 · eVar122 dual meaning · event164 dual meaning · Purchase-ID event165↔167 · DKPIs legacy numbering (eVar122–124, 150–154; events 151–156 remapped) · Product Name duplicated (eVar164 vs eVar171) · mapping-cell typos ("eVar 194", "event 160") — normalize whitespace when parsing.

---

## 4. EDA standard — the two-notebook contract

### 4.1 End state (D1)

```
eda/
├── gwam_canada_retirement_eda.ipynb     ← unified profiler, BOTH rsids, widget-driven
├── gwam_canada_retirement_eda.py        ← paired export (source of truth for review/diff)
├── gwam_canada_retirement_charts.ipynb  ← unified interactive charts, BOTH rsids, widget-driven
├── gwam_canada_retirement_charts.py     ← paired export
└── archive/                             ← retired notebooks (manugrs variant, url_scope_inventory)
```

Exactly **two** runnable EDA notebooks. Every future analysis need is met by adding a widget value or a section — never a new notebook.

### 4.2 Widget contract (both notebooks share it verbatim)

Config is 100% `dbutils.widgets` — already true in the `.py` exports (66 widget refs); the contract below extends it to dual-rsid and editable scope lists.

| Widget | Type | Default | Notes |
|--------|------|---------|-------|
| `rsid_list` | multiselect | `manugrs,manulifeglobalprod` | Replaces single-value `rsid_filter` text widget ([gwam_canada_retirement_eda.py:50-72](../../eda/gwam_canada_retirement_eda.py)). All sections loop/union over selected rsids and label outputs by rsid. |
| `url_scope_list` | text (newline/comma-separated LIKE patterns) | seeded from scope-inventory run (D3) | Add/remove patterns here — never in code. Blank entry = no URL filter. |
| `url_scope_mode` | dropdown: `en_only` / `broad` / `custom` | `broad` | `custom` uses `url_scope_list` verbatim; `en_only`/`broad` apply the named pattern sets (kept in the shared helper, mirroring `settings.py`). |
| `start_date` / `end_date` | text (YYYY-MM-DD) | full history → today | Feeds `resolve_date_expr()`. |
| `window_months` | text int | `13` | Rolling-window sections. |
| `granularity` | dropdown: `daily`/`weekly` | `daily` | Time-series sections. |
| `geo_country` / `geo_region` | dropdown / text | `All` | Carried over from charts/inventory notebooks so the profiler gains geo slicing. |
| `timezone` | text | `America/Toronto` | |
| `top_n`, `sample_rows` | text int | current defaults | |

**Removed:** any masking/redaction widget or helper (D2).

### 4.3 Section map (unified profiler)

Keep the existing S0–S12 spine of `gwam_canada_retirement_eda.py`, now rsid-looped:
S0 config · S1 UC-discovery · S2 delta metadata · S3 daily volume · S4 analysis window · **S4b URL-scope audit** · **S4c URL-column audit** · S5 census · S6 event decode · S7 live eVars/props · S8 time-series profiles · S9 dimensions · S10 data quality · S11 identity evidence (now unmasked, per D2) · S12 synthesis.

S4b/S4c absorb everything `gwam_url_scope_inventory.py` did (scope coverage, addable-rows audit, per-column URL profiling) — that notebook then retires.

### 4.4 SHAREABLE emit + manifest protocol (keep, unredacted)

Every section emits `===== BEGIN SHAREABLE: <id> =====` JSON via `emit()` ([gwam_canada_retirement_eda.py:142](../../eda/gwam_canada_retirement_eda.py)); the run ends with a `run_manifest` of per-section byte counts + sha1s. **Keep this protocol** — it is how runs are verified reproducible (16/16 manifest check). Under D2 the manifest's sha1 **self-redaction is removed**: hashes and payloads are emitted in full.

### 4.5 Shared helper module — `eda/_gwam_common.py`

Extract the helpers currently duplicated across `gwam_canada_retirement_eda.py`, `gwam_canada_retirement_charts.py`, `gwam_url_scope_inventory.py`:
- `resolve_date_expr(df)` (fallback chain — [gwam_canada_retirement_eda.py:216](../../eda/gwam_canada_retirement_eda.py))
- `_resolve_scope_cols` / `scope_condition` (rsid + URL filter — lines 240–244), now accepting a **list** of rsids and a **list** of URL patterns
- the D4 coalesce rule
- `emit()` + manifest builder
- widget registration for the §4.2 contract (one function both notebooks call)

Notebooks import it via the existing `_bootstrap`-shim pattern used by `databricks/` (repo-root `sys.path` insert), so the module works in both Databricks Repos and local runs.

### 4.6 Consolidation work package (for Opus 4.8)

1. `jupyter nbconvert --to script eda/gwam_canada_retirement_eda_manugrs.ipynb` → diff against `gwam_canada_retirement_eda.py`; port any manugrs-specific logic (legacy event ids, date coverage 2024-01→2026-02) into the rsid-conditional paths of the unified notebook.
2. Create `eda/_gwam_common.py` (§4.5); refactor both `.py` exports to use it.
3. Replace `rsid_filter` text widget with `rsid_list` multiselect; wrap scope resolution and section outputs in an rsid loop (union with an `rsid` label column where cross-suite comparison is wanted — reuse doc-14's comparison framings).
4. Remove all masking: `mask()`, `RAW_OK_DIMS`, redaction gating, manifest sha1 self-redaction (D2).
5. Seed `url_scope_list` default from the scope-inventory agent's output when it lands (D3).
6. Apply the same widget contract to the charts notebook.
7. Rebuild both `.ipynb` from the `.py` exports (scripted splice; regenerate cell ids), move `gwam_canada_retirement_eda_manugrs.ipynb` and `gwam_url_scope_inventory.*` to `eda/archive/`.
8. **Verify:** both notebooks run end-to-end on Databricks with defaults (both rsids); manifest completeness check passes; outputs contain rows for BOTH rsids; `grep -ri "mask(\|RAW_OK_DIMS" eda/` returns nothing; exactly 2 runnable notebooks remain in `eda/`.

---

## 5. Databricks pipeline phase

### 5.1 Current state

Medallion pipeline `databricks/src/`: `00_freshness_guard` → `01_bronze_ingest` → `02_silver_conform` → `03_gold_kpis` → `04_detect`, with `common/`, `silver_lib/`, `gold_lib/`. Config is **hardcoded** in [databricks/conf/settings.py](../../databricks/conf/settings.py): `SCOPE_RSID` (line 17), `SCOPE_URL_MODE` (24), `SCOPE_URL_LIKE` (27), `SCOPE_URL_LIKE_BROAD` (33), `_EXCLUDE` (41), `SCOPE_SUITE_MODE="current_only"` (52), `LEGACY_SCOPE_RSID="manugrs"` (58), `LEGACY_SCOPE_URL_LIKE` (59). Only `target_catalog` resolves from job params today.

### 5.2 Required changes (D5, D7)

1. Extend `resolve(dbutils)` so **rsid list, URL mode, URL pattern list, suite mode, and date range** all resolve from job parameters / widgets first, falling back to `settings.py` defaults. Same names as the §4.2 widget contract.
2. Default `SCOPE_SUITE_MODE` → `both` (D7): bronze ingests both rsids, silver conforms them onto one schema with an `rsid` column, gold KPIs are computed per-rsid **and** combined.
3. Reuse the pipeline's existing `event_list` normalization; do not re-implement it in notebooks.
4. **Partition-predicate gotcha:** `process_date` filters must be **dtype-aware** to get Delta partition pruning — build the predicate to match the column's actual type (date vs string) instead of relying on implicit casts, otherwise full scans.
5. Best practices to enforce: idempotent MERGE writes keyed on natural keys; freshness guard stays as job gate; explicit schemas at ingest (no inference in prod); table properties documented; unit-test `silver_lib`/`gold_lib` transforms against `data/synth/` fixtures.

---

## 6. Detection & jobs phase

### 6.1 Detection

- `detect/` library, driven by `research/claude/metric-registry.yaml` and `03-phase1-anomaly-detection.md`. Evaluate against `data/synth/` clean + injected sets with `known_events.json` ground truth (`detect/evaluate.py`).
- **Calibration fact:** the univariate DOW-median baseline cannot reliably detect level shifts below ~×1.9 at acceptable FP rates — do not chase ×1.35 sensitivity with this detector; that requires the multivariate/adaptive tier (doc-07).
- Every detector change re-runs the synth evaluation; record precision/recall deltas in the PR description.

### 6.2 Jobs & orchestration

- Today: single job [databricks/jobs/gmai_pulse_daily.json](../../databricks/jobs/gmai_pulse_daily.json) (`gmai_pulse_gwam_ca_ret_daily`, cron `0 0 6 * * ?` America/Toronto, DBR 16.4.x, currently **PAUSED**). No asset bundle.
- **Adopt Databricks Asset Bundles** (`databricks.yml`): dev/prod targets, per-target catalog, job definitions in source control, `databricks bundle deploy` as the only deployment path.
- **Un-pause criteria:** EDA consolidation done (§4.6 verify), pipeline runs green end-to-end on both rsids for 7 consecutive manual runs, detection outputs land in gold, alert routing tested.
- Add failure notifications (email/Teams webhook) on the job; alerting on freshness-guard trips.

---

## 7. AKS serving/scoring phase (D6)

Concrete architecture (aligned with doc-13 / ADR-0008 — React/TS AI-BI frontend on AKS + Azure AI Foundry):

```
Databricks gold (KPIs, anomalies) ──> Scoring/Serving API (FastAPI container, AKS)
                                          │  reads via Databricks SQL warehouse / delta-sharing
                                          ├──> React AI/BI frontend (AKS, ADR-0008)
                                          └──> Azure AI Foundry agent (investigation copilot)
```

Build order & best practices:
1. **Container:** FastAPI service exposing `/anomalies`, `/kpis`, `/health`; pinned base image; non-root; image in ACR; tags immutable (git sha).
2. **AKS:** Helm chart in-repo; readiness/liveness probes; resource requests+limits; HPA on CPU/RPS; 2+ replicas across zones.
3. **Auth/secrets:** Azure AD workload identity (managed identity) to reach Databricks SQL; secrets via Key Vault CSI driver — nothing in env-var manifests or images.
4. **Batch scoring:** if scheduled scoring moves off Databricks, run it as an AKS `CronJob` using the same image — one codepath for batch and API.
5. **Observability:** structured JSON logs, request metrics to Azure Monitor/Prometheus, alert on p95 latency + non-200 rate.
6. **Rollout:** dev namespace → smoke tests against dev catalog → prod via Helm upgrade with `--atomic`.

---

## 8. Cross-cutting practices & backlog

**Practices**
- Git: feature branches, conventional commits, PR review before `main`; never commit secrets (`.env*` ignored).
- Reproducibility: any figure quoted in a doc must trace to a SHAREABLE block id + run date.
- Data quality gates: S10 checks promoted into `02_silver_conform` as hard/soft expectations; hard failures stop the job.
- Docs: new findings → next numbered doc; decisions → ADR; this blueprint updated when a standing decision changes.

**Backlog (known gaps, in priority order)**
1. Consume URL-scope inventory agent output → seed `url_scope_list` defaults (D3).
2. Wire `new_data/event.tsv` into S6 event decode (the doc-14 provenance caveat references a fix that was never implemented) so custom events (10004–10048+, legacy ids) stop resolving as "unknown".
3. `uc_discovery` scope conditions still reference `post_page_url` only — align with D4 helper.
4. **Reconcile §3 EDDL spec vs S7 live-eVar census per rsid** — includes resolving the eVar166/eVar169 URL-type contradiction (§3.4). Blocks eVar-based detection rules.
5. Asset bundle adoption (§6.2).

---

## 9. Phase plan (end-to-end, with verification)

| # | Phase | Deliverable | Verify |
|---|-------|-------------|--------|
| P0 | Scope freeze | URL scope list from inventory agent; masking removal landed | scope list checked into widget defaults; no `mask(`/`RAW_OK_DIMS` in `eda/` |
| P1 | eVar dictionary | §3 cross-checked vs S7 census per rsid | every live eVar has a name or an explicit "unmapped" flag; eVar166/169 contradiction resolved |
| P2 | EDA consolidation | Exactly 2 widget-driven dual-rsid notebooks (§4.6) | §4.6 step 8 checklist |
| P3 | Pipeline unification | `resolve()` param-driven scope; `SCOPE_SUITE_MODE=both`; dtype-aware `process_date` predicates | end-to-end run green on both rsids; partition pruning confirmed in query plan |
| P4 | Detection tuning | Detectors calibrated on both-rsid gold; synth eval refreshed | precision/recall reported vs `known_events.json`; ≥×1.9 level-shift floor documented in results |
| P5 | Job productionization | Asset bundle; job un-paused per §6.2 criteria | 7 green scheduled runs; alerts fire on induced failure |
| P6 | AKS serving | Scoring API live in dev namespace, then prod (§7) | `/health` green; frontend reads anomalies end-to-end |
| P7 | Frontend + Foundry | ADR-0008 React AI/BI + Foundry investigation copilot | stakeholder walkthrough using live data |

---

*Doc 16 · created 2026-07-19 · supersedes scattered guidance in docs 02/12/14/15 where they conflict.*
