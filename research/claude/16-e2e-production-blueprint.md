# 16 — End-to-End Production Blueprint & Agent Guidance
## GMAI-Pulse · GWAM Canada-Retirement Anomaly Detection

**Status:** Authoritative guidance — written 2026-07-19 for execution by Claude Opus 4.8 (or any successor agent).
**Revised 2026-07-20** — URL scope inventory landed (D3 closed, D8 added, D4/D5 rewritten, cutover claim corrected, EDA consolidated to two notebooks).
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
   - Notebook *output* can self-truncate: a Databricks stdout cap silently dropped 22 of 200 rollup rows mid-payload in the inventory run, injecting `*** WARNING: max output size exceeded ***` between JSON fragments. When a section's payload may be large, lower its top-N or write Delta rather than trusting printed output.
   - `jupyter`/`nbformat` are **not installed** in `.venv`. To export a notebook, either install them or parse the `.ipynb` JSON with stdlib and emit the repo's Databricks format (`# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC %md`).
6. **Retired notebooks live in git history, not on disk.** `gwam_url_scope_inventory.*` and `gwam_canada_retirement_eda_manugrs.ipynb` were removed at `8ac2551`; their last content (including the full inventory run output) is at **`408de5a`**. A second sweep on 2026-07-20 removed `gwam_canada_retirement_eda_manugrs.py`, both paired `.ipynb` run records, and `eda/README.md` — their last content is the commit immediately before that sweep. Recover with `git show <sha>:<path>` — do not recreate them. The `.ipynb` files carried embedded run outputs (438KB / 289KB); that is the only place those particular run results existed.

---

## 1. Standing decisions (authoritative)

| ID | Decision | Detail |
|----|----------|--------|
| **D1** | **Exactly 2 EDA files** | `eda/gwam_canada_retirement_eda.py` (profiler) + `eda/gwam_canada_retirement_charts.py` (interactive charts) — **`.py` only, no paired `.ipynb`, no README**. Both cover **both rsids** (`manugrs` + `manulifeglobalprod`) and the full URL scope, via `rsid_list` / `url_scope_*` widgets. Run instructions live in each file's header cell. Everything else is retired to git history (§0.6). |
| **D2** | **No masking — full data visibility** | All masking/redaction is removed: `mask()`, `RAW_OK_DIMS` gating, sha1 self-redaction, identity suppression. Analysts and agents see data as-is, at all times, to get a complete picture. ADR-0007 is being retired by a separate agent; do **not** re-introduce masking helpers. Exports that leave the company still follow corporate data-handling policy. |
| **D3** | **URL scope is data-driven and widget-editable** ✅ **closed 2026-07-20** | The dual-rsid full-history inventory ran 2026-07-20 (output at `408de5a`). Seeded `url_scope_list` default = `%/group-retirement%`, `%/group-plans%`, `%/regimes-collectifs%` — already language-agnostic, so **no FR-specific patterns are needed**: it covers `manulifeim.com/group-retirement/ca/fr/*` and `/ca/fr/particuliers/regimes-collectifs/retraite-collective`. From here scope changes are made **only via widgets**, never by editing code. Key figures in §2. |
| **D4** | **Coalesce everywhere — `post_page_url` alone is not a scope column** | *Rewritten 2026-07-20; the previous D4 was incoherent — it avoided `post_page_url` for breakdowns because it is blank, then filtered rows on it.* Row filtering **and** breakdowns both use `coalesce(page_url, post_page_url)`. The inventory measured `post_page_url` blank at **36.41%** (manulifeglobalprod) / **45.75%** (manugrs) vs **≤0.013%** for `page_url`. One shared helper (§4.5), used everywhere. **Last tracked violation:** [01_bronze_ingest.py:69](../../databricks/src/01_bronze_ingest.py) still does `F.col("post_page_url").like(SCOPE_URL_LIKE)` in `en_only` mode. See §5.2 for the measurement gate before changing it. |
| **D5** | **One scope vocabulary; defaults may stage** | The EDA widget contract (§4.2) and `databricks/conf/settings.py` must share the same **mode names, pattern lists, and resolution logic** (include patterns OR-ed, minus `SCOPE_URL_LIKE_EXCLUDE`, minus D8 login hosts). Their *default values* may diverge while a rollout is staged, but only when the divergence and its exit criterion are stated here. **Current sanctioned divergence:** EDA defaults to `broad` (analysts should see everything); the pipeline stays `en_only` until the P2 re-profile lands and `%/group-plans%` gets product sign-off (§5.2). |
| **D6** | **AKS = model serving/scoring** | AKS hosts the anomaly-detection scoring service downstream of the Databricks gold layer (concrete phase, §7), aligned with `research/claude/13-global-serving-topology.md` and `adr-0008`. |
| **D7** | **Dual-rsid is permanent — and the suites are CONCURRENT** | Both `manugrs` and `manulifeglobalprod` are in scope for all analysis; rsid selection is a widget defaulting to both. ⚠️ *Corrected 2026-07-20:* `manugrs` did **not** end at the 2026-02-01 cutover. At suite level it ran **320,304,305 hits through 2026-07-19** and is still doing 8–13M/month, in parallel with `manulifeglobalprod`. Only the `manulifeim.com` marketing site wound down. Treat "with_legacy" as a **union of two live suites**, not a history splice. |
| **D8** | **Individual-login traffic is out of anomaly scope** | Business rule, 2026-07-20. Encoded as `SCOPE_LOGIN_HOST_EXCLUDE` ([settings.py:64-77](../../databricks/conf/settings.py)) and subtracted from `suite_scope` in **every** URL/suite mode ([01_bronze_ingest.py:90-97](../../databricks/src/01_bronze_ingest.py)). An explicit **host list, not a `%portal%` pattern** — four of the six hosts don't contain "portal" and the French `portail.manuvie.ca` wouldn't match it. Row-set-identical today (no include pattern reaches those hosts), so it is defense-in-depth against future widening, which would otherwise pull in ~94% of the manugrs suite. **Not yet ruled on, deliberately still in scope:** `retirement.sponsor.manulife.com` (sponsor ≠ member), `manulifeplan.ca`, `epargnemanuvie.ca`. |

---

## 2. Project context

**Goal:** production-grade anomaly detection over Adobe Analytics clickstream for Manulife GWAM **Canada Retirement** (group-retirement web properties), surfacing volume/behavior anomalies (level shifts, drops, spikes) with investigation support.

**Data reality:**
- Two report suites, **both live and concurrent** (D7): `manugrs` (320,304,305 hits, 2024-01-01 → 2026-07-19) and `manulifeglobalprod` (8,412,803 hits, first seen 2026-03-10). The 2026-02-01 "cutover" applies only to the URL-filtered *marketing* population — the `manulifeim.com` site wound down while the underlying suite kept running. eVar overlap is partial (12 shared / 50 current-only / 8 legacy-only — see doc-14).
- **⚠️ Unresolved:** the inventory puts `manulifeglobalprod`'s first unfiltered day at **2026-03-10**, but earlier scoped runs and `BACKFILL_START` say **2026-02-01**. Verify before citing either (backlog §8).
- **Scope reality (2026-07-20 inventory).** Today's shipped filters capture **<1%** of suite traffic: on manugrs the `en_only` filter matches 0% and the legacy filter 5.10%; on manulifeglobalprod `en_only` matches 31.82%. This is mostly **by design** — ~94% of manugrs is D8 login traffic (`portal.manulife.ca` ~130M, `id.manulife.ca` 62.6M, `grsmembers.manulife.com` 64.5M, `gsrs1.manulife.com` 24.4M), whose paths spell `groupretirement` **unhyphenated** and so evade retirement regexes anyway.
- **Genuinely uncovered retirement traffic:** 1,393,973 hits (manugrs) + 661,226 (manulifeglobalprod) match the retirement regex, aren't noise, and no shipped filter ingests them. Overwhelmingly **French** — `manulifeim.com/group-retirement/ca/fr/*` (801,461 at the root), `/ca/fr/particuliers/regimes-collectifs/retraite-collective` (183,698) — plus EN audience variants `/ca/en/{business,advisor}/group-plans/*` and `/ca/en/personal/group-plans/resources` (285,266). The `broad` pattern set already covers all of these (D3); `epargnemanuvie.ca` does not and is blocked on the D8 ruling.
- On `manulifeglobalprod`, **36.29%** of retirement-regex matches are noise (`/ph/` Philippines pages + AEM authoring hosts) — the existing `SCOPE_URL_LIKE_EXCLUDE` is doing real work.
- ~1,198 columns in the raw census; ~96 live eVars in manugrs.
- Event decoding is incomplete: custom events (10000+ range) resolve to "unknown" in `ADOBE_STD_EVENTS`; the `event.tsv` lookup (`new_data/event.tsv`) was never wired into the notebooks — see §8 backlog.
- French-language traffic is material (manugrs ~40.5% vs ~30.1% current suite) — URL scope must not silently drop `/fr/` paths. The inventory confirmed this is the **single largest scope gap**; `LEGACY_SCOPE_URL_LIKE` now carries the FR legacy root alongside EN.
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
├── gwam_canada_retirement_eda.py        ← unified profiler, BOTH rsids, widget-driven
└── gwam_canada_retirement_charts.py     ← unified interactive charts, BOTH rsids, widget-driven
```

**Two files, `.py` only.** The `.py` *is* the artifact: Databricks imports it as a notebook
("source" format). The paired `.ipynb` run records, the `_manugrs.py` snapshot, and
`README.md` were removed — the README's run procedure now lives in each notebook's own
header cell, so the instructions travel with the file you import. Recover any of them from
git history (§0.6).

**Reached 2026-07-20** — `eda/` now holds exactly two runnable notebooks. There is **no `archive/` directory**: retired notebooks were removed at `8ac2551` and git history is the archive (§0.6). Every future analysis need is met by adding a widget value or a section — never a new notebook.

### 4.2 Widget contract (both notebooks share it verbatim)

Config is 100% `dbutils.widgets` — already true in the `.py` exports (66 widget refs); the contract below extends it to dual-rsid and editable scope lists.

| Widget | Type | Default | Notes |
|--------|------|---------|-------|
| `rsid_list` | multiselect | `manugrs,manulifeglobalprod` | Replaces single-value `rsid_filter` text widget ([gwam_canada_retirement_eda.py:50-72](../../eda/gwam_canada_retirement_eda.py)). All sections loop/union over selected rsids and label outputs by rsid. |
| `url_scope_list` | text (newline/comma-separated LIKE patterns) | `%/group-retirement%`<br>`%/group-plans%`<br>`%/regimes-collectifs%` | **Seeded from the 2026-07-20 inventory (D3 closed).** Already language-agnostic — covers EN *and* FR, personal/business/advisor. Add/remove patterns here — never in code. Blank entry = no URL filter. |
| `url_scope_mode` | dropdown: `broad` / `en_only` | `broad` | **Two modes, not three (revised 2026-07-20).** `broad` uses `url_scope_list` verbatim — the visible widget is authoritative, honouring the "add/remove patterns here, never in code" rule above. `en_only` is the sole override, pinning the single pattern the bronze pipeline ingests for like-for-like comparison. `custom` was dropped as redundant: editing the list *is* customising it. The first implementation hardcoded the `broad` set and silently ignored the widget — a trap that made added URLs appear to do nothing. **Diverges from the pipeline's `en_only` by design — see D5.** |
| `url_scope_exclude` | text (LIKE patterns) | `%adobeaemcloud.com%`<br>`%/ph/%` | Noise subtracted after includes. Mirrors `SCOPE_URL_LIKE_EXCLUDE`; removes 36.29% of manulifeglobalprod regex matches. |
| `login_host_exclude` | text (LIKE patterns) | the 6 D8 hosts | Individual-login hosts, subtracted in every mode (D8). Mirrors `SCOPE_LOGIN_HOST_EXCLUDE`. Analysts who need to *study* login traffic clear this widget deliberately — it is never silently off. |
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

S4b/S4c absorb everything `gwam_url_scope_inventory.py` did (scope coverage, addable-rows audit, per-column URL profiling) — **that notebook is now retired** (`8ac2551`; last content at `408de5a`). Port from the git version, and carry over its two load-bearing behaviours: the **blank-guarded coalesce** (Adobe writes empty strings, not NULLs, so a plain `coalesce` returns `""` from `page_url` and never falls through) and **ID generalization** (`/member/12345` → `/member/{id}`) without which rollups shatter into singleton rows.

### 4.4 SHAREABLE emit + manifest protocol (keep, unredacted)

Every section emits `===== BEGIN SHAREABLE: <id> =====` JSON via `emit()` ([gwam_canada_retirement_eda.py:142](../../eda/gwam_canada_retirement_eda.py)); the run ends with a `run_manifest` of per-section byte counts + sha1s. **Keep this protocol** — it is how runs are verified reproducible (16/16 manifest check). Under D2 the manifest's sha1 **self-redaction is removed**: hashes and payloads are emitted in full.

### 4.5 Shared helper module — `eda/_gwam_common.py` *(does NOT exist yet — create it)*

Extract the helpers currently duplicated across `gwam_canada_retirement_eda.py`, `gwam_canada_retirement_charts.py`, and the retired `gwam_url_scope_inventory.py` (`git show 408de5a:eda/gwam_url_scope_inventory.py`):
- `resolve_date_expr(df)` (fallback chain — [gwam_canada_retirement_eda.py:216](../../eda/gwam_canada_retirement_eda.py))
- `_resolve_scope_cols` / `scope_condition` (rsid + URL filter — lines 240–244), now accepting a **list** of rsids and a **list** of URL patterns
- the D4 coalesce rule
- `emit()` + manifest builder
- widget registration for the §4.2 contract (one function both notebooks call)

Notebooks import it via the existing `_bootstrap`-shim pattern used by `databricks/` (repo-root `sys.path` insert), so the module works in both Databricks Repos and local runs.

### 4.6 Consolidation work package — **this is the next wave**

Steps 0, 1, 3, 5, 6 and 7 are **done** (2026-07-20); 2 and 4 remain.

0. ~~Export the manugrs notebook~~ — **done** (`408de5a`, `eda/gwam_canada_retirement_eda_manugrs.py`). `jupyter`/`nbformat` are not installed; that export came from a stdlib JSON parse (§0.5).
1. ~~Port manugrs-specific logic~~ — **nothing to port; file deleted.** ⚠️ **This step rested on a false premise.** `_manugrs.py` was never a suite variant — it was an *older snapshot of the same notebook*. Verified 2026-07-20: its `rsid_filter` default was `manulifeglobalprod` (not `manugrs`); `legacy` appeared 0× in both files; it held 0 hardcoded date literals; its scope helpers were byte-identical; and the unified notebook was a strict superset (it alone has S4c). The "legacy event ids" and "2024-01 → 2026-07 date coverage" this step described **do not exist in code** — both suites derive event ids at runtime from `post_event_list`, and the date range is a property of the data, not the notebook. Do not go looking for them again.
2. **Create `eda/_gwam_common.py`** (§4.5); refactor both `.py` files to use it. ⚠️ Now the *only* duplication left: `_csv()`, the `URL_SCOPE_*` constants, and `like_any()`/`_like_any()` are copy-pasted across the two notebooks. Databricks `%run` or a workspace-file import is required — a plain `import` will not resolve.
3. ~~`rsid_filter` → `rsid_list`~~ — **done.** Both notebooks take `rsid_list` (default `manugrs,manulifeglobalprod`) plus `url_scope_mode` / `url_scope_list` / `url_scope_exclude` / `login_host_exclude`. Implemented as a single `isin` scope over both suites rather than a per-rsid loop-and-union: sections stay untouched, and cross-suite comparison comes from `window_frame.filter.rsid_breakdown` (per-suite row counts, warns when a suite contributes 0) and `chart:traffic_ts.rows_by_rsid`. A full per-section rsid facet is still open if the breakdown proves insufficient.
4. ~~Finish masking removal (D2)~~ — **done (2026-07-23, ADR-0007 §5 full-raw).** `mask()`, `is_sensitive()`/`DIRECT_IDENTIFIERS`, the emit-time email/IPv4 scrubber and every shape-only branch are removed; all columns emit raw, and the coverage caps (S5 top-120, S7 top-25, S9 fixed allow-list) are now census-driven. ⚠️ `synth/` still consumes `<masked:...>` tokens from the *committed* spec (`synth/spec/synthesis_spec.json`) produced by an older run — that data artifact refreshes only by re-running the notebook on Databricks, not by the code change.
5. ~~Seed `url_scope_list` from the inventory~~ — **done** (D3, §4.2).
6. ~~Apply the same widget contract to the charts notebook~~ — **done.** Also fixed a previously untracked D4 violation there: it preferred `post_page_url` over `page_url` (blank 36-46% vs ≤0.013%), now the blank-guarded coalesce.
7. ~~Retire the extra notebooks~~ — **done** (`8ac2551`, extended 2026-07-20). `git rm`, not an `archive/` folder; history is the archive.
8. **Verify:** both notebooks run end-to-end on Databricks with defaults (both rsids); manifest completeness check passes; `window_frame.filter.rsid_breakdown` shows a **non-zero row count for each** of `manugrs` and `manulifeglobalprod`; `grep -rn "mask(\|RAW_OK_DIMS" eda/*.py` returns nothing; `git ls-files eda/` returns **exactly 2 files, both `.py`**.

---

## 5. Databricks pipeline phase

### 5.1 Current state

Medallion pipeline `databricks/src/`: `00_freshness_guard` → `01_bronze_ingest` → `02_silver_conform` → `03_gold_kpis` → `04_detect`, with `common.py`, `silver_lib.py`, `gold_lib.py` (flat modules, not packages). Config is **hardcoded** in [databricks/conf/settings.py](../../databricks/conf/settings.py) — line numbers as of 2026-07-20: `SCOPE_RSID` (19), `SCOPE_URL_MODE` (32), `SCOPE_URL_LIKE` (35), `SCOPE_URL_LIKE_BROAD` (49), `SCOPE_URL_LIKE_EXCLUDE` (57), `SCOPE_LOGIN_HOST_EXCLUDE` (70), `SCOPE_SUITE_MODE="current_only"` (91), `LEGACY_SCOPE_RSID` (99), `LEGACY_SCOPE_URL_LIKE` (100). Only `target_catalog` resolves from job params today.

**Known D4 violation:** [01_bronze_ingest.py:69](../../databricks/src/01_bronze_ingest.py) filters on `post_page_url` alone in `en_only` mode. D8 exclusion is applied correctly in all modes (lines 90-97).

### 5.2 Required changes (D5, D7)

1. Extend `resolve(dbutils)` so **rsid list, URL mode, URL pattern list, suite mode, and date range** all resolve from job parameters / widgets first, falling back to `settings.py` defaults. Same names as the §4.2 widget contract.
2. **D4 migration — measure before changing.** Run one cheap query first: count rows matching `SCOPE_URL_LIKE` on `post_page_url` vs on `coalesce(page_url, post_page_url)`, for `rsid=manulifeglobalprod` since `BACKFILL_START`. If the delta is ≈0, land the coalesce fix as a no-op; if material, sequence it with the P3 backfill. **Do not quote the 36.41% population blank rate as the in-scope loss** — much of that blank mass is D8 login traffic that is out of scope anyway; the true in-scope delta is unmeasured. **Prerequisite:** bronze projects `post_page_url` but *not* `page_url` ([01_bronze_ingest.py:78-79](../../databricks/src/01_bronze_ingest.py), `conf/bronze_columns.py`) — adding `page_url` to the projection is now a D4 blocker, no longer "deferred".
3. Default `SCOPE_SUITE_MODE` → `both` (D7): bronze ingests both rsids, silver conforms them onto one schema with an `rsid` column, gold KPIs are computed per-rsid **and** combined. ⚠️ This is only safe *because of* D8 — without the login-host exclusion, `both` would pull ~320M rows of member-portal traffic into bronze. Confirm `SCOPE_LOGIN_HOST_EXCLUDE` is applied before flipping.
4. **`SCOPE_URL_MODE` flip is gated, not blocked.** Remaining conditions: P2 re-profile of the widened population, and product sign-off on `%/group-plans%` (it is the umbrella containing group-benefits/business/advisor). Any flip must run as a full `mode=backfill` with gold truncated — flipping under `mode=incremental` writes a step change mid-series that the detector reads as a level-shift anomaly.
5. Reuse the pipeline's existing `event_list` normalization; do not re-implement it in notebooks.
6. **Partition-predicate gotcha:** `process_date` filters must be **dtype-aware** to get Delta partition pruning — build the predicate to match the column's actual type (date vs string) instead of relying on implicit casts, otherwise full scans.
7. Best practices to enforce: idempotent MERGE writes keyed on natural keys; freshness guard stays as job gate; explicit schemas at ingest (no inference in prod); table properties documented; unit-test `silver_lib`/`gold_lib` transforms against `data/synth/` fixtures.

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
1. **D4 measurement gate** (§5.2 step 2) — the one query that decides whether the `post_page_url` → coalesce fix is a no-op or a re-baseline. Blocks the D4 code change and the `page_url` bronze projection.
2. ~~Finish D2 masking removal~~ — **done (2026-07-23).** `mask()` and the shape-only carve-out are removed; all columns emit raw (ADR-0007 §5 full-raw). The committed `synth/spec/*.json` still holds `<masked:...>` tokens from an older run — refresh via a Databricks re-run, not a code edit.
3. **Rule on the three unclassified hosts (D8):** `retirement.sponsor.manulife.com` (sponsor portal — sponsor ≠ individual member, so probably IN scope), `manulifeplan.ca`, `epargnemanuvie.ca` (FR brand domain, `/15120cwretraite` paths; no current pattern matches it either way).
4. **Resolve the `manulifeglobalprod` start-date discrepancy** — inventory says first unfiltered day 2026-03-10; `BACKFILL_START` and earlier scoped runs say 2026-02-01. One of them is wrong and `BACKFILL_START` depends on the answer.
5. ~~Wire `new_data/event.tsv` into S6 event decode~~ — **done (2026-07-23).** S6 loads the TSV best-effort (widget `event_lookup_path`) with an inline standard-event map plus an Instance-of-eVar formula fallback (100–199 → eVar1–100, 10000–10099 → eVar101–200), so custom events (10004–10048+) resolve to labels instead of "unknown".
6. `uc_discovery` scope conditions still reference `post_page_url` only — align with the D4 helper.
7. **Reconcile §3 EDDL spec vs S7 live-eVar census per rsid** — includes resolving the eVar166/eVar169 URL-type contradiction (§3.4). Blocks eVar-based detection rules.
8. Asset bundle adoption (§6.2).
9. ~~Consume URL-scope inventory output → seed `url_scope_list`~~ — **done 2026-07-20** (D3).

---

## 9. Phase plan (end-to-end, with verification)

| # | Phase | Deliverable | Verify |
|---|-------|-------------|--------|
| P0 | Scope freeze | ⏳ **in progress** — URL scope list seeded ✅ (D3), login-host rule landed ✅ (D8), notebooks consolidated ✅; masking removal still partial | scope list checked into widget defaults ✅; `grep -rn "mask(\|RAW_OK_DIMS" eda/*.py` returns nothing ❌ (backlog #2) |
| P1 | eVar dictionary | §3 cross-checked vs S7 census per rsid | every live eVar has a name or an explicit "unmapped" flag; eVar166/169 contradiction resolved |
| P2 | EDA consolidation | Exactly 2 widget-driven dual-rsid notebooks (§4.6) | §4.6 step 8 checklist |
| P3 | Pipeline unification | `resolve()` param-driven scope; `SCOPE_SUITE_MODE=both`; dtype-aware `process_date` predicates | end-to-end run green on both rsids; partition pruning confirmed in query plan |
| P4 | Detection tuning | Detectors calibrated on both-rsid gold; synth eval refreshed | precision/recall reported vs `known_events.json`; ≥×1.9 level-shift floor documented in results |
| P5 | Job productionization | Asset bundle; job un-paused per §6.2 criteria | 7 green scheduled runs; alerts fire on induced failure |
| P6 | AKS serving | Scoring API live in dev namespace, then prod (§7) | `/health` green; frontend reads anomalies end-to-end |
| P7 | Frontend + Foundry | ADR-0008 React AI/BI + Foundry investigation copilot | stakeholder walkthrough using live data |

---

*Doc 16 · created 2026-07-19 · revised 2026-07-20 (URL scope inventory landed) · supersedes scattered guidance in docs 02/12/14/15 where they conflict — including their 2026-02-01 cutover framing (D7) and any reference to the retired EDA notebooks.*
