# 17 — CoverMe EDA Readiness & SME Gap Assessment

> **Purpose.** The four CoverMe Databricks jobs have now run —
> `coverme_discovery_probe`, `coverme_url_scope_inventory`, `coverme_eda`, `coverme_charts`
> (HTML exports at repo root). This document (a) confirms the two **prerequisite** runs and the
> **EDA** gave us a comprehensive, internally cross-checked understanding of the CoverMe data,
> (b) lists the **engineering must-fixes** before we proceed, and (c) states exactly **what we
> need from business / SME** to fully proceed to the complete EDA, the medallion pipeline, and
> anomaly detection. Companion to [03 — Phase 1 Anomaly Detection](03-phase1-anomaly-detection.md),
> [16 — E2E Production Blueprint](16-e2e-production-blueprint.md), and the GWAM reference
> [15 — Consolidated EDA Report](15-consolidated-eda-report.md).

---

## 0. Context — why this assessment exists

CoverMe is the **2nd anomaly-detection domain** (with GWAM Canada-Retirement). The EDA/charts
notebooks (`eda/coverme_eda.py`, `eda/coverme_charts.py`) and the two discovery notebooks were
built 2026-07-23, mirroring the GWAM template, and have **now been executed on Databricks**.
Before investing in the productionization tail (medallion pipeline + detector), we need a
clear-eyed answer to one question: **do we actually understand the CoverMe data well enough to
build on it, and what is still unknown?** This assessment answers that and converts the unknowns
into a concrete SME agenda.

### ⭐ Headline: the corpus-wide "#1 blocker" is CLEARED for CoverMe
The research package repeatedly states its **#1 blocker**: *"no production time-series exists yet…
baselines, thresholds, and model selection are blocked until a ≥30-day (ideally 90-day) hit-level
feed lands per domain"* ([README](README.md), [03 §1](03-phase1-anomaly-detection.md),
[10 §3](10-data-profile-alignment.md)). It also assumed **CoverMe lands on Synapse serverless**,
read via a UC external location ([ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md)).

**Both are now stale for CoverMe.** The runs prove CoverMe already exists as a **native Databricks
Delta table** — `csdo_prod_catalog.adobe_coverme_bronze.hit_data`, 17.13 GB, partitioned by
`hit_date` — with **57.7M scoped rows over 2023-02-28 → 2026-07-22 (1,211 daily points)**. That is
~40× the 30-day minimum. **CoverMe is no longer data-blocked** — baselines, seasonality,
thresholds, and backtests can be computed today. (Docs 10/11/16 and ADR-0006 should be updated to
reflect the Databricks-native landing; tracked in §5.)

---

## 1. Prerequisite runs — verified clean

| Run | Sections | Status | Notes |
|---|---|---|---|
| **`coverme_discovery_probe`** | 20/20 SHAREABLE | ✅ none errored | `run_manifest.skipped = {}`. Only `pdf_labels` (S0_5) deliberately skipped (`enabled:false`, no PDF wired). The older "12/12 sections" note is stale — this run emits **20**. |
| **`coverme_url_scope_inventory`** | 7 SHAREABLE | ✅ complete, **print-only** | `s4_delta_write.written = false` because `target_catalog = "__SET_ME__"`. Scope tables were **not persisted** — the run computed the comparison and printed it. Set the catalog before a persisting re-run *if* the pipeline needs the scope tables materialized. |

**Both prerequisites succeeded and agree with each other** on shape, hosts, scope coverage,
identity, and DQ. Parse "failures" seen earlier were **multi-part concatenation blocks**
(`synthesis_spec` = 3 parts, `s6_url_scope_rollup` = 2 parts) — join byte-exact before `JSON.parse`;
nothing is truncated.

---

## 2. What we now understand about CoverMe (comprehensive & cross-checked)

**Source & shape.** `csdo_prod_catalog.adobe_coverme_bronze.hit_data` — **1,180 columns**, Delta
10,633 files / 17.13 GB, partitioned by `hit_date`. Whole table **60,140,963 rows**; **CoverMe-scoped
57,716,721 (95.97%)**. **Single report suite — no `rsid` column** (Adobe pins the suite in the feed);
scope is **URL-only**. Coalesce order is **`page_url` first** (0.0005% blank) → `visit_start_page_url`
→ `first_hit_page_url` → `post_page_url` (this is **inverted vs GWAM**, which is post-first). Daily
feed: **median 24.0 h** interarrival, ~37–40 k rows/day recent; late-arrival lag p50/p95/p99 =
**1 / 2 / 5 days**.

**Volume & seasonality (the anomaly baseline).** Full-history daily **CV = 0.24**, **weekly
autocorrelation (lag-7) = 0.56**, lag-28 = 0.25 → **strong, stable weekly seasonality**. Day-of-week
index is weekday-peak, **weekend ≈ −22%** (Sat 0.78 / Sun 0.80). One clear **level-shift up in
Jan 2026** (2026-01-07…10). Notable single-day swings to validate a detector against:
2024-08-26 ×2.58, 2026-06-04 ×1.99 (spikes); 2026-06-05 ×0.45, 2026-04-23 ×0.49 (drops).
→ Justifies the [03 §3.1](03-phase1-anomaly-detection.md) seasonal forecast approach (`darts`
Holt-Winters / Prophet) with adaptive quantile thresholds.

**Quote → Application funnel (S6b, exact full history).** All five business-tagged events fire
(`events_not_firing: []`):

| Step | event | hits | % scoped |
|---|---|---:|---:|
| Quote Start | 228 | 3,437,847 | 5.96% |
| Quote Complete | 229 | 1,803,883 | 3.13% |
| Save Quote | 232 | 145,607 | 0.25% |
| App Start | 269 | 874,866 | 1.52% |
| App Confirm | 240 | 278,233 | 0.48% |

Conversion: QuoteComplete/QuoteStart **0.52**, AppConfirm/AppStart **0.32**, AppConfirm/QuoteStart
**0.08**. These are hit-presence counts (proxy for volume), not unique visitors.

**Dimensions & identity.** Census (sample): **304 populated / 201 sparse / 675 dead** of 1,180;
**95 core (≥99% populated) — all base columns; no custom eVar/prop reaches the core bar**
(`evar_core = []`, `prop_core = []`). **125 live eVars + 24 live props.** Values *suggest*
semantics — `post_evar4` product line (home 45% / travel 37% / life 6.5%), `evar52` site hierarchy,
`evar8` content language (EN ~96%), `evar65` OneTrust consent (opt-out 91.7%), `evar116` bot flag
(Bot 5.6%), `evar133` page type, `evar9` province (ON 78.6%) — **but labels were not loaded**
(`labels_loaded = false`). **Identity is `mcvisid`-only** (540k–570k distinct, 0% null);
`cust_visid`/`post_cust_visid` are **100% null**, `userid` is a single constant → **no CRM/member
id, no cross-device stitching possible** (consistent with GWAM / [ADR-0007](adr/adr-0007-identity-privacy-layer.md)).

**Data quality.** `pagename` **22% null** (use URL path as primary dimension), `post_event_list`
**6.8% null**; bots/excluded ≈ **5.7%** (`exclude_hit=14` 4.9% + `=4` 0.8%, corroborated by
`evar116` 5.6%); duplicates **0.29%** on `(post_visid_high, post_visid_low, visit_num,
visit_page_num)`; **constant −4/−5 h clock skew** (America/Toronto baked into `date_time`);
**30 missing calendar days** (clustered — see §4 item 8).

**Geo & language.** Country **CAN 84% / USA 12%** (USA largely datacenter — Ashburn/AWS visible in
regions). Language ~50/50 by domain (coverme.com/insttrip = EN, pourmeproteger = FR).

**Charts.** All 11 panels rendered with data (traffic TS, DoW×hour heatmap, 217-country choropleth,
regions, pagenames, language mix, 5-step funnel + timeline, top eVar4/eVar6, monthly volume).

---

## 3. Engineering must-fixes (ours to close — no SME needed)

| # | Defect | Evidence | Fix | Impact if unfixed |
|---|---|---|---|---|
| **E1** | **S6 `event_decode` crashed** → whole section skipped; `ts_events` and `synthesis.events = {}` missing. | `run_manifest.skipped = {S6: ArrayIndexOutOfBounds}`. Root cause at [`coverme_eda.py:984`](../../eda/coverme_eda.py#L984): `try_cast(element_at(split(e,'='),2) as double)` — under Databricks **ANSI mode**, `element_at(…,2)` on a bare event id (1-element array) **throws** (the `try_cast` only guards the cast, not the index). | Change `element_at` → **`try_element_at`** at line 984, then re-run S6 + S8 `ts_events`. One-token change; S6b funnel is unaffected (it uses index `[0]`). | **No per-event value stats and no per-event daily series** — this is the exact input the *operational / tagging-health* detector needs ([03 §2](03-phase1-anomaly-detection.md)). Must fix before that class ships. |
| **E2** | URL-scope inventory persisted **no tables** (`target_catalog = "__SET_ME__"`). | `s4_delta_write.written = false`. | Decide whether scope tables must be materialized; if so, set the catalog and re-run. Otherwise the printed comparison is sufficient. | Low — comparison data already captured. |
| **E3** | EDA "broad" scope (95.97%) vs inventory `cm_broad` (99.42%) differ. | EDA = 3 brand-host LIKE minus UAT/AEM/stage/localhost; inventory `cm_broad` additionally admits ~0.7% of prod-adjacent hosts. | **Reconcile once the scope tier is chosen** (§4 item 3) and lock the single definition in `settings.py`. | Medium — scope is the foundation of every downstream metric; it must be one authoritative definition. |
| **E4** | `event 20100` has `val_mean ≈ −6.7e10`. | Discovery `event_decode`. | Investigate as a counter/overflow/serialization artifact; exclude from value-based KPIs until understood. | Low, but would poison any mean-based event-value metric. |

---

## 4. What we need from Business / SME (the clarity gaps)

Ranked by how hard each one blocks the build. Each row states the **question**, **why it blocks**,
our **current working assumption**, and the **artifact that would resolve it**.

| # | What we need clarified | Why it blocks | Our current assumption | Resolving artifact |
|---|---|---|---|---|
| **1** | **Full event-ID → name dictionary** for the custom events beyond the 5 funnel events. The top-firing events (164 ≈ 71%, 151 ≈ 69%, 103, 10017, 132/153, 510-513 …) are **all unlabeled** (`labels_loaded=false`). | The **operational / tagging-health** detector monitors "an event that always fires suddenly stops" ([03 §2/§3.3](03-phase1-anomaly-detection.md)). We can't decide which high-frequency events are *critical tags* without their meaning. | Only 228/229/232/240/269 are business-meaningful (from `CoverMeDataMap.xlsx`); the rest are page/interaction instances. | The CoverMe **events PDF** / Adobe report-suite event var-map (the `pdf_labels` input that was skipped). |
| **2** | **Authoritative eVar/prop label map** — confirm the *inferred* semantics (evar4 product line, evar52 hierarchy, evar8 language, evar65 consent, evar116 bot, evar133 page type, evar9 province, evar131/111/40 visitor-id-like, evar132/134/135 constants). | Segment/feature engineering ([03 §7](03-phase1-anomaly-detection.md)) and the metric registry name these dimensions; guessing risks mislabeled KPIs. | The value-inferred labels above are correct. | Adobe **var-map export** for the CoverMe suite, or SME sign-off on the inferred list. |
| **3** | **Scope-tier decision.** Pick **cm_strict (82.3%, signal purity)** vs **cm_broad (99.4%, coverage)**; and rule on the 3 unclassified prod hosts (`mlc--cms.na154` 0.35%, `mlc--cms.can52` 0.23%, `coverme-en.apps.cac.pcf` 0.10%) and the **71,130 uncovered legacy life-insurance hits (0.12%)**. Note `insttrip.manulife.com` **died 2024-03-11**. | Scope defines the denominator of **every** metric; ambiguity here propagates to all baselines and the medallion filter. | **cm_broad** (URL-only, 3 brand hosts + prod-adjacent), excluding UAT/AEM/stage. | Business confirmation of "what is in-scope CoverMe traffic," + a ruling on the legacy life-insurance tail. |
| **4** | **Funnel completeness & basis.** Is 228→229→232→269→240 the canonical journey? Are there other conversion signals (phone/chat, PDF download, save-and-resume, agent-assisted)? Is **hit-presence** an acceptable KPI basis, or must it be **unique visitors**? Confirm Save Quote (0.25%) / App Confirm (0.48%) low rates are expected. | These become the first `active` metric-registry KPIs ([03 §5](03-phase1-anomaly-detection.md)); the wrong set means we monitor the wrong business signals. | The 5 events are the funnel; hit-presence is an acceptable v1 proxy. | Business funnel definition / product analytics owner sign-off. |
| **5** | **Language field of record.** Numeric `language` column ≈ **45% FR / 45% EN + ~11% unlabeled** vs content eVar (`evar8/149`) **≈ 96% EN / 4% FR**. Which is authoritative for the language-mix KPI? | A language-mix anomaly KPI needs one definition; the two disagree by ~50 pts. | `evar8` (content language) is the business signal; numeric `language` is browser locale. | SME confirmation of the intended "language" dimension. |
| **6** | **Bot / exclusion policy.** Confirm `exclude_hit ∈ {14,4,11}` (≈5.7%) should be **dropped**, and reconcile with `evar116` "Bot" (5.6%). Which is authoritative? | Bots inflate baselines and create false anomalies; the Silver filter needs one rule. | Drop `exclude_hit>0`; treat `evar116=Bot` as corroboration. | SME/Adobe confirmation of the exclusion definition. |
| **7** | **Identity scope confirmation.** Confirm CoverMe anomaly detection is **traffic/behavior-level only** — no person-level, no cross-device, no CRM join — because `cust_visid`/member-id are 100% null (`mcvisid` only). | Sets the ceiling on segmentation and rules out visitor-stitched KPIs ([11](11-privacy-identity-governance.md)). | Behavior-level only; `mcvisid` is the visitor grain. | SME/data-owner acknowledgement (aligns with [ADR-0007](adr/adr-0007-identity-privacy-layer.md)). |
| **8** | **The 30 missing days** — are these true collection outages, or feed/export gaps? (Clusters incl. 2023-04-09→12, 2023-12-19→21, 2025-08-05→07, 2025-12-02→07, **2026-06-07→23**.) | Missing days distort seasonal baselines and can masquerade as "drop" anomalies; the pipeline must know whether to impute or treat as known-zero ([03 §6](03-phase1-anomaly-detection.md)). | Feed gaps, not zero-traffic days → impute/interpolate. | SME/ops confirmation of which dates are known incidents. |
| **9** | **PII & consent sign-off under full-raw regime.** `evar65`/`evar81` carry OneTrust consent (opt-out 8.3%); ADR-0007 §5 is **full-raw / no masking**. Need the CoverMe data-owner approval that GWAM already has. | Governance gate before Silver/Gold materialize identifiers ([11](11-privacy-identity-governance.md), [ADR-0007 §5](adr/adr-0007-identity-privacy-layer.md)). | Full-raw approved (parity with GWAM 2026-07-23). | CoverMe data-owner written approval + PII-classification review. |
| **10** | **Metric-registry seed sign-off.** Confirm the detection targets: eVars **4,5,6,11,16,52,111,148** + funnel events **228/229/232/240/269** (from `CoverMeDataMap.xlsx` "Anomaly Detection" tags), each with owner + direction. | The registry is config-not-code and drives Gold builds ([03 §5](03-phase1-anomaly-detection.md)); it needs owner-confirmed entries. | The tagged set is the seed; funnel volumes + conversion ratios are the first `active` KPIs. | Business owner confirmation of the seeded `metric-registry.yaml` rows. |

---

## 5. Gap to the full build (roadmap, post-SME)

**A. Finish the EDA (ours, ~hours).** Apply fix **E1** (`try_element_at`) and re-run `coverme_eda`
so S6 `event_decode` + S8 `ts_events` + `synthesis.events` populate. Then the EDA is complete and
every SHAREABLE section is present.

**B. Medallion pipeline (ours, net-new — `databricks/src` is GWAM-only today).**
- `databricks/conf/settings.py` + `bronze_columns.py`: a **CoverMe** config — source
  `csdo_prod_catalog.adobe_coverme_bronze.hit_data`, **no `rsid`** (URL-only scope from the §4-item-3
  decision), **page_url-first coalesce**, bot/exclusion rule (§4 item 6), `mcvisid` identity grain.
- `01_bronze_ingest` → `02_silver_conform` (prune ~1,008 empty slot columns; decode
  eVar/prop/event; pin **America/Toronto** tz; drop excluded hits) → `03_gold_kpis` (funnel KPIs +
  volume/visits/visitors + language/geo + conversion ratios) → `04_detect`.
- A CoverMe `jobs/*.json` (mirror `gmai_pulse_daily.json`).
- **Wire `new_data/event.tsv`** into the decode path — an open backlog item for GWAM too
  ([16 §8](16-e2e-production-blueprint.md)); resolves SME item 1 for standard Adobe events.

**C. Anomaly detection (ours, after A+B).** Seed `metric-registry.yaml` with the §4-item-10 set;
`darts` univariate forecasting on funnel/volume series; `pyod` ECOD/COPOD multivariate on
engineered features; Databricks **Lakehouse Monitoring** for freshness/completeness; adaptive
thresholds + debounce. All now feasible because the data-acquisition gate is cleared (§0).

**D. Doc hygiene.** Update [README](README.md), [10](10-data-profile-alignment.md),
[ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md) to reflect that CoverMe is
**Databricks-native Delta with full history**, superseding the "Synapse / no time-series" framing.

---

## 6. Readiness verdict

| Area | Status |
|---|---|
| Prerequisite runs (probe + URL scope) | ✅ **Complete & consistent** |
| Data understanding (volume, seasonality, funnel, dims, identity, DQ, geo) | ✅ **Comprehensive** |
| Data-acquisition gate (≥30-day feed) | ✅ **Cleared** — 1,211 daily points, 3+ years |
| EDA completeness | 🟡 **~95%** — S6 event_decode (E1) to fix + re-run |
| Scope definition locked | 🟡 **Pending** — SME item 3 → then E3 reconcile |
| Event & eVar/prop labels | 🔴 **Blocked on SME** — items 1, 2 |
| Governance sign-off (PII/consent) | 🔴 **Blocked on SME** — item 9 |
| Medallion pipeline for CoverMe | 🔴 **Not started** — net-new (§5.B) |
| Detector wiring for CoverMe | 🔴 **Not started** — depends on A+B |

**Bottom line:** We have a **comprehensive, cross-checked understanding** of the CoverMe data and
the prerequisites are sound. **One engineering fix** (E1) completes the EDA. The **critical path to
the medallion + anomaly build runs through the SME agenda in §4** — above all the **event/eVar label
dictionaries (items 1–2)** and the **scope-tier ruling (item 3)**. Everything else is our own
build work, and the biggest historical risk (no time-series) is gone.

---

## 7. Verification / how to confirm this is done

1. **E1 fix:** edit [`eda/coverme_eda.py:984`](../../eda/coverme_eda.py#L984) `element_at`→`try_element_at`;
   re-run `coverme_eda` on Databricks; confirm `run_manifest.skipped == {}` and
   `synthesis_spec.events` is non-empty with `ts_events` present.
2. **First-run sanity (already green except S6):** `host_breakdown` shows all prod hosts non-zero;
   `funnel_kpi.events_not_firing == []`; `url_scope_audit.coverage.uncovered_cm_pct ≈ 0.1%`
   ([`coverme_eda.py` "Done" cell](../../eda/coverme_eda.py#L1540)).
3. **SME agenda:** take §4 to the business/analytics owners; capture answers as
   `metric-registry.yaml` seed rows + a locked scope definition in `settings.py`.
4. **Then** build §5.B (medallion) and §5.C (detector), validating each Gold KPI against the
   baselines in §2.
