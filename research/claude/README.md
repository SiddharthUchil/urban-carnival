# GMAI–Pulse — Finalized Anomaly Detection & Insights Solution (Canada Retirement + CoverMe)

**Claude's synthesis** of the Gemini and Perplexity research — re-grounded on the **2026-07-02 data
profiling** (`new_data/`) — into a single, decision-ready solution for detecting and explaining anomalies
in **Canada Retirement (GWAM)** and **CoverMe** web-analytics signals (Adobe Analytics data feeds) on an
**Azure + Databricks** stack, across two phases, each with **offline (batch)** and **online (scheduled
micro-batch)** designs — with a Gen-AI insights layer (models **tuned & served via Adaptive ML**) and a
planned **Akka** migration.

> **What changed 2026-07-02:** the `new_data/` profiling established that the two domains land on two
> different platforms (GWAM on Databricks/Unity Catalog; CoverMe on **Synapse serverless SQL**), that both
> share one canonical Adobe hit schema, and that no streaming collection or production time-series exists
> yet. Full traceability: [10-data-profile-alignment.md](10-data-profile-alignment.md).
>
> **↺ What changed since (2026-07-29 decision audit):** both halves of that finding are superseded.
> CoverMe turned out to be a **Databricks-native Delta table** (`csdo_prod_catalog`, 17 GB — no Synapse
> read path needed; [17 §1](17-coverme-eda-readiness.md)), and **production time-series now exist for
> both domains**: GWAM 3.25 B rows / 883 days ([19 §0](19-gwam-channel-readiness.md)), CoverMe 57.7 M
> scoped rows / 1,211 days ([17 §1](17-coverme-eda-readiness.md)). The 2026-07-02 text is kept as the
> historical record.

## Executive summary

GMAI–Pulse shifts monitoring from reactive to **proactive, explained alerts** ("what changed, where,
when, why, and what to do"). The design rests on eight decisions:

1. **One Databricks compute plane over two source platforms** — detection/ML runs only on Databricks.
   *↺ superseded in part (2026-07-23, doc 17): CoverMe's feed is a native Databricks Delta table, so the
   UC-external-location / Lakehouse-Federation read paths were never needed — the one-compute-plane
   decision stands, the CoverMe binding is direct.*
   *(new — reflects the profiled landing zones)* → [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md).
2. **Batch-first micro-batch ingestion** — both feeds land as at-rest tables, so Phase 1 runs scheduled
   micro-batch jobs at feed cadence (**daily grain first**); the Edge Network → Event Hubs streaming hot
   lane is an explicitly optional **Phase-3+ upgrade** with defined trigger criteria. *(amends the v1
   two-lane design)* → [ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md).
3. **Registry-driven metrics** — detection targets come from a versioned
   [`metric-registry.yaml`](metric-registry.yaml) seeded from the **29 dictionary rows business-tagged
   "Anomaly Detection"**, validated against the machine-readable profile. → [03 §5](03-phase1-anomaly-detection.md).
4. **Right model per signal** — `darts` forecasting for seasonal KPIs, `pyod` ECOD/COPOD for multivariate,
   and Databricks **Lakehouse Monitoring** (built-in freshness/completeness anomaly detection) for the
   operational/tagging-health class. **Adaptive thresholds**, not a fixed cutoff. → [ADR-0002](adr/adr-0002-model-family-selection.md).
5. **Phase-2 RCA via a grounded multi-agent Gen-AI layer** — contribution analysis (Cramér's V + Pearson
   residuals) + change-event correlation, narrated by guard-railed agents (grounding, abstention, JSON
   contracts, confidence gating, human-in-the-loop) over a RAG knowledge base. *(Change-event correlation
   is gated on acquiring the 11 missing correlation keys — [04 §2b](04-phase2-investigation-insights.md).)*
6. **Operational rigor both drafts omitted** — evaluation (PR-AUC, range-based F1, MTTD/MTTR), drift &
   retraining, a cost model, and PIPEDA/Law 25 governance with a **hard PII-classification gate** (24
   flagged columns) before any ingestion.
7. **Model layer = Adaptive ML (Adaptive Engine); runtime future = Akka.** Phase-2 agents run on an
   **RL-tuned open SLM** in a private environment, continuously improved from the analyst-feedback loop
   (frontier LLMs for cold-start/hard cases); clean service boundaries let detection + agents re-host onto
   Akka durable agents later (models via gRPC, at-least-once/saga semantics). Three-layer platform:
   **Databricks → Adaptive ML → Akka**. → [07](07-adaptive-ml-integration.md).
8. **Identity & Privacy layer with keyed pseudonymization; stitching deferred.** Visitor identifiers are
   pseudonymized with a Key Vault-held HMAC key at Bronze→Silver (joinability preserved, crypto-erasure
   possible); identity stitching is **gated** on a person-level ID actually existing in the feed (none
   does today). *↺ amended (2026-07-23, ADR-0007 §5 + doc-16 D2): profiling/EDA is now **full-raw** —
   all masking/redaction helpers were deleted from the notebooks; the pipeline's silver-layer keyed
   pseudonymization of visitor identifiers remains in place. The Synapse secure-view recommendation is
   moot — CoverMe is Databricks-native (doc 17).*
   *(new 2026-07-04 — verdicts on the Perplexity/Gemini extension research)*
   → [11](11-privacy-identity-governance.md), [ADR-0007](adr/adr-0007-identity-privacy-layer.md).

> **↺ #1 blocker — CLEARED for both domains (2026-07-23/29).** Production hit-level history now exists:
> GWAM 3.25 B rows / 883 days, CoverMe 57.7 M scoped rows / 1,211 days. The current top blocker is the
> **D8-vs-D9 conflict** — the SME's four-channel scope needs the sign-in traffic the D8 login rule
> excludes ([20 Q1](20-gwam-sme-questions.md), [16 §1 D8](16-e2e-production-blueprint.md)) — followed by
> `manucustomer.prod` feed access (20 Q2) and segment-scope sign-off (20 Q3).

## How to read this package

| Order | Document | What it covers |
|---|---|---|
| 1 | [01-critique-and-synthesis.md](01-critique-and-synthesis.md) | Critique of Gemini & Perplexity; what we keep/correct/add; **2026-07-02 reassessment** |
| 2 | [02-solution-architecture.md](02-solution-architecture.md) | End-to-end architecture; data grounding; stack; MLOps; **eval, cost, governance** |
| 3 | [03-phase1-anomaly-detection.md](03-phase1-anomaly-detection.md) | Phase 1 detection — offline + online; **metric registry** |
| 4 | [04-phase2-investigation-insights.md](04-phase2-investigation-insights.md) | Phase 2 RCA & insights — offline + online |
| 5 | [05-genai-and-akka.md](05-genai-and-akka.md) | Gen-AI strategy (agents, prompts, guardrails) + **Akka migration** |
| 6 | [06-diagrams.md](06-diagrams.md) | **5 Mermaid diagrams (D1–D5)** + Lucidchart import guide |
| 7 | [07-adaptive-ml-integration.md](07-adaptive-ml-integration.md) | **Adaptive ML** — the model-tuning/serving layer; how it changes the solution |
| 8 | [08-library-mapping.md](08-library-mapping.md) | pyod / darts / anomaly-detection-resources / ai-data-science-team → roles |
| 9 | [09-references.md](09-references.md) | Cited sources + confidence flags |
| 10 | [10-data-profile-alignment.md](10-data-profile-alignment.md) | **What the 2026-07-02 `new_data/` profiling changed** + open blockers |
| 11 | [11-privacy-identity-governance.md](11-privacy-identity-governance.md) | **Privacy, identity & governance** — Identity & Privacy layer, Law 25/C-27 mapping, Synapse secure views, erasure, roles |
| 12 | [12-eda-findings-analysis.md](12-eda-findings-analysis.md) | EDA findings from the real GWAM Databricks feed |
| 13 | [13-global-serving-topology.md](13-global-serving-topology.md) | **Global serving topology** — React/AKS surface + BFF, Azure AI Foundry Gen-AI plane, unstructured data lane (ADLS Gen2 → AI Search), global access/residency |
| 14 | [14-manugrs-cross-suite-analysis.md](14-manugrs-cross-suite-analysis.md) | **manugrs cross-suite analysis** — legacy vs current report suite, eVar overlap, geo/language profile · ⚠️ its 2026-02-01 cutover reading is corrected by doc-16 D7 (marketing site only; the suite is still live) |
| 15 | [15-consolidated-eda-report.md](15-consolidated-eda-report.md) | Consolidated EDA report across both report suites (stakeholder-facing) · same cutover caveat as doc 14 · **rev. 2026-07-22**: EDDL dictionary folded in, privacy regime inverted, open questions refreshed, **§8b EDA exit criteria** added |
| 16 | [16-e2e-production-blueprint.md](16-e2e-production-blueprint.md) | **End-to-end production blueprint & agent guidance — START HERE** — standing decisions D1–**D11**, EDDL eVar dictionary, 2-notebook EDA contract, URL scope inventory results, Databricks/jobs/AKS phases; supersedes older docs on conflict. Revised 2026-07-28: **D9** four-channel Canada-Retirement scope + **D10** segment-vs-URL re-baseline (both 🟡 proposed), and a ⚠️ conflict notice on D8 · revised 2026-07-29 (decision audit) · ↺ **revised 2026-07-29 (SME rulings): D11 — GWAM scope narrowed to the Public Website channel ONLY**, superseding D9, dissolving D8's conflict, re-pricing D10; marketing defined as the CID query parameter |
| 17 | [17-coverme-eda-readiness.md](17-coverme-eda-readiness.md) | **CoverMe** EDA readiness & SME gap assessment — E1–E4 engineering must-fixes, the SME agenda, readiness verdict. E1 fixed and re-run verified 2026-07-27 · ↺ **2026-07-29**: item 5 (language) resolved, item 8 (missing days) root-caused to the Databricks migration, item 9 **PII cleared verbally** — the backfill job is no longer doc-gated |
| 18 | [18-coverme-sme-questions.md](18-coverme-sme-questions.md) | **CoverMe** send-ready SME questionnaire, with Kerrian's rulings merged inline (Q1–Q10) · ↺ **2026-07-29**: Q4 language **answered** (domain rule approved; eVar149 the likely permanent field), Q7 PII/consent **answered** (no PII from Adobe; eVar65 = cookie consent), Q8 missing days **answered** (migration feed gap). Still ⏳: Q10 events 510-514, eVar148 verification |
| 19 | [19-gwam-channel-readiness.md](19-gwam-channel-readiness.md) | **GWAM Canada Retirement** channel readiness & SME gap assessment — the 2026-07-28 four-channel scope table mapped cell-by-cell to the repo, G1–G6 engineering gates, SME agenda. ↺ **Probe run clean 2026-07-29 — G1 closed** · ↺ **re-scoped 2026-07-29 to the Public Website channel ONLY (D11)**: D8 conflict dissolved, `manucustomer.prod` request retired, G2 promoted to critical, **new §1.1** (three SME anomaly signals) + **§2.5.1** (the CID marketing rule) |
| 20 | [20-gwam-sme-questions.md](20-gwam-sme-questions.md) | **GWAM Canada Retirement** SME questionnaire (Q1–Q12) with Abhisekh's answers merged inline. ↺ **2026-07-29**: Q1/Q2/Q7–Q11 **withdrawn** (their channels left scope), Q5 **answered** (marketing = CID query parameter), Q3 partly answered → **new Q3b** (`wealth-ca` / `pvt-wealth` brand variants), Q6 **escalated to blocking**, **new Part 4** (his three anomaly suggestions) |
| — | [metric-registry.yaml](metric-registry.yaml) | Versioned Phase-1 metric registry — **v0.5.0**: 29 CoverMe AD-tagged seeds (SME-confirmed; language/PII/data-gap rulings recorded in `meta`) + 19 GWAM entries — 5 `candidate` on the public website (3 traffic + 2 new anomaly-signal seeds), 14 `deferred` by the single-channel ruling |
| — | [adr/](adr/) | ADR-0001 ingestion (v2) · ADR-0002 models · ADR-0003 Gen-AI · ADR-0004 Akka · ADR-0005 Adaptive ML · ADR-0006 compute plane · ADR-0007 identity & privacy · **ADR-0008 serving topology & Gen-AI plane** |

> **Namespace note:** "D1–D5" in doc 06 are *Mermaid diagram ids* and "D6/D7" in doc 13 continue that
> *diagram/topology* numbering — both are unrelated to doc 16's *standing decisions* **D1–D10** (where
> D6 = AKS serving, D7 = concurrent suites). Same letters, different registries; resolve by source doc.
> Do not renumber either scheme.

## Requirement-coverage map

| Your ask | Where it's addressed |
|---|---|
| **Phase 1 — offline** anomaly detection | [03 §3](03-phase1-anomaly-detection.md), diagram [D2](06-diagrams.md) |
| **Phase 1 — online** detection (scheduled micro-batch) | [03 §4](03-phase1-anomaly-detection.md), diagram [D2](06-diagrams.md) |
| **Phase 2 — offline** investigation & insights | [04 §3](04-phase2-investigation-insights.md), diagram [D3](06-diagrams.md) |
| **Phase 2 — online** triage & ChatOps | [04 §4](04-phase2-investigation-insights.md), diagram [D3](06-diagrams.md) |
| **Lucid diagrams** (reduced, effective set) | [06](06-diagrams.md) D1–D5 — Mermaid, Lucidchart-importable |
| **Both data domains** (Canada Retirement + CoverMe) | [02 §2–3](02-solution-architecture.md), [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md), diagram [D1](06-diagrams.md) |
| **Leverage Gen-AI** | [05 Part A](05-genai-and-akka.md); also [03 §3.3](03-phase1-anomaly-detection.md), [04](04-phase2-investigation-insights.md) |
| **Azure + Databricks stack** | [02 §5](02-solution-architecture.md) |
| **Reference repos** (pyod, darts, resources, ai-data-science-team) | [08](08-library-mapping.md) |
| **Akka migration (later stage)** | [05 Part B](05-genai-and-akka.md), [ADR-0004](adr/adr-0004-akka-migration-strategy.md), diagram [D5](06-diagrams.md) |
| **Adaptive ML partnership** (model tuning/serving + leverage) | [07](07-adaptive-ml-integration.md), [ADR-0005](adr/adr-0005-model-tuning-adaptive-ml.md), diagram [D4](06-diagrams.md) |
| **Critique Gemini & Perplexity** | [01](01-critique-and-synthesis.md) |
| **Identify causes** (deployments, upgrades, outages, trends) | [04 §2(b)](04-phase2-investigation-insights.md) (change-event correlation — gated on key acquisition) |
| **Recommendations / actionable insights** | [04 §5](04-phase2-investigation-insights.md) |
| **Data-profile alignment + blockers** | [10](10-data-profile-alignment.md) |
| **Privacy/identity extension research** (Perplexity & Gemini `*_extending_GMAI.md`) | [11](11-privacy-identity-governance.md) verdicts + design, [ADR-0007](adr/adr-0007-identity-privacy-layer.md) |
| **React/TS AI-BI app for global users** (AKS serving, Foundry Gen-AI) | [13](13-global-serving-topology.md) D6, [ADR-0008](adr/adr-0008-global-serving-and-genai-plane.md) |
| **Unstructured data** (transcripts, PDFs via ADLS Gen2 → RAG) | [13 §4](13-global-serving-topology.md) D7, [ADR-0008](adr/adr-0008-global-serving-and-genai-plane.md) |

## Scope notes & assumptions

- **Both domains in scope:** Canada Retirement (GWAM) + CoverMe — one pipeline, one canonical Adobe hit
  schema, per-domain source bindings ([02 §2](02-solution-architecture.md)). Landing points are per
  `new_data/README.md` and marked **provisional pending data-platform-owner confirmation**.
- **Latency honesty:** detection latency = source feed cadence (daily first). No "real-time" claims; the
  streaming upgrade path and its trigger criteria live in [ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md).
- **Hard gates before build:** ↺ *all cleared 2026-07-29* — production feed acquisition is **done for
  both domains** (GWAM 883 days, CoverMe 1,211 days); the PII review is superseded by the **full-raw
  regime** (ADR-0007 §5), and the CoverMe consent sign-off that was the last outstanding gate is
  **cleared verbally** (Kerrian, 2026-07-29: no PII comes from Adobe; eVar65 is OneTrust *cookie*
  consent, carrying no PII and not an analytics-suppression flag —
  [17 §4 item 9](17-coverme-eda-readiness.md)).
- Open items needing business input: feed refresh cadence/SLA per domain, holiday/campaign calendars, the
  labeled incident set for evaluation ([02 §7](02-solution-architecture.md)), and owners for the **43
  still-ungoverned** entries among the 48 registry metrics (v0.5.0: 29 CoverMe + 19 GWAM; only the 5
  CoverMe funnel events are `active` with an owner, and of the GWAM entries 5 are `candidate` and 14
  `deferred`). Still with the SMEs: GWAM — the `wealth-ca` / `pvt-wealth` brand variants
  ([20](20-gwam-sme-questions.md) Q3b) and page-views-vs-hits (Q6); CoverMe — the identity of events
  510-514, eVar148 bot-detector verification, whether eVar149 becomes the permanent language field, and
  per-date confirmation of the ~30 missing days.
- Diagrams are **Mermaid** (render in GitHub/VS Code; import to Lucidchart) per the agreed format.

> This package is a solutioning blueprint, not running code. Detection logic, thresholds, and Gen-AI prompts
> are illustrative and must be backtested/tuned on real feed history before production.
