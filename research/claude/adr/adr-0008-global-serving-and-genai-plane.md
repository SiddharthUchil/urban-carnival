# ADR-0008 — Global serving topology & Gen-AI plane (React/AKS + Azure AI Foundry + AI Search)

**Status:** Accepted · **Date:** 2026-07-09 · **Deciders:** GMAI–Pulse solutioning

## Context
Three enterprise facts arrived after the package was finalized: Manulife's existing **React/TypeScript
AI/BI app** (target hosting **AKS**) must be the business-user surface; **Azure AI Foundry** is the
enterprise standard for all Gen-AI use cases; and scope now includes **unstructured data**
(call/chat transcripts + feedback, policy/product PDFs) landing in **ADLS Gen2**, with **global**
Manulife users consuming the results. [ADR-0003](adr-0003-genai-platform-and-guardrails.md) chose
Databricks Mosaic AI as the primary Gen-AI platform with "Foundry Agent Service + Azure AI Search" as
the named Azure-plane alternative; docs [02 §5](../02-solution-architecture.md) named Databricks AI/BI
dashboards + Power BI as the BI surface. The Phase-1 medallion + detect job (built under `databricks/`)
runs daily and writes `gold.kpi_daily`, `gold.anomalies`, `gold.run_meta`.

## Decision
1. **Business-user surface = the enterprise React/TS app + a Node/TS BFF on AKS.** The BFF is the only
   trust boundary: Entra Workload Identity → least-privilege service principal with UC grants on
   `gmai_pulse_gold` only; Statement Execution API reads over a serverless SQL warehouse; responses
   cached keyed on `run_meta.run_id` (data changes once daily). Databricks SQL dashboards remain
   analyst/engineering-internal. Triage write-back goes through the BFF, never direct client writes.
2. **Gen-AI plane (primary) = Azure AI Foundry** — the Azure-plane alternative in ADR-0003 is promoted
   to primary (Agent Service + Responses API; Foundry model deployments incl. embeddings). **All
   ADR-0003 guardrail, evaluation, cost, and audit requirements carry over unchanged.** Adaptive ML
   (ADR-0005) remains the later tuning/serving loop behind the same BFF interface.
3. **RAG store = Azure AI Search** (hybrid + semantic ranking), replacing Mosaic AI Vector Search in the
   primary stack. The index is a rebuildable **projection** of a governed gold chunk Delta table —
   lineage, access control, and erasure stay in UC/Delta.
4. **Unstructured lane runs on Databricks** (ADR-0006 single compute plane holds): ADLS Gen2 landing →
   Auto Loader `availableNow` (batch-first per ADR-0001) → bronze manifest → **PII redaction before
   silver** (ADR-0007 extension; transcripts are the most sensitive source) → chunked gold → AI Search.
   Sources are declared in a `corpus-registry.yaml` (config-not-code, mirrors `metric-registry.yaml`).
5. **Global reach = single Canadian data region + Azure Front Door edge** (WAF, TLS, edge cache, Entra
   SSO). No data leaves Canada ([11 §5](../11-privacy-identity-governance.md)); daily-batch cadence +
   `run_id` caching makes single-region serving viable worldwide. Multi-region data planes are a later
   per-domain extension, not part of this decision.

Full topology and phasing: [13-global-serving-topology.md](../13-global-serving-topology.md) (D6/D7).

## Consequences
- (+) One Gen-AI governance plane (Foundry) matching enterprise standards; one visualization surface
  (the existing app) instead of three; UC remains the single data-governance root.
- (+) Frontend/BFF is stack-native (TypeScript) for the existing app team; Databricks pipeline needs no
  changes to be served.
- (−) Amends ADR-0003's platform choice; Mosaic AI Agent Evaluation / MLflow-native agent tracing must be
  replaced with Foundry evaluation + tracing equivalents (guardrail *requirements* unchanged, tooling remaps).
- (−) Two planes to keep in sync for RAG (Delta chunk table ↔ AI Search index); mitigated by treating the
  index as a rebuildable projection with the sync inside the daily job.
- (−) BFF is new operational surface (AKS deploy, cache invalidation, authZ tests) owned outside Databricks.
- (−) Genie/NL-over-tables capabilities are foregone in v1 (single-plane rule); free-text analytics waits
  for guard-railed Foundry NL→SQL (v2).

## Alternatives rejected
- **Databricks Apps hosting the frontend** — governance-simple but duplicates the existing enterprise app
  and team stack; kept as fallback if AKS hosting stalls.
- **AI/BI Genie embedding for NL analytics** — governed text-to-SQL, but violates the single-Foundry
  Gen-AI plane mandate; revisit only if that mandate changes.
- **Mosaic AI Vector Search as primary RAG store** — lakehouse-native, but Foundry-side retrieval
  (grounding, "on your data", agent tools) integrates natively with AI Search and the mandate.
- **Multi-region data planes now** — no non-Canadian feed exists yet; premature cost/complexity.
- **Per-request model inference for predictions** — anomalies need history; batch precompute + optional
  parameterized job re-run is simpler and matches ADR-0001.
