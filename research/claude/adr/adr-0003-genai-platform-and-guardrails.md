# ADR-0003 — Gen-AI platform & guardrails for RCA

**Status:** Accepted — platform choice amended by [ADR-0008](adr-0008-global-serving-and-genai-plane.md) (2026-07-09: the Azure-plane alternative — Foundry + AI Search — is promoted to primary; guardrails/eval requirements unchanged) · **Date:** 2026-06-30 · **Deciders:** GMAI–Pulse solutioning

## Context
Phase 2 needs a multi-agent **RCA + recommendations** layer with retrieval over historical incidents/runbooks,
in a **regulated** (insurance, PIPEDA) setting where a hallucinated root cause is worse than none. The prior
drafts described agents but specified **no guardrails, evaluation, or platform decision**.

## Decision
- **Platform (primary):** **Databricks Mosaic AI Agent Framework** (LangGraph-style tool-calling agents,
  MLflow Tracing, **Agent Evaluation** groundedness/correctness judges) + **Mosaic AI Vector Search** (hybrid
  + reranking) for RAG — lakehouse-native, least data movement.
- **Platform (Azure-plane alternative):** **Azure AI Foundry Agent Service** + Responses API + Azure AI Search.
  *Do not build on the retiring Azure Assistants API.*
- **Models:** Claude- and GPT-class on Foundry/Databricks; **tier by difficulty** (small for triage, large for
  hard RCA). Model IDs are time-sensitive — confirm at build time.
- **Guardrails (mandatory):** strict **grounding** on retrieved evidence + **atomic-claim verification** with
  **abstention**; **structured JSON** output validated against the `anomaly_insights` schema; **confidence
  gating**; **human-in-the-loop** before any external action; **deterministic fallback** (evidence bundle) on
  agent/LLM failure; full **prompt/evidence/response audit logging**.
- **Cost:** invoke RCA **only on confirmed, severe anomalies**; cache system prompts; **Batch API** for offline
  reports.

## Consequences
- (+) Governable, auditable, evaluable RCA aligned to Responsible AI; compounding institutional memory.
- (−) Some capabilities are in Public Preview; preview/model changes must be tracked.
- (−) Guardrail + eval machinery adds engineering surface (worth it for trust).

## Alternatives rejected
- **Single ungrounded LLM prompt** — hallucination risk unacceptable in a regulated domain.
- **Azure Assistants API** — being retired.
