# ADR-0005 — Adaptive ML (Adaptive Engine) as the model tuning & serving layer

**Status:** Accepted · **Date:** 2026-06-30 · **Deciders:** GMAI–Pulse solutioning
**Supersedes in part:** [ADR-0003](adr-0003-genai-platform-and-guardrails.md) (model layer — orchestration &
guardrails there still hold)

## Context
[ADR-0003](adr-0003-genai-platform-and-guardrails.md) chose Mosaic AI / Azure AI Foundry for agent
orchestration and frontier (Claude/GPT) models, tiered by difficulty. Manulife has since publicly
**selected Adaptive ML** (PR Newswire, Dec 22, 2025) as the **reinforcement-learning engine to fine-tune,
evaluate, and serve open-weight SLMs** for its enterprise AI platform — alongside **Akka** (agentic runtime,
Mar 2026). The Phase-2 RCA/insights agents are exactly the "specialist, high-volume, cost-sensitive,
data-resident" workload Adaptive Engine targets, and we already designed an analyst-feedback loop that is a
ready-made reward signal.

## Decision
- **Adopt Adaptive Engine as the model tuning + serving layer** for the Phase-2 Gen-AI agents (Narrative,
  Runbook Advisor, Data/SQL) and the Gen-AI bits of Phase-1.
- **Hybrid model strategy** (per Manulife CAIO): **Adaptive-tuned open SLMs as the production default**;
  **frontier LLMs (Claude/GPT) for cold-start, hardest cases, and as teachers** for synthetic data / reward
  judging.
- **Feedback-as-reward:** wire the `analyst_feedback` loop ([04 §5](../04-phase2-investigation-insights.md))
  into Adaptive Engine's **Metrics-logging API**; optimize measurable targets (analyst acceptance,
  groundedness, MTTR) via online iterative RL (PPO/GRPO/DPO).
- **Keep all guardrails** ([ADR-0003](adr-0003-genai-platform-and-guardrails.md)) — tuning **complements**
  grounding + atomic-claim verification + JSON contracts + HITL; it does not replace them.
- **Serve in private env (VPC/on-prem)** for PIPEDA residency; register/evaluate via MLflow; expose endpoints
  the (future Akka) agents call over gRPC/HTTP ([ADR-0004](adr-0004-akka-migration-strategy.md)).

## Consequences
- (+) Lower cost/latency, data residency, and a **self-improving flywheel** — accuracy compounds from
  analyst feedback rather than plateauing at a fixed prompt.
- (+) Aligns GMAI–Pulse with Manulife's actual **Databricks → Adaptive ML → Akka** platform.
- (−) Requires a feedback corpus before tuning pays off → **cold-start on frontier first**, then distill/tune.
- (−) New failure modes — **reward hacking, feedback drift** — needing multi-dimensional rewards, held-out
  evals, and monitoring.

## Alternatives rejected
- **Frontier-API-only** (original ADR-0003 stance) — higher recurring cost, weaker domain specialization,
  cross-border data concerns, and ignores Manulife's chosen platform.
- **Build our own RLOps** (raw TRL/Ray on Databricks) — duplicates what Adaptive Engine (and the Manulife
  agreement) already provide; slower, more maintenance.
- **Fine-tune via a generic cloud fine-tuning API** — lacks the production-feedback RL flywheel and the
  private-environment serving Manulife selected Adaptive ML for.
