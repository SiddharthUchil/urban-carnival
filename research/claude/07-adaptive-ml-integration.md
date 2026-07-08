# 07 — Adaptive ML Integration (the model-tuning layer)

> **Why this doc exists.** After the base solution was written, Manulife publicly **selected Adaptive ML**
> (PR Newswire, **Dec 22, 2025**) as the **reinforcement-learning engine to fine-tune, evaluate, and serve**
> models for its enterprise AI platform — complementing its **Akka** selection (Mar 10, 2026, agentic
> runtime). This changes the **Gen-AI model layer** of GMAI–Pulse. Detection (`darts`/`pyod`/monitoring) is
> unaffected. Read with [05-genai-and-akka.md](05-genai-and-akka.md); decision in
> [ADR-0005](adr/adr-0005-model-tuning-adaptive-ml.md); diagram D4 in [06-diagrams.md](06-diagrams.md);
> sources in [09-references.md](09-references.md).

## 1. What Adaptive ML is (grounded)

**Adaptive ML** is an independent NY company (founded 2023 by the ex-Hugging Face / Falcon / BLOOM team;
$20M seed led by Index Ventures, **incl. Databricks Ventures**). Its product, **Adaptive Engine**, is an
end-to-end **RLOps** ("Reinforcement Learning Operations") platform — *"the flywheel for enterprise AI:
evaluate, tune, and serve the best LLMs for your business"* — that lets enterprises **outperform frontier
models on their tasks with reinforcement fine-tuning** of **open-weight SLMs**. Capabilities (from the
Adaptive Engine page):

- **Tune** — one-click **PPO, GRPO, DPO**; full-model or adapter tuning; "if your business measures it,
  Adaptive Engine can optimize it."
- **Learn efficiently** — synthetic-data recipes; learn from AI **and** human feedback; **distill** large-model
  capability into smaller, cheaper models.
- **Reasoning** — automated reasoning tuning + test-time search.
- **Refine with production feedback** — a **Metrics-logging API** to **directly optimize business KPIs**,
  improving the model continuously as operations evolve.
- **Evaluate** + **Serve** — personalized evals before deploy; serving in the enterprise's **private
  environment**.

## 2. The Manulife platform: three complementary layers

Manulife is assembling one enterprise AI platform (beta; targeting **$1B+ value by 2027**) from layers that
map cleanly onto GMAI–Pulse:

| Layer | Manulife's choice | GMAI–Pulse role |
|---|---|---|
| **Data / training / detection** | **Databricks** (+ Azure) | Lakehouse, features, `darts`/`pyod` detection, monitoring ([02](02-solution-architecture.md), [03](03-phase1-anomaly-detection.md)) |
| **Model tuning / serving (RLOps)** | **Adaptive ML / Adaptive Engine** (Dec 2025) | Tune + serve the Phase-2 RCA/insight **SLMs**; continuous RL from analyst feedback (this doc) |
| **Agentic runtime** | **Akka** (Mar 2026) | Durable, governed agent orchestration ([05 Part B](05-genai-and-akka.md)) |

Manulife's CAIO **Jodie Wallis**: *"In addition to using LLMs for certain use cases, the targeted use of
specialist language models tuned on this platform will seek to provide both accuracy and cost efficiency
benefits."* Adaptive's CEO **Julien Launay**: *"…deploying its own specialized agents using Adaptive Engine
as the strategic reinforcement learning operations layer… throughout their private environments worldwide."*
This **validates a hybrid model strategy** and **private/VPC deployment** (PIPEDA-friendly) — which we adopt.

## 3. How it changes the GMAI–Pulse suggestions

**Unchanged:** anomaly **detection** (`darts`, `pyod`, Lakehouse Monitoring) — not LLM work. **Changed:** the
**Gen-AI brain** (Phase-2 RCA + the Gen-AI bits of Phase-1).

1. **Model layer (was: frontier API only).** The production workhorse for the **Narrative**, **Runbook
   Advisor**, and **Data/SQL** agents becomes an **Adaptive-tuned open SLM** served in Manulife's private
   environment. **Frontier LLMs (Claude/GPT) are used for cold-start, the hardest RCA, and to generate/judge
   reward data** — exactly the Wallis hybrid.
2. **Feedback loop → reward signal (the standout synergy).** The `analyst_feedback` loop we already designed
   (analysts confirm/dismiss RCA reports — [04 §5](04-phase2-investigation-insights.md)) is **precisely the
   preference/reward data Adaptive Engine consumes via its Metrics-logging API**. GMAI–Pulse becomes a
   **self-improving flywheel** (diagram D4): every triaged anomaly makes the next RCA better. We optimize
   *measurable* targets — analyst acceptance rate, groundedness, MTTR — directly.
3. **Guardrails complement, not replace.** Tuning improves grounding, on-brand tone, domain reasoning, and
   "refuse-when-unsure" behavior, **but** retrieval grounding + atomic-claim verification + JSON contracts +
   HITL stay. All three layers are needed; tuning reduces how often guardrails must fire.
4. **Cost / latency / residency.** A tuned SLM (e.g., 7–8B class) typically runs **~50–80% cheaper**, at
   **sub-second** latency, and **in-VPC/on-prem** — concretizing [ADR-0003](adr/adr-0003-genai-platform-and-guardrails.md)'s
   model-size tiering and satisfying PIPEDA data-residency.
5. **MLOps alignment.** Reward models + tuned SLMs are versioned/evaluated (MLflow; Adaptive ML is
   Databricks-Ventures-backed and serves into the same private cloud) and exposed as endpoints the
   (future Akka) agents call over gRPC/HTTP — consistent with [ADR-0004](adr/adr-0004-akka-migration-strategy.md).

## 4. RLOps workflow applied to our RCA agents

```
cold-start (frontier LLM + grounding + guardrails)        ← Phase-2 ships here (no labels yet)
   │  collect analyst confirm/dismiss + edits  (Metrics-logging API)
   ▼
SFT on accepted reports  →  reward model from preferences  →  RL tune (PPO/GRPO/DPO)
   │  evaluate (Adaptive evals + our PR-AUC/groundedness judges)  →  promote if it beats incumbent
   ▼
serve tuned SLM in private env  →  keep logging feedback  →  periodic RL refresh (online iterative)
```

- **Synthetic data + distillation** bootstrap the SLM from the frontier model's RCA behavior before enough
  real labels exist (addresses the cold-start gap).
- **Reinforcement fine-tuning with verifiable rewards (RLVR)** suits the parts of RCA that are checkable
  (did the cited evidence support the claim? did the recommended action resolve the incident?).

## 5. Hybrid model strategy (which task runs on what)

| Agent / task | Default | Escalation / bootstrap |
|---|---|---|
| Online triage narrative | **Adaptive-tuned SLM** (fast, cheap, in-VPC) | Frontier on low-confidence |
| Offline RCA narrative | **Adaptive-tuned SLM** | Frontier for novel/critical incidents |
| Data/SQL agent (text-to-SQL) | **Adaptive-tuned SLM** (AT&T precedent: "AskData") | Frontier for hard query synthesis |
| Runbook/recommendation | **Adaptive-tuned SLM** (tuned on past resolutions) | Frontier cold-start |
| Reward-data generation / judging | — | **Frontier LLM** (teacher) |

**Precedent:** AT&T deployed Adaptive Engine to tune **text-to-SQL** ("AskData"), customer support, and RAG —
directly analogous to our Data/SQL and Narrative agents.

## 6. Risks & mitigations

| Risk | Mitigation |
|---|---|
| **Reward hacking** (e.g., verbosity over accuracy) | Multi-dimensional reward (groundedness + accuracy + brevity + analyst-accept); adversarial eval |
| **Feedback bias / drift** | Held-out labeled test set; periodic re-eval; meta-reward monitoring; analyst-calibration |
| **Cold-start (no labels)** | Ship on frontier + grounding first; distill/synthetic-data bootstrap; tune once feedback accrues |
| **Over-trusting a tuned model** | Keep grounding + guardrails + HITL regardless of model; gate by confidence |
| **Vendor/platform lock-in** | Open-weight base models + MLflow registry keep exit options open |

## 7. Phased roadmap

1. **P2 launch** — frontier LLM + RAG grounding + guardrails; **turn on the Metrics-logging feedback capture**
   from day one. (Sequenced after the Phase-1 data-acquisition gate clears — no RCA before a real feed and
   detection baselines exist, [03 §1](03-phase1-anomaly-detection.md).)
2. **Accrue feedback** — analyst confirm/dismiss/edits become the preference corpus.
3. **Tune** — SFT + reward model + RL on Adaptive Engine; evaluate vs frontier incumbent on our metrics.
4. **Serve** the tuned SLM in private env as default; frontier on escalation; **continuous online RL refresh**.
5. **Akka** — when the runtime migrates ([05 Part B](05-genai-and-akka.md)), agents call the Adaptive-served
   endpoints over gRPC; the flywheel keeps running.

## 8. Bottom line

Adaptive ML doesn't replace our architecture — it **upgrades the Gen-AI layer from "rent a frontier model" to
"own a continuously-improving specialist model,"** turns the analyst-feedback loop we already designed into a
**training asset**, and slots GMAI–Pulse cleanly into Manulife's real **Databricks → Adaptive ML → Akka**
platform. See [ADR-0005](adr/adr-0005-model-tuning-adaptive-ml.md).
