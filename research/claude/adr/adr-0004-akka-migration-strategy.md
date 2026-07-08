# ADR-0004 — Akka migration strategy (future runtime)

**Status:** Accepted (future-dated) · **Date:** 2026-06-30 · **Deciders:** GMAI–Pulse solutioning

## Context
Manulife publicly **selected Akka to operationalize agentic AI**; GMAI–Pulse must plan a migration once the
Databricks solution is validated. Akka is **JVM-only**; our ML stack is Python (`pyod`/`darts`/MLflow).
Gemini's draft claimed Akka gives "exactly-once execution" and implied porting models — both need correction.

## Decision
- **Keep Databricks** as the **data/training/feature plane and system-of-record**. Move only the **online
  detection watchdog** and **Phase-2 agents** to Akka as **durable agents / workflows / event-sourced
  entities**.
- **Python ↔ JVM boundary:** serve models behind **HTTP/gRPC** (MLflow Model Serving / Azure ML / FastAPI);
  Akka calls them from **Workflows/Consumers** (not inside an agent's code path). gRPC for low-latency;
  Kafka for async scoring only if the Phase-3+ hot lane exists ([ADR-0001 v2](adr-0001-near-real-time-microbatch.md)).
- **Migration = strangler-fig:** (1) dual-run/shadow on the same Gold increments and alert triggers → (2)
  shadow-validate vs. the Databricks scheduled-job output → (3) per-feature cutover via routing weight →
  (4) consolidate — retire interim Databricks-hosted agent/watchdog services (batch/training/data plane stays).
- **Correctness model:** design for **durable execution + at-least-once steps + idempotency/compensation
  (saga)** — **not** literal exactly-once (per Akka's own docs). Make external actions idempotent.

## Consequences
- (+) Low-risk, reversible (rollback = flip routing weight); leverages Akka governance (intent/interaction
  logging, OPA) for Responsible AI.
- (+) No ML rewrite — clean service boundaries designed in Phase 1/2 carry over.
- (−) Cross-language remote-serving hop adds latency/ops; idempotency must be engineered, not assumed.

## Alternatives rejected
- **Rewrite `pyod`/`darts` in JVM** — high cost/risk, no benefit.
- **Big-bang cutover** — unacceptable risk in production.
- **Never migrate** — viable interim, but forgoes the enterprise Akka agentic platform Manulife selected.
