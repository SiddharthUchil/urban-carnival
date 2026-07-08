# ADR-0006 — Unified Databricks compute plane over two source platforms

**Status:** Accepted · **Date:** 2026-07-02 · **Deciders:** GMAI–Pulse solutioning

## Context
`new_data/` discovery (2026-07-02) established that the two in-scope domains land on **different platforms**:

| Domain | Landing | Query surface | Fully-qualified table |
|---|---|---|---|
| Canada Retirement (GWAM) | ADLS Gen2 → Delta/Parquet → Unity Catalog | Databricks SQL | `gwam_prod_catalog.inv_typed_common.adobe_hit_data` |
| CoverMe | ADLS Gen2 → external table/view | **Synapse serverless SQL** | `martech.adobe_coverme.hit_data` |

Both are the **same canonical Adobe Analytics data-feed hit schema** (~1,198 columns); the CoverMe data
dictionary decodes it with 95.98% column coverage and 100% eVar/prop slot coverage, so one pipeline and one
enrichment layer can serve both. But **Synapse serverless SQL cannot host the detection stack**: it runs no
Structured Streaming, no DLT/Lakeflow, no Lakehouse Monitoring, and no Python ML. The prior package
implicitly assumed CoverMe was on Databricks; it is not.

## Decision
**Databricks is the single detection/ML compute plane for both domains.** CoverMe data reaches it without
disturbing the existing Synapse surface:

- **Option A (primary): Unity Catalog external location over CoverMe's ADLS Gen2 files.** A UC storage
  credential + external location is registered over the *same* ADLS files that back the Synapse serverless
  external table; multiple engines reading one external location is a supported, safe pattern
  ([external locations on ADLS](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-adls),
  [external tables](https://learn.microsoft.com/en-us/azure/databricks/tables/external)).
- **Option B (interim/fallback): Lakehouse Federation.** A UC connection of `TYPE sqldw` exposes the Synapse
  database as a foreign catalog for federated queries
  ([query-federation/sqldw](https://learn.microsoft.com/en-us/azure/databricks/query-federation/sqldw)).
  Useful for early exploration or if direct file access is blocked by storage permissions.

**Synapse serverless SQL remains untouched** as CoverMe's existing serving/query surface for current
consumers. GMAI-Pulse adds a reader, not a migration.

## Consequences
- (+) **One pipeline, one metric registry, one enrichment layer** for both domains — per-domain
  configuration, not per-domain code ([02 §3](../02-solution-architecture.md)).
- (+) Lakehouse Monitoring, darts/pyod, MLflow, and Mosaic AI ([ADR-0002](adr-0002-model-family-selection.md),
  [ADR-0003](adr-0003-genai-platform-and-guardrails.md)) apply to **both** domains once data lands in the
  Databricks medallion.
- (+) The Identity & Privacy layer (24 flagged columns — [ADR-0007](adr-0007-identity-privacy-layer.md))
  is enforced once, at the Databricks ingestion boundary ([02 §9](../02-solution-architecture.md)).
- (−) **Residual Synapse governance gap** (noted 2026-07-04): "remains untouched / adds a reader" holds
  for GMAI-Pulse, but Synapse **serverless** does not support DDM/RLS on external tables — existing
  *direct* consumers see raw columns. Recommendation (secure views + `IS_MEMBER()`, owned by the
  CoverMe platform team): [ADR-0007](adr-0007-identity-privacy-layer.md),
  [11 §4](../11-privacy-identity-governance.md).
- (−) **Dual-engine schema drift risk** on the shared CoverMe files (Synapse DDL and UC table definition can
  diverge). Mitigation: schema contract tests in the Bronze ingest job; alert on drift.
- (−) Federation (Option B) has pushdown/perf limits on wide hit-level scans. Mitigation: prefer Option A;
  use B only for exploration or as a stopgap.
- (−) Requires storage-permission alignment with the CoverMe data-platform owners (managed identity read on
  the ADLS container) — an engagement dependency, not a technical one.

## Alternatives rejected
- **Run detection on Synapse serverless** — impossible: no ML runtime, no streaming, no monitoring.
- **Two per-platform pipelines** — doubles build/run cost for identical schemas; forks the metric registry
  and thresholds; contradicts the profiling evidence that both feeds are one subject area.
- **Physical copy into Databricks managed tables** — extra storage + latency + sync jobs; revisit only if
  external-location read performance proves insufficient.
