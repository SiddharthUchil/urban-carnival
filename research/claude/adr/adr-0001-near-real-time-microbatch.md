# ADR-0001 — Near-real-time = micro-batch over at-rest tables (batch-first; streaming hot lane deferred)

**Status:** Accepted — Amended 2026-07-02 (v2) · **Date:** 2026-06-30 · **Deciders:** GMAI–Pulse solutioning

## Context
The README requires an **online (near-real-time)** and **offline (batch)** path for both phases. The prior
Gemini/Perplexity drafts implied the online path could run off "Adobe RT APIs" or "DLT continuous." Our
verified research shows the **classic Adobe Analytics Data Feed is batch** — delivered hourly/daily with
"several hours … up to 12 hours or more" latency and no SLA. Sub-10-minute data requires a different
collection path (**Edge Network**). Separately, `trigger(continuous)` is **experimental/unsupported on
Databricks**.

**New evidence (2026-07-02, `new_data/` profiling):** both in-scope feeds land as **at-rest tables**, not
streams — Canada Retirement (GWAM) as `gwam_prod_catalog.inv_typed_common.adobe_hit_data` (Delta/Parquet on
ADLS Gen2, Unity Catalog, Databricks SQL) and CoverMe as `martech.adobe_coverme.hit_data` (ADLS Gen2
external table, Synapse serverless SQL). No Edge Network / Event Hubs collection is evidenced anywhere in
the discovery corpus, and the corpus itself contains **no time-series data** (schema + dictionary + a
10-row sample only). Designing a streaming hot lane ahead of any demonstrated latency requirement would be
speculative infrastructure.

## Decision
**Batch-first micro-batch.** The Phase-1 online mode is **scheduled micro-batch Databricks jobs over the
at-rest ADLS/Delta tables** (per [ADR-0006](adr-0006-unified-databricks-compute-plane.md)), running at the
cadence the source feed actually refreshes — **daily grain first**, tightening toward hourly as feed
cadence and volume are confirmed. What v1 called the "degraded fallback" is now the primary — and only —
Phase-1 mode.

The **Edge Network → Event Forwarding → Event Hubs → Structured Streaming hot lane is deferred** to an
explicitly optional **Phase-3+ upgrade**, to be triggered only when **both** hold:
1. A business latency SLA is confirmed that is tighter than the source feed cadence, **and**
2. A ≥30-day (ideally 90-day) production hit-level feed confirms intraday volume sufficient for stable
   intraday baselines ([03 §1](../03-phase1-anomaly-detection.md)).

"Real-time" language is retired from Phase-1 scope: the honest latency floor equals the **source feed
refresh cadence**. If the hot lane is later built, its target is single-digit minutes, explicitly not
sub-second before the Akka phase ([ADR-0004](adr-0004-akka-migration-strategy.md)).

## Consequences
- (+) **No streaming infrastructure cost or ops burden in Phase 1** — no Event Hubs, no always-on clusters.
- (+) One code path (scheduled jobs) instead of two lanes plus a reconciliation step.
- (+) The design matches the evidenced reality of both sources; nothing is promised that the feeds cannot deliver.
- (−) Detection latency is bounded by feed cadence (hours, not minutes) until/unless the hot lane is built.
- (−) The hot-lane option must be kept genuinely open: table schemas and the metric registry
  ([03](../03-phase1-anomaly-detection.md), `metric-registry.yaml`) are designed grain-agnostic so intraday
  can be enabled without rework.

## Alternatives rejected
- **Data Feed as a real-time source** — impossible (hours of latency, no SLA).
- **`trigger(continuous)`** — unsupported on Databricks.
- **Building the Edge/Event Hubs hot lane now** — speculative: no evidenced latency requirement, no
  streaming collection in place, and no production feed to validate intraday baselines against.
- **True sub-second now** — deferred to Edge + Akka runtime ([ADR-0004]).

## Changelog
- **v1 (2026-06-30):** Adopted a two-lane (lambda) architecture — Edge Network hot lane (minutes) +
  Data Feed cold lane (authoritative). Reflected the assumption that CoverMe/GWAM collection could be
  upgraded to Edge streaming in Phase 1.
- **v2 (2026-07-02):** Amended to **batch-first micro-batch over at-rest tables**; hot lane demoted to an
  optional Phase-3+ upgrade with explicit trigger criteria. Drivers: `new_data/` profiling showed both
  feeds land as at-rest tables on two platforms (see [ADR-0006](adr-0006-unified-databricks-compute-plane.md)),
  no streaming infra evidenced, and no time-series data yet exists to justify intraday detection. See
  [10-data-profile-alignment.md](../10-data-profile-alignment.md).
