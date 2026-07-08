# 10 — Data-Profile Alignment (what the 2026-07-02 profiling changed)

> One-page traceability record for the `new_data/` discovery drop: what it contains, the facts it
> established, and exactly where each fact changed this package. New readers: skim this, then
> [02-solution-architecture.md](02-solution-architecture.md).

## 1. What `new_data/` contains

| Artifact | What it is |
|---|---|
| `README.md` | GMAI-Pulse project README — problem framing, taxonomy, and both domains' **Azure landing points** |
| `data_inventory.md` | Narrative inventory of the profiled corpus |
| `data_profiling_report.md` | Deep, human-readable column-level profile |
| `data_profile_summary.json` | **Machine-readable summary**: per-file ad-readiness, **joinability graph**, grain hints, metric/dimension shortlists — the input for [`metric-registry.yaml`](metric-registry.yaml) |
| `generated_data_profile.json` / `.md` | Full aggregate raw profile (every column, every stat) |

**Corpus reality check:** 5 sheets, 509 rows total — a **10-row Adobe hit-level schema sample** plus the
**4-sheet CoverMe data dictionary** (224 feed columns / 93 eVars / 26 props / 156 events). It is **schema +
dictionary only**; no production time-series exists in the corpus.

## 2. The established facts → where each changed the package

| # | Fact (2026-07-02) | Doc/section changed |
|---|---|---|
| 1 | **Two domains, two platforms:** GWAM → `gwam_prod_catalog.inv_typed_common.adobe_hit_data` (UC/Delta, Databricks SQL); CoverMe → `martech.adobe_coverme.hit_data` (ADLS external table, **Synapse serverless SQL**, which cannot run ML/streaming/monitoring) | **[ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md)** (new); [02 §2/§3/§5](02-solution-architecture.md); [01 reassessment](01-critique-and-synthesis.md); [06 D1](06-diagrams.md); README scope |
| 2 | **Both feeds land as at-rest tables; no streaming collection evidenced** | **[ADR-0001 amended to v2](adr/adr-0001-near-real-time-microbatch.md)** (batch-first; hot lane → Phase 3+ option); [02 §3/§8](02-solution-architecture.md); [03 §4](03-phase1-anomaly-detection.md); [06 D1/D2](06-diagrams.md); [ADR-0004](adr/adr-0004-akka-migration-strategy.md) |
| 3 | **One canonical ~1,198-column Adobe hit schema** for both domains; the dictionary decodes it (95.98% columns, 100% eVar slots, 100% prop slots) → one pipeline, per-domain bindings | [02 §1/§2](02-solution-architecture.md); [03 §3](03-phase1-anomaly-detection.md); [01 reassessment](01-critique-and-synthesis.md) |
| 4 | Data arrives **already typed** — no raw-TSV parsing; ~1,008 of 1,198 columns are empty Adobe slots to prune | [02 §4](02-solution-architecture.md); [03 §3](03-phase1-anomaly-detection.md); [06 D2](06-diagrams.md) |
| 5 | **29 dictionary rows business-tagged "Anomaly Detection"** (9 `data_feed_columns` + 8 `post_eVar` + 12 `post_event_list`) | **[`metric-registry.yaml`](metric-registry.yaml)** (new); [03 §5](03-phase1-anomaly-detection.md); [08 model table](08-library-mapping.md); [02 principle 5](02-solution-architecture.md) |
| 6 | **No metric columns exist** — KPIs are downstream aggregates of decoded `post_event_list` events (`Purchase`, `Product View`, `Cart Open/Add/Remove`, …) | [02 §2](02-solution-architecture.md); [03 §2/§7](03-phase1-anomaly-detection.md) |
| 7 | **Hit timestamps fully populated** (`hit_time_gmt`, `date_time`, `post_cust_hit_time_gmt` at 0% null) → intraday possible, but only **daily** is defensible with no volume history | [03 §1 gate](03-phase1-anomaly-detection.md); registry `grain` field; [02 §2](02-solution-architecture.md) |
| 8 | **24 sensitive/PII columns** flagged (`ip`, `cookies`, `visid_*`, visitor counters, `geo_zip`, `userid`, …) | [02 §9 hard gate](02-solution-architecture.md); Silver PII boundary in [03 §3](03-phase1-anomaly-detection.md); disposition table in [11 §2](11-privacy-identity-governance.md); [ADR-0007](adr/adr-0007-identity-privacy-layer.md) |
| 9 | **11 expected correlation keys missing** (`url`, `page_path`, `deployment_id`, `release_id`, `campaign_id`, `content_id`, `tag_name`, `owner`, `site_id`, `brand`, `region`) + zero change-event sources in the corpus | [04 §2b caveat](04-phase2-investigation-insights.md); [02 §10](02-solution-architecture.md); [06 D3 caveat](06-diagrams.md) |
| 10 | Dictionary-quality nits: `Status` mixed-case (`Enabled`/`ENABLED`); `post_eVar.Name` not unique; ~10 events missing `Friendly Name`; wide-integer lookup-ID columns falsely flagged as metrics/timestamps | [02 §2 DQ gates](02-solution-architecture.md); [05 A.1](05-genai-and-akka.md); [03 §3](03-phase1-anomaly-detection.md) |
| 11 | **ad_readiness: nothing is detection-ready** — every profiled file is enrichment/lookup; the sample is volumetrically disqualified (10 rows) | [03 §1 data gate + §5 pilot ordering](03-phase1-anomaly-detection.md); [02 §7 gate](02-solution-architecture.md) |

Also: the ten-diagram set was consolidated to **five** ([06-diagrams.md](06-diagrams.md) D1–D5), aligning
with the 4–5-core-diagram consensus in the prior research.

## 3. Open blockers (own these before build)

1. **Production feed acquisition (#1 blocker).** Source ≥30 (ideally 90) days of hit-level feed per domain.
   Nothing model-related can be validated until this lands ([03 §1](03-phase1-anomaly-detection.md)).
2. **PII data-classification review.** Formal review of the 24 flagged columns with the data-platform
   owners; ratify the **pseudonymize/drop/generalize** disposition per column **before any production
   ingestion** — the review feeds the Identity & Privacy layer
   ([02 §9](02-solution-architecture.md); [11 §2](11-privacy-identity-governance.md);
   [ADR-0007](adr/adr-0007-identity-privacy-layer.md)).
3. **Metric-registry extraction.** One pandas pass over the dictionary workbook's `Notes` columns to fill
   the 29 placeholder entries in [`metric-registry.yaml`](metric-registry.yaml) (procedure embedded in the
   file header). The profiles confirm counts; the workbook holds the row identities.
4. **Correlation-key + change-event acquisition (Phase-2 entry criterion).** Add the 11 missing keys and
   onboard deployment/release logs, tag-change history, campaign calendars ([04 §2b](04-phase2-investigation-insights.md)).
5. **Storage-permission alignment for CoverMe.** Managed-identity read on the CoverMe ADLS container for
   the UC external location ([ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md)).
6. **Confirm landing points with data-platform owners.** The `new_data/README.md` marks both landing
   descriptions as provisional; ADR-0006 assumes them.
7. **No person-level identifier exists** (full-database check with the data owner, 2026-07-04):
   `cust_visid`/`post_cust_visid` are **completely NULL on all rows**, and `userid` is a **single
   constant value on every row** (account-level, cardinality 1). Identity stitching is therefore
   **deferred and gated** ([ADR-0007](adr/adr-0007-identity-privacy-layer.md)); standing **MarTech
   ask**: capture the login/customer ID in a dedicated eVar ([11 §3](11-privacy-identity-governance.md)).
8. **No feed-cadence/SLA evidence.** The corpus contains no statement of per-domain feed cadence or
   latency SLA; the daily-first grain (ADR-0001 v2) and stakeholder alert-timeliness expectations both
   need this confirmed with the platform owners.

## 4. Known discrepancies in the source artifacts

- The **inventory narrative** claims hit timestamps are missing; the **profiling report/JSON** show them
  fully populated. The machine profile wins — timestamps exist (fact 7).
- The profiler counts **190 populated columns** in the 10-row sample; the inventory narrative says "26
  business-relevant." Treat 190 as machine truth; the narrower set is a business filter.
- `data_feed_columns.Description` is flagged sensitive — a **false positive** (long free-text tripped the
  sniffer; it is documentation, not PII).
