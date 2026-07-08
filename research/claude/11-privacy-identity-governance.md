# 11 — Privacy, Identity & Governance (PIPEDA / Law 25 / Bill C-27)

> Extends [02 §9](02-solution-architecture.md) into a full privacy/identity design. Anchored by
> [ADR-0007](adr/adr-0007-identity-privacy-layer.md) (Identity & Privacy layer, keyed pseudonymization,
> stitching deferred). Related: the Silver boundary in [03 §3](03-phase1-anomaly-detection.md), the
> open blockers in [10 §3](10-data-profile-alignment.md), and the compute-plane split in
> [ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md).

## 1. Why this doc — and verdicts on the extension research

Two deep-research extensions (`research/perplexity/perplexity_extending_GMAI.md`,
`research/gemini/gemini_extending_GMAI.md`) proposed widening scope around identity, privacy, and
governance. Assessed against the profiled data and the accepted ADRs:

| Proposal | Verdict | Rationale |
|---|---|---|
| Named Identity & Privacy layer in ingestion | **Adopted** | Refines the existing Silver PII gate; §2, ADR-0007 |
| Keyed pseudonymization instead of "hash or drop" | **Adopted** | Preserves joinability + future stitching option; ADR-0007 |
| Synapse serverless masking workaround (secure views) | **Adopted** (recommendation to platform owners) | Real platform gap; §4 |
| PIPEDA + Quebec Law 25 + Bill C-27 mapping | **Adopted** | §5 |
| Right-to-erasure design | **Adopted** (design-forward) | §6 |
| Full identity-stitching pipeline (replay engine) | **Deferred, gated** | No person-level ID exists in the data; §3 |
| External environment signals (macro/regulatory) | **Deferred to Phase 3** | Noted in [04 §2b](04-phase2-investigation-insights.md); Phase-2 correlation is already gated on 11 missing keys |
| Real-time ingestion tier (Event Hubs) | **Rejected** | [ADR-0001 v2](adr/adr-0001-near-real-time-microbatch.md) batch-first stands; streaming remains trigger-gated |
| HSM storage tiering; "4.7× real-time value" claim | **Rejected** | Generic / weakly sourced; see [09](09-references.md) caveats |

## 2. The Identity & Privacy layer (Bronze→Silver)

One enforcement point, applied identically to both domains on the Databricks plane (per ADR-0006).
Every column of the 24 flagged by the profiler ([10 §2](10-data-profile-alignment.md), fact 8) gets an
explicit disposition — **pseudonymize, drop, or generalize** — ratified by the data-classification
review (still the hard pre-ingestion gate from [02 §9](02-solution-architecture.md)):

| Column class | Examples | Default disposition | Why |
|---|---|---|---|
| Visitor/device identifiers | `mcvisid`, `post_visid_high/low`, `cookies`, `persistent_cookie` | **Pseudonymize** (keyed HMAC) | Join keys for sessionization, dedup, and any future stitching |
| Network addresses | `ip`, `ip2`, `ipv6` | **Drop** (after optional bot/geo enrichment in Bronze) | High re-identification risk; no detection value at KPI grain |
| Fine geo | `geo_zip`, `post_zip` | **Generalize** → region/DMA | KPI segmentation needs region, not postal code |
| Device fingerprint | `user_agent`, `accept_language` | **Generalize** → browser family / language | Fingerprinting risk; families suffice for tagging-health KPIs |
| Visitor counters | `daily_visitor` … `yearly_visitor` | **Keep** (flags, not identifiers) | Binary new-visitor flags; no identity content |
| Account/system | `userid`, `username`, `user_hash` | **Drop** | Single constant account-level values (cardinality 1) — no analytical value |
| Personalization/social IDs | `post_tnt`, `socialaccountandappids` | **Pseudonymize or drop** per review | Value depends on Phase-2 segmentation needs |

Mechanics (ADR-0007): **HMAC-SHA-256 with a key in Azure Key Vault**, key-versioned so rotation never
orphans history. Deterministic → the same visitor yields the same pseudonym across days and domains,
so baselines and visit-level KPIs survive; without the key, pseudonyms are not reversible.

Two standing caveats:

- **Pseudonymized data is still personal information** under PIPEDA and Law 25 — this layer reduces
  risk; it does not exit the regulatory perimeter. Downstream RBAC/masking (§7) stays mandatory.
- **Consent**: hits carrying Adobe consent-opt-out signals are excluded before Silver; the exclusion
  count is itself a monitored data-quality metric (a consent-misfire anomaly class,
  [02 §9](02-solution-architecture.md)).

## 3. Identity model today — stitching deferred

The extension research assumed an authenticated ID exists to stitch to. The data says otherwise
(full-database check with the data owner, 2026-07-04):

- `cust_visid` / `post_cust_visid` (the custom-visitor-ID slots an authenticated login would populate):
  **completely NULL on all rows**.
- `userid`: populated on every row but a **single constant value** (Adobe account-level column,
  cardinality 1) — not a person key.
- The only well-populated identifiers (`mcvisid`, `post_visid_high/low`) are **device/cookie-level**.

So GMAI-Pulse Phase 1 operates on a **device-level identity model**, which is sufficient for every
registry KPI (traffic, conversion, tagging health — none require person-level joins). Stitching
becomes worthwhile only for Phase-2+ journey/CX analysis, and is gated (ADR-0007) on:

1. **MarTech implementation ask (standing):** capture the login/customer ID in a dedicated eVar at
   authentication, so `post_evar` rows carry a person key going forward; then
2. a ≥30-day production feed showing that eVar populated, and
3. classification-review approval to use it (pseudonymized, never raw).

When the gates pass, stitching is a Silver re-key by pseudonymized person ID — start with plain
deterministic joins; adopt a replay/lookback mechanism only if pre-login attribution proves material.

## 4. Synapse serverless governance (CoverMe's residual surface)

[ADR-0006](adr/adr-0006-unified-databricks-compute-plane.md) leaves `martech.adobe_coverme.hit_data`
serving its existing consumers on **Synapse serverless SQL**. Gap: serverless pools do **not** support
Dynamic Data Masking or Row-Level Security on external tables / Delta reads — those are dedicated-pool
features. Any consumer with SELECT on the external table sees raw `ip`, cookie IDs, and `geo_zip`.

**Recommendation to the CoverMe platform owners** (not a GMAI-Pulse deliverable):

- Create a custom schema (user objects cannot live in `dbo` alongside lake tables) holding **secure
  views** over the external table; apply conditional masking inline via `IS_MEMBER()` per Entra group
  (e.g., analysts see masked identifiers, a privileged group sees pseudonyms — never raw).
- `GRANT SELECT` on the secure schema only; `DENY`/revoke direct SELECT on the external table for
  human/reporting principals (existing Power BI datasets repoint to the views).
- **GMAI-Pulse is unaffected**: its reader is an Entra service principal reading the ADLS files via a
  UC external location, governed by Unity Catalog — raw columns never leave the Databricks Bronze
  zone un-pseudonymized (§2).

## 5. Canadian privacy mapping

| Obligation | Regime | Design control | Where |
|---|---|---|---|
| Purpose limitation & transparency | PIPEDA | Purposes fixed as operational anomaly detection + CX monitoring; no secondary use without review | this doc; [02 §9](02-solution-architecture.md) |
| Consent | PIPEDA / Law 25 | Consent-signal exclusion pre-Silver; misfire monitoring | §2 |
| De-identification by default | Law 25 | Identity & Privacy layer — pseudonymize/drop/generalize before analytics | §2, ADR-0007 |
| Privacy Impact Assessment (PIA) | Law 25 | PIA required before production ingestion; owner TBD (§8) | §8 |
| Cross-border / outside-Québec transfer assessment | PIPEDA / Law 25 | Data residency in Canadian Azure regions; private/VPC model serving ([ADR-0005](adr/adr-0005-model-tuning-adaptive-ml.md), [07](07-adaptive-ml-integration.md)); any non-Canadian processing needs an equivalency assessment | [02 §8](02-solution-architecture.md) |
| Deletion / right to erasure | PIPEDA / Law 25 | Crypto-erasure + Delta delete design | §6 |
| Safeguards & accountability | PIPEDA / Law 25 | UC RBAC, masking, lineage, LLM prompt/response audit log | §7; [02 §9](02-solution-architecture.md) |
| Incident register & breach notification | Law 25 | Anomaly/incident log doubles as evidence trail; formal register owner TBD | §8 |

**Bill C-27 (CPPA/AIDA) caveat:** treated as directional only — track status; do not design to a
moving target. Law 25 is the binding high-water mark today; legal review owns the exact penalty
exposure figures.

## 6. Erasure & deletion design (design-forward)

Deletion requests are rare for pseudonymized clickstream, but Law 25 makes the path mandatory:

1. **Locate**: resolve the subject to pseudonyms (requires the HMAC key — privileged, audited op).
2. **Delete forward**: Delta `DELETE` by pseudonym across Silver/Gold/anomaly tables; `VACUUM` to
   expire time-travel snapshots past the retention window.
3. **Crypto-erase history**: where physical rewrite is disproportionate (Bronze archives), deleting
   the relevant HMAC key version severs re-linkability — the pseudonyms become unlinkable tokens.
4. **"Unstitching"** (replacing person IDs with the original anonymous device IDs) applies **only if**
   stitching (§3) is ever built; noted for that future, not designed now.

Retention: raw Bronze at the shortest window that supports reprocessing (target: match the ≥90-day
baseline-training need, [10 §3](10-data-profile-alignment.md)); Gold KPI aggregates are
non-personal and may be retained long-term.

## 7. Role model (sketch)

| Principal | Sees | Mechanism |
|---|---|---|
| Pipeline service principal | Raw Bronze (transient), pseudonymized Silver+ | UC service credentials; no interactive access |
| ML engineers | Pseudonymized Silver/Gold | UC group + column masks on residual quasi-identifiers |
| Analysts / Phase-2 agent users | Gold KPIs, anomalies, insights — no identifiers | UC RBAC on Gold/anomaly schemas |
| Executive BI | Aggregates only | Dashboard-level, no table access |
| Privacy officer / DSAR operator | Pseudonym-resolution capability | Key Vault access policy + audited break-glass workflow |

Synapse side mirrors the same Entra groups in the `IS_MEMBER()` view logic (§4), so a person's access
class is consistent across both engines. All grants least-privilege; UC lineage + audit logs per
[02 §9](02-solution-architecture.md).

## 8. Open items

1. **Data-classification review** of the 24 flagged columns — ratifies the §2 disposition table
   (pre-existing hard gate, [10 §3](10-data-profile-alignment.md) blocker 2).
2. **Key Vault ownership**: who owns the HMAC key, access policy, rotation calendar.
3. **Law 25 PIA owner** and timeline — must precede production ingestion.
4. **MarTech eVar ask** (§3): file the implementation request; stitching gates start counting only
   after it ships.
5. **Inventory of direct Synapse consumers** — needed before the §4 secure-view recommendation can be
   sized by the CoverMe platform team.
