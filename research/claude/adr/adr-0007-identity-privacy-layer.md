# ADR-0007 — Identity & Privacy layer: keyed pseudonymization, stitching deferred

**Status:** Accepted · **Date:** 2026-07-04 · **Deciders:** GMAI–Pulse solutioning

## Context
Two extension-research passes (`research/perplexity/perplexity_extending_GMAI.md`,
`research/gemini/gemini_extending_GMAI.md`) proposed adding Adobe identity stitching (ECID → person ID,
field-based stitching with replay windows, a custom Spark replay engine) and an explicit identity/privacy
masking layer to the ingestion phase. Three facts constrain how much of that we adopt now:

1. **The current design is ambiguous and partly irreversible.** [02 §9](../02-solution-architecture.md)
   says "hash visitor identifiers with salted SHA-256 (or tokenize/drop, per classification outcome)".
   Dropping — or hashing with an unmanaged salt — permanently destroys the joinability of `mcvisid` /
   `post_visid_high/low`, which any future person-level stitching would need as its device-side key.
2. **There is nothing to stitch today.** At full-database scale (confirmed with the data owner,
   2026-07-04): `cust_visid` / `post_cust_visid` — the slots where an authenticated custom visitor ID
   would land — are **completely NULL**, and `userid` carries a **single constant value on every row**
   (an account-level Adobe column, cardinality 1, not a person key). No populated person-level
   identifier exists in the feed.
3. **CoverMe's residual query surface has a governance gap.** Synapse **serverless** SQL pools do not
   support Dynamic Data Masking or Row-Level Security on external tables / Delta reads
   ([Microsoft Q&A](https://learn.microsoft.com/en-us/answers/questions/2120635/dynamic-data-masking-on-synapse-serverless-sql-dat)).
   ADR-0006 keeps that surface untouched for existing consumers — so any PII visible there is governed
   only by table-level GRANTs today.

## Decision
1. **Name the boundary.** The Bronze→Silver PII step becomes the **Identity & Privacy layer** — the
   single enforcement point (per [ADR-0006](adr-0006-unified-databricks-compute-plane.md)) where
   identifier handling, PII disposition, and consent exclusion happen.
2. **Deterministic keyed pseudonymization for identifiers.** Visitor/device identifiers (`mcvisid`,
   `post_visid_high/low`, cookie IDs) are pseudonymized with **HMAC-SHA-256 under a key held in Azure
   Key Vault** (key-versioned; rotation documented). This replaces "salted SHA-256 (or tokenize/drop)".
   Same input → same pseudonym, so series remain joinable across days and across both domains; without
   the key, values are not reversible. Non-identifier sensitive columns (e.g., `ip`, `geo_zip`,
   `user_agent`) are dropped or generalized per the classification review — disposition table in
   [11 §2](../11-privacy-identity-governance.md).
3. **Identity stitching is deferred, behind two gates:** (a) a populated person-level identifier
   (login/customer ID captured in an eVar — a standing MarTech implementation ask) present in a
   ≥30-day production feed, and (b) approval of that identifier's use in the PII classification review.
   If both gates pass, stitching is a Silver-layer re-key by pseudonymized person ID — no Adobe CDA
   dependency and no replay engine is prescribed until the data justifies one.
4. **Secure views for residual direct Synapse consumers.** Recommend to the CoverMe platform owners:
   expose `martech.adobe_coverme.hit_data` to human consumers only through views in a custom schema
   applying `IS_MEMBER()` conditional masking (DDM/RLS being unavailable on serverless external
   tables). GMAI-Pulse's own reader is an Entra service principal governed by Unity Catalog and is
   unaffected — detail in [11 §4](../11-privacy-identity-governance.md).

5. **Analysis-time visibility is separate from pipeline-time disposition (added 2026-07-19;
   revised to full-raw 2026-07-23).** §1–§4 govern what the *shipped pipeline* persists at
   Bronze→Silver. They were being applied a second time, far more broadly, to *EDA notebook
   output* — where the profiler masked every value not on a 24-entry allowlist to a
   `<masked:xxxxxxxx>` sha1 token, reduced URLs to `domain|depth|seg1`, truncated every string at
   160 characters, and redacted any 10+ digit run. eVar and prop contents, page paths, campaign
   codes and event labels came back unreadable, and hit counts and epoch timestamps were redacted
   as "long numbers". That cost real analytical fidelity while protecting no person, for three
   reasons: (a) **there is no person-level identifier in the in-scope feed** —
   `cust_visid`/`post_cust_visid` are wholly NULL and `userid` is a single constant account value
   (Context §2, confirmed with the data owner), and authenticated/login hosts are excluded from
   scope upstream (D8); (b) the masked columns were business semantics, not identifiers; and
   (c) EDA runs inside the Unity-Catalog-governed workspace, which is the actual access boundary.

   **Decision (data owner, 2026-07-23):** for exploratory analysis, **every column profiles and
   emits raw** — including the direct/quasi-identifier set (visitor/device IDs, cookies,
   `ip`/`ip2`/`ipv6`, `geo_zip`, `user_agent`) and tracking eVars (ECID `eVar131`, Medallia UUID
   `eVar140`). There is no shape-only carve-out and no emit-time value redaction: a comprehensive
   view of all in-scope columns was the explicit requirement, and the identifier values the earlier
   carve-out withheld are needed to characterise those columns fully. URL query strings profile
   raw by default (a notebook widget re-enables stripping); the emit-time scrubber is reduced to
   display formatting only — long-string truncation and float rounding, with no PII patterns. This
   is the "PII-classification-review" outcome the 2026-07-19 revision deferred: the data owner has
   made the call for this feed and scope.

   **This changes nothing about what the pipeline persists.** HMAC-SHA-256 pseudonymization at
   Bronze→Silver (§2), the disposition table in [11 §2](../11-privacy-identity-governance.md), and
   the crypto-erasure property below all stand. Pseudonymized output — and now the raw EDA output —
   remains personal information under PIPEDA / Quebec Law 25.

   **Compensating control (load-bearing).** EDA notebooks print `SHAREABLE` blocks that are copied
   *out* of the workspace into chats, docs and tickets, and those blocks now carry raw identifiers
   and PII — IP addresses, postal codes, device IDs, User-Agent strings. The notebook prints a PII
   banner on every run, and a **mandatory human read-through before any block leaves the governed
   workspace** is the control that replaces the automated carve-out. Sharing is irreversible, so
   the review happens *before* export, not after.

## Consequences
- (+) **Joinability preserved, stitching stays an option** — pseudonyms are stable join keys; nothing
  irreversible happens to identifier columns before the classification review concludes.
- (+) **Crypto-erasure**: deleting (a version of) the HMAC key severs re-linkability, supporting
  PIPEDA / Quebec Law 25 deletion obligations without rewriting history tables ([11 §6](../11-privacy-identity-governance.md)).
- (+) One enforcement point for both domains, consistent with the single-compute-plane decision.
- (−) **Key-management dependency**: Key Vault ownership, access policy, and rotation must be assigned;
  rotating without key-versioning breaks longitudinal joins.
- (−) **Pseudonymized ≠ anonymized** — outputs remain personal information under PIPEDA/Law 25;
  UC RBAC + masking on analytical tables are still required.
- (−) The Synapse secure-view work is owned by the CoverMe platform team (recommendation, not a
  GMAI-Pulse deliverable) — an engagement dependency like the ADR-0006 storage-permission item.
- (+) **EDA is comprehensive over every column** (§5) — business dimensions AND the identifier set
  profile at full fidelity, so scope definition, metric-candidate discovery and anomaly-baseline
  work read real values rather than sha1 tokens or shape-only summaries. This unblocks the
  URL-scope, event-decode and full-column-coverage questions that stalled on masked output.
- (−) **Exported EDA blocks now carry raw business data AND raw identifiers/PII** (§5) — page
  paths, campaign codes and eVar contents, plus IP addresses, postal codes, device IDs and
  User-Agent strings, leave the workspace inside `SHAREABLE` output. Accepted by the data owner for
  this feed and scope (no person-level identifier; login hosts excluded), but the automated
  redaction net has been removed by design, so a **mandatory human read-through is now the only
  gate** before EDA output is posted anywhere outside the engagement — and it must run before
  export, because sharing is irreversible.

## Alternatives rejected
- **Irreversible hash or drop of visitor identifiers** — destroys the only device-level join keys;
  forecloses stitching for no privacy gain over keyed pseudonymization.
- **Build the stitching pipeline now** (Gemini's Spark replay engine) — nothing to stitch: the
  authenticated-ID columns are empty and its sample code references columns (`customer_id`) that do
  not exist in the profiled schema.
- **Real-time identity resolution at the edge** — requires the streaming hot lane; reopening
  [ADR-0001 v2](adr-0001-near-real-time-microbatch.md) is explicitly out of scope.
- **HSM-style storage tiering** (Gemini) — generic storage lifecycle advice; adds nothing over Delta +
  ADLS lifecycle policies already implied by the medallion design.
