# Shareable Field Analysis for Anomaly Detection Research

## 1. Purpose

This document summarizes Adobe Analytics data dictionary and profile metadata to support methodology research for anomaly-detection and investigation tooling. It is intended as an external-shareable artifact: it captures the *shape* of the available metadata, the *categories* of candidate metrics, dimensions, and join keys, and the *data-quality findings* visible at the profile level — without exposing raw enterprise rows, customer identifiers, internal URLs, credentials, or sensitive values. The analysis is profile-derived; it supports sheet-level and column-level conclusions, not a complete per-row slot catalogue.

## 2. Generalized Project Context

The receiving programme is an enterprise anomaly-detection and insights initiative covering digital marketing, SEO, web analytics, MarTech, and customer-experience signals. The work product is intended to:

- Detect anomalies in traffic, events, tagging, campaigns, and measurement.
- Provide correlation and root-cause support across deployments, content changes, and campaign activity.
- Generate business-friendly investigation summaries describing *what changed, where, when, and likely why*.
- Support digital analytics monitoring and triage across multiple sites and brands.
- Cover both operational anomalies (tagging, data collection, platform) and behavioural anomalies (visitor activity, conversion, contact-centre, sentiment).
- Build institutional memory of recurring incident patterns for faster future diagnosis.

## 3. Workbook Overview

- **Workbook:** `CoverMe_Adobe_Analytics_Data_Dictionary_Web_-_Data_Feed.xlsx`
- **Worksheets analyzed:**
  - `data_feed_columns`
  - `post_eVar`
  - `post_prop`
  - `post_event_list`

| Worksheet | Rows | Columns | Notes column present |
|---|---:|---:|---|
| data_feed_columns | 225 | 4 | Yes (heavily null) |
| post_eVar | 200 | 6 | Yes |
| post_prop | 75 | 6 | Yes (fully null) |
| post_event_list | 1263 | 6 | Yes |

The available analysis is **profile-based**. It supports conclusions at the worksheet and column level, including shape, candidate roles per column, and observable data-quality issues. It does **not** support a complete per-row, slot-level semantic catalogue, and no attempt is made to derive one here.

## 4. Worksheet-Level Analysis

### data_feed_columns

- **Inferred purpose:** Reference list of Adobe Analytics data-feed columns documenting field name, description, data type, and free-text notes. Acts as the catalogue against which downstream metric and dimension fields are interpreted.
- **Relevant schema/column groups:** an identifier/preamble column carrying Adobe documentation text; a description column; a data-type column; a notes column.
- **Candidate timestamp fields:** none.
- **Candidate metric fields:** none directly at the metadata level; metric candidates flow downstream into individual data-feed columns themselves.
- **Candidate dimension fields:** the data-type column and the notes column (used for grouping and filtering the catalogue).
- **Candidate join-key fields:** the identifier/preamble column, when normalized to a clean slot or column name.
- **Sensitive-column candidates:** the identifier/preamble column is heuristically flagged sensitive because it carries long free-form documentation text rather than an identifier value.
- **Notes-based anomaly-detection tagging:** **9** rows tagged `Anomaly Detection`.
- **Anomaly-detection relevance:** medium — provides the field vocabulary needed to map raw data-feed columns into metric and dimension categories.
- **Investigation and insights relevance:** medium — supplies the canonical labels and data-type hints used in human-readable summaries.
- **Data-quality observations:** notes column ~95.6% null with `Anomaly Detection` (9) the dominant non-null value; data-type column is dominated by `varchar(255)` with a long tail of additional types; column header naming is irregular (some columns are unnamed in the profile).
- **Recommended use:** treat as a controlled vocabulary for the Adobe data feed; use the `Anomaly Detection`-tagged subset to prioritize which data-feed columns enter Phase 1.

### post_eVar

- **Inferred purpose:** Catalogue of Adobe Analytics **eVar** (conversion / persistent dimension) slots, mapping slot identifier to business name plus operational metadata.
- **Schema profile:** slot identifier column, business name column, description column, status column, example-value column, notes column.
- **Notes-based anomaly-detection tagging:** **8** rows tagged `Anomaly Detection`.
- **Candidate dimensions:** the status column and the notes column at the metadata level; downstream, the catalogued eVar slots themselves become dimension candidates for segmentation.
- **Candidate join keys:** the slot identifier column and the business name column (with caveats — see quality issues).
- **Candidate investigation fields:** the `Anomaly Detection`-tagged subset of eVar slots, used to enrich investigation summaries with the business meaning of changed dimensions.
- **Quality issues:**
  - The description column is **100% null** — no business descriptions available from the profile.
  - The example-value column is **100% null** — no example values available from the profile.
  - The name column contains duplicate values (e.g., the same business name reused across two slots in at least two cases). Direct use as a semantic join key requires disambiguation.
  - The status column is reasonably populated and clean (Enabled / Disabled only).
- **Recommended use:** primary source of **dimension candidates** for segmentation, triage, and routing. Resolve duplicate business names before treating the name column as a join key; supplement with business interviews to restore missing descriptions.

### post_prop

- **Inferred purpose:** Catalogue of Adobe Analytics **prop** (traffic / pathing / custom property) slots, mapping slot identifier to business name plus operational metadata.
- **Schema profile:** slot identifier column, business name column, description column, status column, example-value column, notes column.
- **Notes-based anomaly-detection tagging:** **0** rows tagged `Anomaly Detection`.
- **Candidate dimensions:** the status column at the metadata level; downstream, the catalogued prop slots themselves become dimension candidates for pathing and segmentation.
- **Quality issues:**
  - Description, example-value, and notes columns are **all 100% null**.
  - The name column is ~32% null — a meaningful fraction of slots lack a friendly label.
  - The status column is populated and clean.
- **Recommended use:** reserve for segmentation, pathing, and investigation enrichment. Zero `Anomaly Detection` Notes tagging in the profile does **not** imply zero future utility; it indicates that this sheet has not yet been business-tagged. Treat as Phase 2+ once labels and descriptions are restored.

### post_event_list

- **Inferred purpose:** Catalogue of Adobe Analytics **event / metric** slots, mapping a numeric event slot to an event name, optional friendly name, description, status, and notes.
- **Schema profile:** numeric event-slot column, event-name column, friendly-name column, description column, status column, notes column.
- **Notes-based anomaly-detection tagging:** **12** rows tagged `Anomaly Detection`.
- **Candidate metrics:** the catalogued event slots themselves are the primary source of **event-count metric candidates** for anomaly detection.
- **Candidate investigation fields:** the friendly-name column and the `Anomaly Detection`-tagged subset of event slots, used to label changed metrics in business-friendly investigation summaries.
- **Data-quality issues:**
  - The status column has **casing inconsistency** (e.g., `Enabled` and `ENABLED` both appear, alongside `Disabled`); ~75.6% null. Normalize casing and resolve null status before any filtering or aggregation.
  - The friendly-name column is ~79.6% null — most events lack a business-friendly label.
  - The description column is ~87.6% null with a single masked value dominating populated rows; not directly usable as descriptive metadata.
  - The numeric event-slot column is flagged by heuristic as a timestamp candidate, but it is structurally a slot index / join key (100% distinct, large integer range). Treat as identifier, **not** as time.
- **Recommended use:** primary source of **event-count metric candidates**. Use the `Anomaly Detection`-tagged subset as the initial Phase 1 metric short-list, and pair every selected event with status normalization plus a business-friendly label.

## 5. Field Categorization Summary

The categories below are derived from sheet-level and column-level evidence in the profile. Where the profile only supports column-level evidence, this is stated explicitly; specific in-row slot semantics are not reproduced.

- **Candidate metrics** — *Evidence: column-level only.* Event-slot rows in `post_event_list` are the dominant metric source. The `Example Value(s)` columns in `post_eVar` and `post_prop` are flagged by heuristic but are fully null in the profile and are not usable.
- **Candidate dimensions** — *Evidence: column-level only.* eVar slot rows in `post_eVar`, prop slot rows in `post_prop`, status columns across sheets, and the data-feed data-type column.
- **Timestamp / date fields** — *Evidence: none at row level.* No native timestamp columns are present across the four sheets. A numeric event-slot column in `post_event_list` is heuristically flagged as timestamp but is structurally an identifier.
- **Event fields** — Event-slot rows in `post_event_list`, including the event-name and friendly-name columns.
- **Campaign / content / page fields** — *Evidence: category-level only.* The profile confirms eVar and prop catalogues but does not, by itself, identify which specific slots map to campaign, content, or page semantics. Business-tagging interviews are required to populate this category.
- **Technical implementation fields** — Data types, status flags, and slot-identifier conventions across all four sheets. The data-type distribution in `data_feed_columns` (varchar / int / tinyint / text) is observable in the profile.
- **Join keys** — Slot identifier columns in `post_eVar`, `post_prop`, and `post_event_list`; event-name column in `post_event_list`; preamble identifier column in `data_feed_columns`. Confidence varies — see §8.
- **Fields requiring governance review** — Description columns across the three Adobe-Analytics-style sheets, the long documentation/preamble column in `data_feed_columns`, and any high-cardinality identifier columns. See §10.

## 6. Candidate Anomaly-Detection Metrics

The metric categories below are inferred from the worksheet roles, candidate lists, and Notes-based tagging in the profile. No specific confidential metric names are introduced beyond what the profile supports as a category.

### Event-count metrics from the event catalogue
- **Why useful:** events are the most directly observable, countable unit in Adobe Analytics; the event catalogue in `post_event_list` has the largest set of `Anomaly Detection`-tagged candidates (12).
- **Possible anomaly types:** spike, drop, sustained level shift, missing data, zero-volume, seasonality deviation, dimension-specific anomaly.
- **Likely caveats:** sparse low-volume events; status-casing inconsistency requires normalization; many events lack friendly labels; need a clear grain (hour / day / site).

### Traffic and behaviour metrics from Adobe data-feed columns
- **Why useful:** data-feed columns supply session, visit, and page-level numeric signals; the `data_feed_columns` catalogue has 9 `Anomaly Detection`-tagged entries.
- **Possible anomaly types:** spike, drop, sustained level shift, missing data, zero-volume, seasonality deviation, dimension-specific anomaly.
- **Likely caveats:** many varchar columns are dimensions, not metrics; selecting the right numeric columns requires alignment with the business-tagged subset; some metrics carry implicit dimensionality (per-visit vs per-visitor).

### Tagging and completeness proxies
- **Why useful:** sudden drops in tagged-event coverage or in eVar/prop population are a leading indicator of broken implementations; status fields and tagging-completeness ratios are direct proxies.
- **Possible anomaly types:** drop, sustained level shift, missing data, zero-volume, dimension-specific anomaly.
- **Likely caveats:** requires baseline population rates per slot; thresholds must accommodate intentional disable/enable cycles; status-casing inconsistencies must be normalized first.

### Conversion or funnel-related event metrics
- **Why useful:** funnel-stage events (browse, add-to-cart, checkout, purchase-type) are the highest-value metrics for protecting revenue and media efficiency.
- **Possible anomaly types:** spike, drop, sustained level shift, missing data, zero-volume, seasonality deviation, dimension-specific anomaly.
- **Likely caveats:** identifying funnel-stage events from the profile alone is not possible — requires business-tagging interviews; conversion volumes can be sparse and seasonal.

### Missing-data and zero-volume monitoring candidates
- **Why useful:** explicit zero-volume monitoring on critical events catches tagging breakage that suppression of low-volume signals would otherwise hide.
- **Possible anomaly types:** missing data, zero-volume, sustained level shift, dimension-specific anomaly.
- **Likely caveats:** must be paired with an "expected to be non-zero" allowlist; false positives are likely for legitimately low-volume events; seasonality deviation rules need calendar awareness.

## 7. Candidate Dimensions

The dimension categories below correspond to the catalogues and metadata columns visible in the profile. Each supports segmentation, triage, routing, root-cause investigation, and explainability of anomaly summaries.

- **eVar-based dimensions** — persistent / conversion-context dimensions from the `post_eVar` catalogue. Useful for segmenting anomalies by user attributes, journey stage, and conversion context.
- **prop-based dimensions** — traffic / pathing / custom property dimensions from the `post_prop` catalogue. Useful for pathing analysis and short-lived contextual segmentation.
- **Page / content dimensions** — *category-level only.* The profile confirms catalogues that typically include page and content slots, but it does not directly identify which slots are page-level. Useful for "where it changed" attribution.
- **Campaign dimensions** — *category-level only.* Same caveat: campaign-bearing eVars and props exist in similar catalogues but cannot be enumerated from the profile alone. Useful for correlating anomalies with media activity.
- **User / session / device / channel dimensions** — visible at category level in `data_feed_columns` (data-type distribution suggests session and device columns). Useful for triage and ownership routing.
- **Technical implementation dimensions** — data type, status, and slot-status fields. Useful for filtering out disabled or deprecated slots before alerting.

Together these dimensions support:

- **Segmentation** — slicing each metric by candidate dimensions to locate where the deviation is concentrated.
- **Triage** — narrowing alerts to the affected segment to route to the right owner.
- **Routing** — using dimension values (e.g., site, channel, implementation status) to pick a destination team.
- **Root-cause investigation** — co-occurring shifts across multiple dimensions hint at upstream cause.
- **Explainability** — converting "metric X dropped" into "metric X dropped *on segment Y* *in channel Z*" for business-friendly summaries.

## 8. Candidate Join Keys

Confidence labels reflect both the profile evidence and the data-quality findings.

| Category | Source | Confidence | Notes |
|---|---|---|---|
| Timestamp / date | none observed | **N/A** | No native timestamp columns present in the four sheets. Time joining must come from downstream feeds, not from this workbook. |
| Event identifier | `post_event_list` event-name column | **High** | 100% distinct, fully populated. |
| Event slot index | `post_event_list` numeric event-slot column | **High** | 100% distinct; heuristic flag as timestamp is incorrect — treat as identifier. |
| eVar / prop slot metadata (slot identifier) | `post_eVar` and `post_prop` slot-identifier columns | **High** structurally | 100% distinct slot identifiers. |
| eVar / prop slot metadata (business name) | `post_eVar` and `post_prop` name columns | **Medium** | Duplicate names in eVar; ~32% null in prop. Disambiguation required. |
| Page / content keys | catalogue-level only | **Low** | Not directly identifiable from the profile. Requires business mapping. |
| Campaign keys | catalogue-level only | **Low** | Not directly identifiable from the profile. Requires business mapping. |
| Session / user-level keys | inferred from `data_feed_columns` only | **Low** | Governance review mandatory before any session- or user-level join. |
| Data-feed technical keys | `data_feed_columns` preamble / identifier column | **Medium** | Carries long documentation text; normalize to clean slot name before joining. |

## 9. Candidate Investigation & Insights Fields

These categories describe the field shapes needed to populate an investigation summary. Several are sourced from the profile catalogues; some must come from downstream metric feeds, deployment logs, or campaign systems.

- **What changed** — affected metric name, observed change direction and magnitude.
- **Where it changed** — segmenting dimensions (eVar / prop / page / channel) that isolate the change.
- **When it changed** — change-point timestamp (sourced downstream; not in this workbook).
- **Affected metric** — canonical metric label from `post_event_list` or from data-feed metric columns.
- **Affected dimension** — canonical dimension label from `post_eVar` / `post_prop` / `data_feed_columns`.
- **Related campaign / content / page context** — campaign or content identifiers from the catalogued slots (requires business mapping).
- **Technical implementation context** — slot status, data type, tagging health proxies.
- **Business Notes / context** — `Anomaly Detection`-tagged Notes entries that capture business intent.
- **Owner / routing fields** — *not present in the profile.* Recommend adding owner / routing metadata as part of the canonical model.

## 10. Fields Requiring Governance Review

Sensitive categories visible at the profile level are listed below without exposing any values. Sensitivity is treated by category, not by individual value.

- **Customer or session identifiers** — any visit / session / order identifier surfaced through data-feed columns.
- **Visitor identifiers** — persistent visitor IDs, including cookie-derived identifiers.
- **IP / device / cookie-like fields** — IP address, device ID, cookie ID, or proxied equivalents.
- **Internal URLs** — referrer or destination URLs that may carry internal hostnames or query strings.
- **Free-text comments** — open-text columns including description and notes fields where business intent is not yet bounded.
- **Account / policy / customer-like identifiers** — any account, policy, or contract-style identifier appearing in eVar or prop catalogues.
- **High-cardinality identifiers** — any column that is fully or near-fully distinct and not pre-classified.

Recommended handling per category:

- **Exclude** — drop the field from the modelling layer if it has no analytical value.
- **Aggregate** — replace raw values with grouped buckets (e.g., country instead of IP).
- **Hash** — apply a salted one-way hash before joining.
- **Mask** — render values opaque in any externally shared artefact.
- **Restrict access** — keep the raw column inside a controlled enclave.
- **Review required** — escalate for business and privacy review before any use.

## 11. Recommended Canonical Modeling Approach

The model below describes how the catalogued fields should map into the anomaly-detection solution. Each subsection is a target schema, not an implementation.

### Metric observations
- `timestamp`
- `metric_name`
- `metric_value`
- `source`
- `dimensions` (key-value set drawn from the dimension catalogues)
- `quality_flags`

### Anomaly events
- `anomaly_id`
- `timestamp`
- `metric`
- `observed_value`
- `expected_value`
- `score`
- `severity`
- `dimensions`
- `method`

### Dimension context
- eVar / prop / data-feed dimensions
- page / campaign / channel / context fields (populated once business mapping is complete)

### Correlation / root-cause context
- events
- campaigns
- tagging changes
- implementation metadata
- content / page context

### Investigation insight summaries
- what changed
- where
- when
- magnitude
- likely cause
- confidence
- recommended next action

## 12. Recommended Phase 1 Field Set

Phase 1 prioritization follows the Notes-based `Anomaly Detection` tagging counts visible in the profile: `post_event_list` (12) → `data_feed_columns` (9) → `post_eVar` (8). `post_prop` has no current Notes-based tagging in the profile but remains useful later for segmentation and investigation.

- **Phase 1 metric categories:**
  - Event-count metrics drawn from the `Anomaly Detection`-tagged subset of `post_event_list`.
  - Traffic and behavioural metrics drawn from the `Anomaly Detection`-tagged subset of `data_feed_columns`.
- **Phase 1 dimension categories:**
  - eVar-based dimensions from the `Anomaly Detection`-tagged subset of `post_eVar`.
  - Status / data-type technical dimensions across all sheets.
- **Phase 1 join-key categories:**
  - Event identifier (`post_event_list` event-name column).
  - eVar slot identifier (`post_eVar` slot column).
  - Data-feed slot / column identifier (after normalization of the preamble column).
- **Phase 1 investigation fields:**
  - `Anomaly Detection`-tagged Notes entries across the three priority sheets.
  - Status, data type, and slot identifier on each catalogued field.
- **Quality checks required before modelling:**
  - Normalize `Status` casing in `post_event_list` (`Enabled` vs `ENABLED`).
  - Resolve duplicate `Name` values in `post_eVar` (e.g., reused business names).
  - Backfill missing labels in `post_prop` `Name` (~32% null) before any prop-based segmentation.
  - Confirm that `post_event_list` numeric event-slot column is treated as identifier, not timestamp.
  - Capture descriptions for `post_eVar` and `post_prop` from business interviews — the profile shows 100% null in both.

`post_prop` is deferred to Phase 2+ until business tagging and descriptions are restored; this is a decision about *current readiness*, not about future utility.

## 13. Fields to Defer

- Fields without business Notes / context.
- Fields with high missingness in label or description columns (e.g., `post_event_list` friendly-name at ~79.6% null; `post_prop` name at ~32% null).
- Fields with ambiguous meaning where the same business name is reused across multiple slots.
- Sensitive identifiers requiring governance decisions (visitor / session / cookie / IP / account-style fields).
- Fields requiring per-slot business mapping that is not present in the profile.
- Rows or slots whose semantics cannot be interpreted from profile-level data alone.

## 14. Methodology Research Questions

- What are the best methods for Adobe Analytics event-count anomaly detection at site-and-day grain?
- How should sparse event time series be handled to avoid false-positive zero-volume alerts?
- How can broken or missing analytics tags be detected from event-count patterns alone?
- How should event-count drops and zero-volume conditions be distinguished from legitimate low-traffic periods?
- What detection approaches work best for combined traffic and conversion anomalies?
- How should seasonality in digital analytics (daily, weekly, holiday) be modelled or removed?
- How should high-cardinality dimensions be handled in dimension-aware anomaly detection?
- How can eVars and props be used for segmentation without exploding alert volume?
- How should anomalies be correlated with deployment, campaign, content, and tagging-change context to surface likely cause?
- How should stakeholder-friendly investigation summaries be designed to balance precision and readability?
- How should alert severity and suppression rules be defined to avoid alert fatigue?
- How can anomaly detection be back-tested in environments with limited labelled incidents?

## 15. Business Clarification Questions

- Which Notes-tagged fields across the four worksheets are the highest priority for Phase 1?
- What is the intended business meaning of each tagged eVar, event, and data-feed field?
- Which fields are metrics versus dimensions in the business interpretation?
- What is the expected grain of detection (e.g., site / channel / hour / day)?
- Which dimensions are required for segmentation by stakeholder group?
- Which fields identify page, campaign, channel, product, or customer-journey stage?
- Which fields should never be used because of privacy or sensitivity constraints?
- What are the known historical incidents that should be used to validate detection methodology?
- What are the expected seasonality patterns by site, channel, and audience?
- What alert thresholds and severity levels are actionable for each stakeholder group?
- Who owns each metric or field group, including escalation and routing rules?

## 16. Governance Notes

- The output in this document is sanitized for external sharing.
- Raw enterprise rows were not included; only profile-derived shape and category-level conclusions appear here.
- Profile-level evidence was used throughout; no XLSX re-parse or row-level extraction was performed.
- Sensitive values, identifiers, and free-text content were excluded or generalized.
- The internal-only profile JSON should be reviewed by the data governance owner before any wider distribution.
- A formal governance review is required before this analysis is used to drive production modelling, alerting, or routing decisions.
