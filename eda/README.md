# GWAM Canada Retirement — EDA Notebook

`gwam_canada_retirement_eda.py` is a **read-only** Databricks notebook that profiles the
Adobe Analytics hit-level table for the GMAI-Pulse anomaly-detection project. Its outputs
fill the evidence gaps in `research/claude/` (volume/cadence — open blocker #8), seed the
29 placeholder slots in `research/claude/metric-registry.yaml`, and produce the spec used
to generate synthetic data locally.

## How to run

1. In the Databricks workspace: **Workspace → Import → File**, select
   `gwam_canada_retirement_eda.py` (it imports as a notebook — the file is in
   Databricks "source" format).
2. Attach to any cluster with Unity Catalog access (DBR 13+ recommended). A small
   cluster is fine — the heavy sections run on a 5% sample.
3. Run the **S0 config cell** once; widgets appear at the top. Defaults:
   - `table_fqn` = `gwam_prod_catalog.inv_typed_common.adobe_hit_data` (provisional —
     if S1 says `resolves: false`, pick a candidate from its output and update the widget)
   - `window_months` = 13, `sample_fraction` = 0.05
4. **Run All.** Expected runtime: S1/S2 seconds; S3 is the one full-table scan
   (narrow projection — minutes); S5–S11 run on the sample; S8 makes two exact passes
   over the 13-month window.
5. A section that fails prints `===== SKIPPED: <id> | <reason> =====` and the run
   continues — paste SKIPPED lines back too.

If the run is too slow or the cluster is small: lower `sample_fraction` to 0.01 and/or
`window_months` to 6 and re-run from S4 onward (sections rebuild their frames).

## What to paste back

Copy each `===== BEGIN SHAREABLE: <id> ===== ... ===== END SHAREABLE =====` block
verbatim. Priority order if splitting across messages:

| Priority | Block(s) | Feeds |
|---|---|---|
| 1 | `synthesis_spec` | Master consolidated spec → synthetic data generator |
| 2 | `ts_daily`, `ts_events`, `ts_profiles` | Anomaly-model design (seasonality, lags, volatility) |
| 3 | `event_decode`, `live_custom_dims`, `population_census` | Filling metric-registry.yaml's 29 slots |
| 4 | `daily_volume`, `delta_meta` | Volume/cadence evidence (closes blocker #8) |
| 5 | `dim_candidates`, `dq_baseline`, `identity_evidence`, `uc_discovery`, `window_frame` | Dimensional slicing, DQ rules, ADR-0007 validation |

Multi-part blocks (`part 1 of N`) reassemble by concatenation — paste all parts.

## Privacy guarantees (ADR-0007 / doc-11)

- Sensitive columns (visitor IDs, IPs, cookies, geo_zip, user_agent, userid, ...) are
  reported **shape-only**: null %, approximate cardinality, length stats. No values,
  not even masked ones.
- All other high-cardinality values are masked as `<masked:xxxxxxxx>` tokens (same
  format as `new_data/generated_data_profile.json`).
- URLs and pagenames are query-stripped; full URLs never printed — only
  domain + path-depth + first-segment shapes.
- A final scrubber redacts anything resembling an email, IP, or long numeric/hex ID
  and truncates all strings to 160 chars.
- The Delta `location` path and table properties are excluded from `delta_meta`.

Sanity-check the output before pasting; if anything looks like a leak, flag it instead
of pasting so the notebook can be tightened.
