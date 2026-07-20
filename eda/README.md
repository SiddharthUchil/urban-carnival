# GWAM Canada Retirement — EDA Notebook

`gwam_canada_retirement_eda.py` is a **read-only** Databricks notebook that profiles the
Adobe Analytics hit-level table for the GMAI-Pulse anomaly-detection project. Its outputs
fill the evidence gaps in `research/claude/` (volume/cadence — open blocker #8), seed the
29 placeholder slots in `research/claude/metric-registry.yaml`, and produce the spec used
to generate synthetic data locally.

## Scope: CA Retirement is a subset

The table holds **all** GWAM Adobe data. Canada Retirement is the subset selected by two
widgets (both default-on; blank either to disable it):

- `rsid_filter` = `manulifeglobalprod` (Manulife.com storefront report suite)
- `url_filter` = `manulife.com/ca/en/personal/group-plans/group-retirement` (page URL
  contains this — matches the section root and all subpages)

Every profiling section (S4–S11) and the `synthesis_spec` describe this subset. The only
whole-table outputs are `delta_meta` (S2) and the `total_hits` column of `daily_volume`
(S3), which reports total vs subset side by side plus the `ca_share_pct`. The scope
columns are resolved defensively against the schema (`rsid`/`report_suite`/… and
`post_page_url`/`page_url`); if a column is absent the condition is dropped and flagged in
`uc_discovery.scope` and `window_frame.filter`.

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

**If nothing downstream has data** (empty S5–S12): the scope filter matched 0 rows. S3
and S4 both flag this loudly. Check `window_frame.filter.top_rsids` for the real
report-suite value/casing and `window_frame.filter.url_only_match` to see whether the URL
substring is right, then adjust the `rsid_filter` / `url_filter` widgets and re-run from S3.

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

## Privacy guarantees (ADR-0007 §5 / doc-11)

- Sensitive columns (visitor IDs, IPs, cookies, geo_zip, user_agent, userid, ...) are
  reported **shape-only**: null %, approximate cardinality, length stats. No values,
  not even masked ones.
- All other values are emitted **raw**. eVar/prop contents are business semantics (form
  steps, plan codes, tool names) and profiling them is the point of the run; ADR-0007 §5
  exempts analysis-time output from the pipeline-time masking rule. Note the committed
  `synth/spec/*.json` still carries `<masked:xxxxxxxx>` tokens (25 schema entries) from
  runs that predate this change — `synth/` consumes them via the back-compat path in
  `synth/spec.py`.
- URLs and pagenames are query-stripped, but the full path is retained. Query strings stay
  stripped because they are the part of a URL that carries session tokens; the earlier
  domain + path-depth + first-segment reduction was dropped because it collapsed every
  Canadian page into a single bucket.
- A final scrubber redacts anything resembling an email, IP, or long numeric/hex ID
  and truncates all strings to 160 chars.
- The Delta `location` path and table properties are excluded from `delta_meta`.

Sanity-check the output before pasting; if anything looks like a leak, flag it instead
of pasting so the notebook can be tightened.
