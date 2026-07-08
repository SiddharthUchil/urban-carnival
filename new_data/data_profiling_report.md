# GMAI-Pulse — Deep Data Profiling Report

> **Purpose.** Column-level anomaly-detection readiness profile for every file under `data/`. Companion to the decision-oriented narrative in [data_inventory.md](data_inventory.md) and the aggregate auto-summary in [generated_data_profile.md](generated_data_profile.md). Together the three artifacts cover: **why** each file matters (inventory), **what** the profiler saw at a glance (auto-summary), and **how detection-ready** each column is (this report).

## Safety

- Inspection was **fully local**; no external APIs, no uploads, no network egress.
- This document contains only **aggregate statistics, column names, and masked example patterns**. Sensitive columns are named for schema completeness but their values are never displayed — not raw, not truncated, not even in masked form.
- Sensitive-column detection is **heuristic, not a compliance audit.** A separate data-classification review is a prerequisite before any production ingestion.

## Generation

- Generated at: `2026-07-02T16:45:44.553117+00:00`
- Tool version: `0.1.0`
- Input root: `/Users/uchilsi/Desktop/GMAI-Pulse/data`
- Files/sheets profiled: **5**
- Total rows (best-effort): **509**
- Total size on disk: **258,204 bytes**

Regenerate with:

```bash
python src/gmai_pulse/profiling/data_profiler.py \
    --input data --output docs/discovery --verbose
```

## Corpus at a glance

| # | File / sheet | Rows | Cols | Populated cols | Domain | Role | Confidence | Rel | Rdy |
|---:|---|---:|---:|---:|---|---|---|---:|---:|
| 1 | `Canada Retirement.xlsx#sheet=result` | 10 | 1,198 | 190 | `web_analytics` | 🔵 enrichment/dimension data | Medium | 8 | 8 |
| 2 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=data_feed_columns` | 224 | 4 | 4 | `web_analytics` | 🔵 enrichment/dimension data | Low | 4 | 6 |
| 3 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | 93 | 4 | 4 | `web_analytics` | 🔵 enrichment/dimension data | Low | 4 | 6 |
| 4 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | 26 | 3 | 3 | `web_analytics` | 🔵 enrichment/dimension data | Low | 4 | 6 |
| 5 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_event_list` | 156 | 5 | 5 | `web_analytics` | 🔵 enrichment/dimension data | Low | 4 | 6 |

## Per-file profiles

### 1. Canada Retirement — sheet `result`

#### 1. Dataset overview

| Property | Value |
|---|---|
| Path | `Canada Retirement.xlsx#sheet=result` |
| Sheet | `result` |
| Format | `.xlsx` |
| Business domain | `web_analytics` |
| Parse status | `ok` |
| Row count (true) | 10 |
| Sampled rows | 10 |
| Column count | 1,198 |
| Duplicate rows | 0 |
| Apparent grain | `event/hit-level` |

#### 2. Schema profile

- Total columns: **1198** (populated: **190**, empty in sample: **1008**)
- Sensitive-flagged columns: **24** (values are never displayed in this document)
- This sheet is treated as a **hit-level feed** — all example values are shown as category tags (`<url>`, `<hash>`, `<internal>`, `<structured>`, `<text:N>`, `<masked:…>`) regardless of the profiler's per-column sensitivity flag.

Populated columns — masked / abstracted examples only:

| Column | Type | Nullable | Distinct | Cardinality | Example pattern | Semantic |
|---|---|---:|---:|---|---|---|
| `accept_language` | `string` | no | 4 | 40% | `<masked:1dd3a7bf>`, `<masked:3fab8827>`, `<masked:7ec07213>` | `dimension` |
| `browser` | `integer` | no | 5 | 50% | `<masked:6adb7ab5>`, `<masked:5f82f62e>`, `<masked:e08a8853>` | `attribute` |
| `browser_height` | `integer` | no | 7 | 70% | `<masked:b66cd90e>`, `<masked:4707f922>`, `<masked:f7b41d20>` | `attribute` |
| `browser_width` | `integer` | no | 7 | 70% | `<masked:f369b411>`, `<masked:4d416364>`, `<masked:a1773d62>` | `attribute` |
| `campaign` | `string` | yes | 1 | 100% | `<text:41>` | `join_key` |
| `ch_hdr` | `string` | yes | 3 | 42.9% | `<text:41>`, `<text:41>`, `<text:41>` | `dimension` |
| `ch_js` | `string` | yes | 3 | 42.9% | `<structured>`, `<structured>`, `<structured>` | `dimension` |
| `channel` | `string` | no | 1 | 10% | `<masked:ac4743cc>`, `<masked:ac4743cc>`, `<masked:ac4743cc>` | `dimension` |
| `click_action_type` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `click_context_type` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `click_sourceid` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `code_ver` | `string` | no | 1 | 10% | `<masked:2dbf03ee>`, `<masked:2dbf03ee>`, `<masked:2dbf03ee>` | `dimension` |
| `color` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `connection_type` | `integer` | no | 1 | 10% | `<masked:da4b9237>`, `<masked:da4b9237>`, `<masked:da4b9237>` | `attribute` |
| `cookies` | `string` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `country` | `integer` | no | 3 | 30% | `<masked:ca3512f4>`, `<masked:ca3512f4>`, `<masked:79816ecb>` | `attribute` |
| `curr_factor` | `integer` | no | 1 | 10% | `<masked:da4b9237>`, `<masked:da4b9237>`, `<masked:da4b9237>` | `attribute` |
| `curr_rate` | `float` | no | 1 | 10% | `<masked:e8dc057d>`, `<masked:e8dc057d>`, `<masked:e8dc057d>` | `attribute` |
| `cust_hit_time_gmt` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `timestamp` |
| `daily_visitor` | `integer` | no | 2 | 20% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `date_time` | `string` | no | 10 | 100% | `<masked:58dd4585>`, `<masked:46171674>`, `<masked:68b832ab>` | `timestamp` |
| `domain` | `string` | no | 5 | 50% | `<masked:2619f7fd>`, `<masked:2619f7fd>`, `<masked:189826d0>` | `dimension` |
| `duplicate_purchase` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `evar101` | `string` | no | 6 | 60% | `<masked:9d56b8db>`, `<masked:9d56b8db>`, `<text:41>` | `dimension` |
| `evar102` | `string` | no | 1 | 10% | `<masked:f0341797>`, `<masked:f0341797>`, `<masked:f0341797>` | `dimension` |
| `evar103` | `string` | no | 1 | 10% | `<masked:f37f72c0>`, `<masked:f37f72c0>`, `<masked:f37f72c0>` | `dimension` |
| `evar104` | `string` | no | 1 | 10% | `<masked:fb06270f>`, `<masked:fb06270f>`, `<masked:fb06270f>` | `dimension` |
| `evar105` | `string` | no | 1 | 10% | `<masked:bb540fd7>`, `<masked:bb540fd7>`, `<masked:bb540fd7>` | `dimension` |
| `evar106` | `string` | no | 1 | 10% | `<masked:77aa2f13>`, `<masked:77aa2f13>`, `<masked:77aa2f13>` | `dimension` |
| `evar107` | `string` | no | 6 | 60% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `evar108` | `string` | no | 6 | 60% | `<text:41>`, `<text:41>`, `<text:41>` | `dimension` |
| `evar109` | `string` | no | 1 | 10% | `<masked:094b0fe0>`, `<masked:094b0fe0>`, `<masked:094b0fe0>` | `dimension` |
| `evar131` | `string` | no | 7 | 70% | `<hash>`, `<hash>`, `<hash>` | `dimension` |
| `evar137` | `string` | no | 1 | 10% | `<masked:131c006f>`, `<masked:131c006f>`, `<masked:131c006f>` | `dimension` |
| `evar138` | `string` | no | 1 | 10% | `<masked:4ed04285>`, `<masked:4ed04285>`, `<masked:4ed04285>` | `dimension` |
| `evar140` | `string` | yes | 4 | 100% | `<hash>`, `<hash>`, `<hash>` | `join_key` |
| `evar144` | `string` | no | 7 | 70% | `<masked:9d56b8db>`, `<masked:9d56b8db>`, `<text:41>` | `dimension` |
| `evar145` | `string` | no | 2 | 20% | `<masked:c2a6b03f>`, `<masked:c0ac4842>`, `<masked:c0ac4842>` | `dimension` |
| `evar200` | `string` | no | 7 | 70% | `<structured>`, `<structured>`, `<structured>` | `dimension` |
| `event_list` | `string` | no | 3 | 30% | `<text:41>`, `<text:41>`, `<text:41>` | `dimension` |
| `exclude_hit` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `first_hit_page_url` | `string` | yes | 3 | 37.5% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `first_hit_pagename` | `string` | yes | 3 | 37.5% | `<masked:9d56b8db>`, `<text:41>`, `<masked:9d56b8db>` | `dimension` |
| `first_hit_ref_domain` | `string` | yes | 1 | 33.3% | `<masked:baea954b>`, `<masked:baea954b>`, `<masked:baea954b>` | `dimension` |
| `first_hit_ref_type` | `integer` | no | 2 | 20% | `<masked:da4b9237>`, `<masked:c1dfd96e>`, `<masked:da4b9237>` | `attribute` |
| `first_hit_referrer` | `string` | yes | 1 | 33.3% | `<url>`, `<url>`, `<url>` | `dimension` |
| `first_hit_time_gmt` | `integer` | no | 7 | 70% | `<masked:fdfe6e5d>`, `<masked:ea1aeec8>`, `<masked:39ceb0dd>` | `timestamp` |
| `geo_city` | `string` | no | 6 | 60% | `<masked:496809a9>`, `<masked:2ed0a613>`, `<masked:fba07459>` | `dimension` |
| `geo_country` | `string` | no | 1 | 10% | `<masked:7e9219a0>`, `<masked:7e9219a0>`, `<masked:7e9219a0>` | `dimension` |
| `geo_dma` | `integer` | no | 4 | 40% | `<masked:3fc134c3>`, `<masked:3fc134c3>`, `<masked:1c50ba4a>` | `attribute` |
| `geo_region` | `string` | no | 3 | 30% | `<masked:db3d405b>`, `<masked:db3d405b>`, `<masked:da23614e>` | `dimension` |
| `geo_zip` | `string` | no | 6 | 60% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `hit_source` | `integer` | no | 1 | 10% | `<masked:356a192b>`, `<masked:356a192b>`, `<masked:356a192b>` | `attribute` |
| `hit_time_gmt` | `integer` | no | 10 | 100% | `<masked:fdfe6e5d>`, `<masked:3ea1da53>`, `<masked:b1edd9ae>` | `timestamp` |
| `hitid_high` | `integer` | no | 10 | 100% | `<hash>`, `<hash>`, `<hash>` | `join_key` |
| `hitid_low` | `integer` | no | 10 | 100% | `<hash>`, `<hash>`, `<hash>` | `join_key` |
| `homepage` | `string` | no | 1 | 10% | `<masked:b2c7c0ca>`, `<masked:b2c7c0ca>`, `<masked:b2c7c0ca>` | `dimension` |
| `hourly_visitor` | `integer` | no | 2 | 20% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `ip` | `string` | no | 7 | 70% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `j_jscript` | `float` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `java_enabled` | `string` | no | 1 | 10% | `<masked:b2c7c0ca>`, `<masked:b2c7c0ca>`, `<masked:b2c7c0ca>` | `dimension` |
| `javascript` | `integer` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `language` | `integer` | no | 2 | 20% | `<masked:ca3512f4>`, `<masked:fb644351>`, `<masked:ca3512f4>` | `attribute` |
| `last_hit_time_gmt` | `integer` | no | 9 | 90% | `<masked:b6589fc6>`, `<masked:658a5b61>`, `<masked:96a8de80>` | `timestamp` |
| `last_purchase_num` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `last_purchase_time_gmt` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `timestamp` |
| `mcvisid` | `string` | no | 7 | 70% | `<hash>`, `<hash>`, `<hash>` | `dimension` |
| `mobile_id` | `integer` | no | 3 | 30% | `<masked:b2575a07>`, `<masked:b6589fc6>`, `<masked:b51af334>` | `join_key` |
| `monthly_visitor` | `integer` | no | 2 | 20% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `new_visit` | `integer` | no | 2 | 20% | `<masked:356a192b>`, `<masked:356a192b>`, `<masked:356a192b>` | `attribute` |
| `os` | `integer` | no | 5 | 50% | `<masked:aa90e8d7>`, `<masked:abb45d13>`, `<masked:e3a0a490>` | `attribute` |
| `page_event` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `page_url` | `string` | no | 6 | 60% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `pagename` | `string` | no | 6 | 60% | `<masked:9d56b8db>`, `<masked:9d56b8db>`, `<text:41>` | `dimension` |
| `paid_search` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `persistent_cookie` | `string` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `post_browser_height` | `integer` | no | 7 | 70% | `<masked:b66cd90e>`, `<masked:4707f922>`, `<masked:f7b41d20>` | `attribute` |
| `post_browser_width` | `integer` | no | 7 | 70% | `<masked:f369b411>`, `<masked:4d416364>`, `<masked:a1773d62>` | `attribute` |
| `post_campaign` | `string` | yes | 1 | 100% | `<text:41>` | `join_key` |
| `post_channel` | `string` | no | 1 | 10% | `<masked:ac4743cc>`, `<masked:ac4743cc>`, `<masked:ac4743cc>` | `dimension` |
| `post_clickmaplink` | `string` | yes | 1 | 100% | `<masked:c78eddce>` | `join_key` |
| `post_clickmaplinkbyregion` | `string` | yes | 1 | 100% | `<structured>` | `join_key` |
| `post_clickmappage` | `string` | yes | 1 | 100% | `<text:41>` | `join_key` |
| `post_clickmapregion` | `string` | yes | 1 | 100% | `<masked:07339575>` | `join_key` |
| `post_cookies` | `string` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `post_currency` | `string` | no | 1 | 10% | `<masked:b821b203>`, `<masked:b821b203>`, `<masked:b821b203>` | `dimension` |
| `post_cust_hit_time_gmt` | `integer` | no | 10 | 100% | `<masked:fdfe6e5d>`, `<masked:3ea1da53>`, `<masked:b1edd9ae>` | `timestamp` |
| `post_evar101` | `string` | no | 6 | 60% | `<masked:9d56b8db>`, `<masked:9d56b8db>`, `<text:41>` | `dimension` |
| `post_evar102` | `string` | no | 1 | 10% | `<masked:f0341797>`, `<masked:f0341797>`, `<masked:f0341797>` | `dimension` |
| `post_evar103` | `string` | no | 1 | 10% | `<masked:f37f72c0>`, `<masked:f37f72c0>`, `<masked:f37f72c0>` | `dimension` |
| `post_evar104` | `string` | no | 1 | 10% | `<masked:fb06270f>`, `<masked:fb06270f>`, `<masked:fb06270f>` | `dimension` |
| `post_evar105` | `string` | no | 1 | 10% | `<masked:bb540fd7>`, `<masked:bb540fd7>`, `<masked:bb540fd7>` | `dimension` |
| `post_evar106` | `string` | no | 1 | 10% | `<masked:77aa2f13>`, `<masked:77aa2f13>`, `<masked:77aa2f13>` | `dimension` |
| `post_evar107` | `string` | no | 6 | 60% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `post_evar108` | `string` | no | 6 | 60% | `<text:41>`, `<text:41>`, `<text:41>` | `dimension` |
| `post_evar109` | `string` | no | 1 | 10% | `<masked:094b0fe0>`, `<masked:094b0fe0>`, `<masked:094b0fe0>` | `dimension` |
| `post_evar131` | `string` | no | 7 | 70% | `<hash>`, `<hash>`, `<hash>` | `dimension` |
| `post_evar137` | `string` | no | 1 | 10% | `<masked:131c006f>`, `<masked:131c006f>`, `<masked:131c006f>` | `dimension` |
| `post_evar138` | `string` | no | 1 | 10% | `<masked:4ed04285>`, `<masked:4ed04285>`, `<masked:4ed04285>` | `dimension` |
| `post_evar140` | `string` | yes | 5 | 62.5% | `<hash>`, `<hash>`, `<hash>` | `dimension` |
| `post_evar144` | `string` | no | 7 | 70% | `<masked:9d56b8db>`, `<masked:9d56b8db>`, `<text:41>` | `dimension` |
| `post_evar145` | `string` | no | 2 | 20% | `<masked:c2a6b03f>`, `<masked:c0ac4842>`, `<masked:c0ac4842>` | `dimension` |
| `post_evar193` | `string` | yes | 1 | 25% | `<masked:c78eddce>`, `<masked:c78eddce>`, `<masked:c78eddce>` | `dimension` |
| `post_evar194` | `string` | yes | 3 | 75% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `post_evar200` | `string` | no | 7 | 70% | `<structured>`, `<structured>`, `<structured>` | `dimension` |
| `post_event_list` | `string` | no | 4 | 40% | `<text:41>`, `<text:41>`, `<text:41>` | `dimension` |
| `post_java_enabled` | `string` | no | 1 | 10% | `<masked:b2c7c0ca>`, `<masked:b2c7c0ca>`, `<masked:b2c7c0ca>` | `dimension` |
| `post_keywords` | `string` | yes | 1 | 33.3% | `<masked:f6c16169>`, `<masked:f6c16169>`, `<masked:f6c16169>` | `dimension` |
| `post_page_event` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `post_page_url` | `string` | no | 6 | 60% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `post_pagename` | `string` | no | 6 | 60% | `<masked:9d56b8db>`, `<masked:9d56b8db>`, `<text:41>` | `dimension` |
| `post_pagename_no_url` | `string` | no | 6 | 60% | `<masked:9d56b8db>`, `<masked:9d56b8db>`, `<text:41>` | `dimension` |
| `post_persistent_cookie` | `string` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `post_product_list` | `string` | no | 1 | 10% | `<masked:e32a5d37>`, `<masked:e32a5d37>`, `<masked:e32a5d37>` | `dimension` |
| `post_prop51` | `string` | no | 6 | 60% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `post_prop52` | `string` | no | 6 | 60% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `post_prop53` | `boolean` | no | 1 | 10% | `<masked:97cdbdc7>`, `<masked:97cdbdc7>`, `<masked:97cdbdc7>` | `dimension` |
| `post_prop54` | `string` | no | 1 | 10% | `<masked:094b0fe0>`, `<masked:094b0fe0>`, `<masked:094b0fe0>` | `dimension` |
| `post_prop55` | `string` | yes | 3 | 42.9% | `<url>`, `<url>`, `<url>` | `dimension` |
| `post_prop56` | `string` | no | 1 | 10% | `<masked:594fd161>`, `<masked:594fd161>`, `<masked:594fd161>` | `dimension` |
| `post_prop57` | `boolean` | no | 2 | 20% | `<masked:88b33e4e>`, `<masked:97cdbdc7>`, `<masked:97cdbdc7>` | `dimension` |
| `post_referrer` | `string` | yes | 3 | 42.9% | `<url>`, `<url>`, `<url>` | `dimension` |
| `post_search_engine` | `integer` | no | 2 | 20% | `<masked:9109c85a>`, `<masked:b6589fc6>`, `<masked:9109c85a>` | `attribute` |
| `post_t_time_info` | `string` | no | 7 | 70% | `<masked:0d73b1d7>`, `<masked:2162320a>`, `<masked:d93a5e99>` | `timestamp` |
| `post_tnt` | `string` | yes | 1 | 16.7% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `post_visid_high` | `integer` | no | 7 | 70% | `<hash>`, `<hash>`, `<hash>` | `attribute` |
| `post_visid_low` | `integer` | no | 7 | 70% | `<hash>`, `<hash>`, `<hash>` | `attribute` |
| `post_visid_type` | `integer` | no | 1 | 10% | `<masked:ac3478d6>`, `<masked:ac3478d6>`, `<masked:ac3478d6>` | `attribute` |
| `post_zip` | `string` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `prev_page` | `integer` | no | 4 | 40% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `prop51` | `string` | no | 6 | 60% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `prop52` | `string` | no | 6 | 60% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `prop53` | `boolean` | no | 1 | 10% | `<masked:97cdbdc7>`, `<masked:97cdbdc7>`, `<masked:97cdbdc7>` | `dimension` |
| `prop54` | `string` | no | 1 | 10% | `<masked:094b0fe0>`, `<masked:094b0fe0>`, `<masked:094b0fe0>` | `dimension` |
| `prop55` | `string` | yes | 3 | 42.9% | `<url>`, `<url>`, `<url>` | `dimension` |
| `prop56` | `string` | no | 1 | 10% | `<masked:594fd161>`, `<masked:594fd161>`, `<masked:594fd161>` | `dimension` |
| `prop57` | `boolean` | no | 2 | 20% | `<masked:88b33e4e>`, `<masked:97cdbdc7>`, `<masked:97cdbdc7>` | `dimension` |
| `quarterly_visitor` | `integer` | no | 2 | 20% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `ref_domain` | `string` | yes | 1 | 33.3% | `<masked:baea954b>`, `<masked:baea954b>`, `<masked:baea954b>` | `dimension` |
| `ref_type` | `integer` | no | 3 | 30% | `<masked:77de68da>`, `<masked:c1dfd96e>`, `<masked:77de68da>` | `attribute` |
| `referrer` | `string` | yes | 3 | 42.9% | `<url>`, `<url>`, `<url>` | `dimension` |
| `resolution` | `integer` | no | 5 | 50% | `<masked:d5f0d910>`, `<masked:87d538ef>`, `<masked:5f573b82>` | `attribute` |
| `s_resolution` | `string` | no | 6 | 60% | `<masked:6570f98b>`, `<masked:35a4248d>`, `<masked:4bdf8c91>` | `dimension` |
| `sampled_hit` | `string` | no | 1 | 10% | `<masked:23eb4d3f>`, `<masked:23eb4d3f>`, `<masked:23eb4d3f>` | `dimension` |
| `search_engine` | `integer` | no | 2 | 20% | `<masked:9109c85a>`, `<masked:b6589fc6>`, `<masked:9109c85a>` | `attribute` |
| `search_page_num` | `integer` | no | 2 | 20% | `<masked:356a192b>`, `<masked:b6589fc6>`, `<masked:356a192b>` | `attribute` |
| `secondary_hit` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `service` | `string` | no | 1 | 10% | `<masked:c1c93f88>`, `<masked:c1c93f88>`, `<masked:c1c93f88>` | `dimension` |
| `sourceid` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `stats_server` | `string` | no | 10 | 100% | `<masked:0f988ef5>`, `<masked:7b81fcaa>`, `<masked:3e9738d4>` | `join_key` |
| `t_time_info` | `string` | no | 10 | 100% | `<masked:0d73b1d7>`, `<masked:2162320a>`, `<masked:d93a5e99>` | `timestamp` |
| `truncated_hit` | `string` | no | 1 | 10% | `<masked:b51a6073>`, `<masked:b51a6073>`, `<masked:b51a6073>` | `dimension` |
| `user_agent` | `string` | no | 6 | 60% | `<text:41>`, `<text:41>`, `<text:41>` | `dimension` |
| `user_hash` | `integer` | no | 1 | 10% | `<masked:c0cc81d3>`, `<masked:c0cc81d3>`, `<masked:c0cc81d3>` | `attribute` |
| `userid` | `integer` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `username` | `string` | no | 1 | 10% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `va_closer_detail` | `string` | no | 4 | 40% | `<masked:44415ce7>`, `<masked:9d56b8db>`, `<masked:44415ce7>` | `dimension` |
| `va_closer_id` | `integer` | no | 4 | 40% | `<masked:77de68da>`, `<masked:f1abd670>`, `<masked:77de68da>` | `join_key` |
| `va_finder_detail` | `string` | no | 3 | 30% | `<masked:44415ce7>`, `<masked:9d56b8db>`, `<masked:44415ce7>` | `dimension` |
| `va_finder_id` | `integer` | no | 4 | 40% | `<masked:77de68da>`, `<masked:f1abd670>`, `<masked:77de68da>` | `join_key` |
| `va_instance_event` | `integer` | no | 2 | 20% | `<masked:356a192b>`, `<masked:356a192b>`, `<masked:356a192b>` | `attribute` |
| `va_new_engagement` | `integer` | no | 2 | 20% | `<masked:356a192b>`, `<masked:356a192b>`, `<masked:b6589fc6>` | `attribute` |
| `visid_high` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `visid_low` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `attribute` |
| `visid_new` | `string` | no | 1 | 10% | `<masked:b51a6073>`, `<masked:b51a6073>`, `<masked:b51a6073>` | `dimension` |
| `visid_timestamp` | `integer` | no | 1 | 10% | `<masked:b6589fc6>`, `<masked:b6589fc6>`, `<masked:b6589fc6>` | `timestamp` |
| `visid_type` | `integer` | no | 1 | 10% | `<masked:ac3478d6>`, `<masked:ac3478d6>`, `<masked:ac3478d6>` | `attribute` |
| `visit_keywords` | `string` | yes | 1 | 33.3% | `<masked:f6c16169>`, `<masked:f6c16169>`, `<masked:f6c16169>` | `dimension` |
| `visit_num` | `integer` | no | 5 | 50% | `<masked:356a192b>`, `<masked:ac3478d6>`, `<masked:ac3478d6>` | `attribute` |
| `visit_page_num` | `integer` | no | 5 | 50% | `<masked:356a192b>`, `<masked:356a192b>`, `<masked:356a192b>` | `attribute` |
| `visit_ref_domain` | `string` | yes | 1 | 33.3% | `<masked:baea954b>`, `<masked:baea954b>`, `<masked:baea954b>` | `dimension` |
| `visit_ref_type` | `integer` | no | 2 | 20% | `<masked:77de68da>`, `<masked:c1dfd96e>`, `<masked:77de68da>` | `attribute` |
| `visit_referrer` | `string` | yes | 1 | 33.3% | `<url>`, `<url>`, `<url>` | `dimension` |
| `visit_search_engine` | `integer` | no | 2 | 20% | `<masked:9109c85a>`, `<masked:b6589fc6>`, `<masked:9109c85a>` | `attribute` |
| `visit_start_page_url` | `string` | no | 4 | 40% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `visit_start_pagename` | `string` | no | 4 | 40% | `<masked:9d56b8db>`, `<masked:9d56b8db>`, `<text:41>` | `dimension` |
| `visit_start_time_gmt` | `integer` | no | 7 | 70% | `<masked:fdfe6e5d>`, `<masked:3ea1da53>`, `<masked:b1edd9ae>` | `timestamp` |
| `weekly_visitor` | `integer` | no | 2 | 20% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `yearly_visitor` | `integer` | no | 2 | 20% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `process_timestamp` | `datetime` | no | 1 | 10% | `<masked:adb620f5>`, `<masked:adb620f5>`, `<masked:adb620f5>` | `timestamp` |
| `Source_filename` | `string` | no | 1 | 10% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `process_date` | `datetime` | no | 1 | 10% | `<masked:2feb44b3>`, `<masked:2feb44b3>`, `<masked:2feb44b3>` | `timestamp` |
| `As_of_date` | `datetime` | no | 1 | 10% | `<masked:17858005>`, `<masked:17858005>`, `<masked:17858005>` | `timestamp` |
| `business_unit` | `string` | no | 1 | 10% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `site_url` | `string` | no | 1 | 10% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `sourcefile_name` | `string` | no | 1 | 10% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `rsid` | `string` | no | 1 | 10% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `site_name` | `string` | no | 1 | 10% | `<internal>`, `<internal>`, `<internal>` | `dimension` |
| `customer_perspective` | `integer` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `post_customer_perspective` | `integer` | no | 1 | 10% | _withheld (3 masked values)_ | `sensitive_identifier` |

<details><summary>Empty columns in sample (1008)</summary>

`adclassificationcreative`, `adload`, `aemassetid`, `aemassetsource`, `aemclickedassetid`, `c_color`, `carrier`, `click_action`, `click_context`, `click_tag`, `clickmaplink`, `clickmaplinkbyregion`, `clickmappage`, `clickmapregion`, `ct_connect_type`, `currency`, `cust_visid`, `dataprivacyconsentoptin`, `dataprivacyconsentoptout`, `duplicate_events`, `duplicated_from`, `ef_id`, `evar1`, `evar2`, `evar3`, `evar4`, `evar5`, `evar6`, `evar7`, `evar8`, `evar9`, `evar10`, `evar11`, `evar12`, `evar13`, `evar14`, `evar15`, `evar16`, `evar17`, `evar18`, `evar19`, `evar20`, `evar21`, `evar22`, `evar23`, `evar24`, `evar25`, `evar26`, `evar27`, `evar28`, `evar29`, `evar30`, `evar31`, `evar32`, `evar33`, `evar34`, `evar35`, `evar36`, `evar37`, `evar38`, … (+948 more)

</details>

#### 3. Time profile

| Column | Type | Null % | Distinct | Notes |
|---|---|---:|---:|---|
| `cust_hit_time_gmt` | `integer` | 0% | 1 | populated |
| `date_time` | `string` | 0% | 10 | populated |
| `first_hit_time_gmt` | `integer` | 0% | 7 | populated |
| `hit_time_gmt` | `integer` | 0% | 10 | populated |
| `last_hit_time_gmt` | `integer` | 0% | 9 | populated |
| `last_purchase_time_gmt` | `integer` | 0% | 1 | populated |
| `mobileactioninapptime` | `float` | 100% | 0 | empty in sample |
| `mobileactiontotaltime` | `float` | 100% | 0 | empty in sample |
| `mobileinstalldate` | `float` | 100% | 0 | empty in sample |
| `mobileplacedwelltime` | `float` | 100% | 0 | empty in sample |
| `post_cust_hit_time_gmt` | `integer` | 0% | 10 | populated |
| `post_mobileinstalldate` | `float` | 100% | 0 | empty in sample |
| `post_socialaveragesentiment` | `float` | 100% | 0 | empty in sample |
| `post_socialaveragesentiment_deprecated` | `float` | 100% | 0 | empty in sample |
| `post_socialtotalsentiment` | `float` | 100% | 0 | empty in sample |
| `post_t_time_info` | `string` | 0% | 7 | populated |
| `post_videoqoebuffertimeevar` | `float` | 100% | 0 | empty in sample |
| `post_videoqoetimetostartevar` | `float` | 100% | 0 | empty in sample |
| `socialaveragesentiment` | `float` | 100% | 0 | empty in sample |
| `socialaveragesentiment_deprecated` | `float` | 100% | 0 | empty in sample |
| `socialtotalsentiment` | `float` | 100% | 0 | empty in sample |
| `t_time_info` | `string` | 0% | 10 | populated |
| `videochaptertime` | `float` | 100% | 0 | empty in sample |
| `videopausetime` | `float` | 100% | 0 | empty in sample |
| `videoqoebuffertimeevar` | `float` | 100% | 0 | empty in sample |
| `videoqoetimetostartevar` | `float` | 100% | 0 | empty in sample |
| `videototaltime` | `float` | 100% | 0 | empty in sample |
| `videouniquetimeplayed` | `float` | 100% | 0 | empty in sample |
| `visid_timestamp` | `integer` | 0% | 1 | populated |
| `visit_start_time_gmt` | `integer` | 0% | 7 | populated |
| `process_timestamp` | `datetime` | 0% | 1 | populated |
| `process_date` | `datetime` | 0% | 1 | populated |
| `As_of_date` | `datetime` | 0% | 1 | populated |

**Verdict.** Timestamp columns populated but only 10 rows — confirms feed shape; volume insufficient for baselines.

#### 4. Metric profile

_No columns matched the metric-name heuristic in this sample._ (Adobe hit-level feeds emit metrics implicitly via `post_event_list` — aggregation happens downstream.)

#### 5. Dimension profile

| Dimension | Cardinality | Missing % | Segmentation | Join key? |
|---|---:|---:|:---:|:---:|
| `evar145` | 2 | 0% | high | — |
| `post_evar145` | 2 | 0% | high | — |
| `post_prop57` | 2 | 0% | high | — |
| `prop57` | 2 | 0% | high | — |
| `ch_hdr` | 3 | 30% | high | — |
| `ch_js` | 3 | 30% | high | — |
| `event_list` | 3 | 0% | high | — |
| `first_hit_page_url` | 3 | 20% | high | — |
| `first_hit_pagename` | 3 | 20% | high | — |
| `geo_region` | 3 | 0% | high | — |
| `post_evar194` | 3 | 60% | high | — |
| `post_prop55` | 3 | 30% | high | — |
| `post_referrer` | 3 | 30% | high | — |
| `prop55` | 3 | 30% | high | — |
| `referrer` | 3 | 30% | high | — |
| `va_finder_detail` | 3 | 0% | high | — |
| `accept_language` | 4 | 0% | high | — |
| `evar140` | 4 | 60% | high | ✅ |
| `post_event_list` | 4 | 0% | high | — |
| `va_closer_detail` | 4 | 0% | high | — |
| `visit_start_page_url` | 4 | 0% | high | — |
| `visit_start_pagename` | 4 | 0% | high | — |
| `domain` | 5 | 0% | high | — |
| `post_evar140` | 5 | 20% | high | — |
| `evar101` | 6 | 0% | high | — |
| `evar107` | 6 | 0% | high | — |
| `evar108` | 6 | 0% | high | — |
| `geo_city` | 6 | 0% | high | — |
| `geo_zip` | 6 | 0% | high | — |
| `page_url` | 6 | 0% | high | — |
| `pagename` | 6 | 0% | high | — |
| `post_evar101` | 6 | 0% | high | — |
| `post_evar107` | 6 | 0% | high | — |
| `post_evar108` | 6 | 0% | high | — |
| `post_page_url` | 6 | 0% | high | — |
| `post_pagename` | 6 | 0% | high | — |
| `post_pagename_no_url` | 6 | 0% | high | — |
| `post_prop51` | 6 | 0% | high | — |
| `post_prop52` | 6 | 0% | high | — |
| `prop51` | 6 | 0% | high | — |
| … (+77 additional dimensions in `data_profile_summary.json`) | | | | |

#### 6. Data quality profile

- Duplicate rows in sample: **0**
- Constant-valued populated columns (distinct=1): **94**
- High-cardinality (>1000 distinct): **0**
- Numeric columns with ≥50% zero values: **29**
- Numeric columns with negative values: **0**
- Numeric columns flagged as outlier-bearing: **9**
- Sensitive columns (names only): **24**
  - `cookies`, `daily_visitor`, `geo_zip`, `hourly_visitor`, `ip`, `ip2`, `ipv6`, `j_jscript`, `javascript`, `monthly_visitor`, `persistent_cookie`, `post_cookies`, … (+12 more)

#### 7. Joinability profile

| Direction | Other file | Via | Match | Coverage | Type | Confidence |
|---|---|---|---:|---:|---|---|
| inbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=data_feed_columns` | headers ← `Column name` | 215 | 95.98% | `dictionary_lookup` | High |
| inbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | headers ← `Column` | 93 | 100.0% | `slot_expansion` | High |
| inbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | headers ← `Column` | 26 | 100.0% | `slot_expansion` | High |

**Join keys not yet in the corpus** but expected once other sources arrive: `url`, `page_path`, `deployment_id`, `release_id`, `campaign_id`, `content_id`, `tag_name`, `owner`, `site_id`, `brand`, `region`.

#### 8. Anomaly-detection readiness

| Property | Value |
|---|---|
| Relevance score (1–10) | **8** |
| Readiness score (1–10) | **8** |
| Confidence | **Medium** |
| Recommended role | 🔵 **enrichment/dimension data** |

**Rationale**

- parse_status=ok
- row_count=10
- timestamp_candidates=33
- metric_candidates=1
- dimension_candidates=117
- row volume below 1k — insufficient for stable baselines

---

### 2. CoverMe Adobe Analytics Data Dictionary Web - Data Feed — sheet `data_feed_columns`

#### 1. Dataset overview

| Property | Value |
|---|---|
| Path | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=data_feed_columns` |
| Sheet | `data_feed_columns` |
| Format | `.xlsx` |
| Business domain | `web_analytics` |
| Parse status | `ok` |
| Row count (true) | 224 |
| Sampled rows | 224 |
| Column count | 4 |
| Duplicate rows | 0 |
| Apparent grain | `reference/lookup` |

#### 2. Schema profile

- Total columns: **4** (populated: **4**, empty in sample: **0**)
- Sensitive-flagged columns: **1** (values are never displayed in this document)

Populated columns — masked / abstracted examples only:

| Column | Type | Nullable | Distinct | Cardinality | Example pattern | Semantic |
|---|---|---:|---:|---|---|---|
| `Column name` | `string` | no | 224 | 100% | `accept_language`, `adload`, `aemassetid` | `join_key` |
| `Description` | `string` | no | 224 | 100% | _withheld (3 masked values)_ | `sensitive_identifier` |
| `Data type` | `string` | no | 26 | 11.6% | `char(20)`, `varchar(255)`, `text` | `dimension` |
| `Notes` | `string` | yes | 1 | 11.1% | `Anomaly Detection`, `Anomaly Detection`, `Anomaly Detection` | `dimension` |

#### 3. Time profile

_No timestamp candidates detected._

#### 4. Metric profile

_No columns matched the metric-name heuristic in this sample._ (Adobe hit-level feeds emit metrics implicitly via `post_event_list` — aggregation happens downstream.)

#### 5. Dimension profile

| Dimension | Cardinality | Missing % | Segmentation | Join key? |
|---|---:|---:|:---:|:---:|
| `Data type` | 26 | 0% | medium | — |
| `Notes` | 1 | 96.0% | low | — |

#### 6. Data quality profile

- Duplicate rows in sample: **0**
- Constant-valued populated columns (distinct=1): **1**
- High-cardinality (>1000 distinct): **0**
- Numeric columns with ≥50% zero values: **0**
- Numeric columns with negative values: **0**
- Numeric columns flagged as outlier-bearing: **0**
- Sensitive columns (names only): **1**
  - `Description`

#### 7. Joinability profile

| Direction | Other file | Via | Match | Coverage | Type | Confidence |
|---|---|---|---:|---:|---|---|
| outbound | `Canada Retirement.xlsx#sheet=result` | `Column name` → headers | 215 | 95.98% | `dictionary_lookup` | High |

**Join keys not yet in the corpus** but expected once other sources arrive: `url`, `page_path`, `deployment_id`, `release_id`, `campaign_id`, `content_id`, `tag_name`, `owner`, `site_id`, `brand`, `region`.

#### 8. Anomaly-detection readiness

| Property | Value |
|---|---|
| Relevance score (1–10) | **4** |
| Readiness score (1–10) | **6** |
| Confidence | **Low** |
| Recommended role | 🔵 **enrichment/dimension data** |

**Rationale**

- parse_status=ok
- row_count=224
- no timestamp candidates
- no metric candidates matched via name heuristic
- dimension_candidates=2
- row volume below 1k — insufficient for stable baselines

---

### 3. CoverMe Adobe Analytics Data Dictionary Web - Data Feed — sheet `post_eVar`

#### 1. Dataset overview

| Property | Value |
|---|---|
| Path | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` |
| Sheet | `post_eVar` |
| Format | `.xlsx` |
| Business domain | `web_analytics` |
| Parse status | `ok` |
| Row count (true) | 93 |
| Sampled rows | 93 |
| Column count | 4 |
| Duplicate rows | 0 |
| Apparent grain | `reference/lookup` |

#### 2. Schema profile

- Total columns: **4** (populated: **4**, empty in sample: **0**)
- Sensitive-flagged columns: **0** (values are never displayed in this document)

Populated columns — masked / abstracted examples only:

| Column | Type | Nullable | Distinct | Cardinality | Example pattern | Semantic |
|---|---|---:|---:|---|---|---|
| `Column` | `string` | no | 93 | 100% | `post_evar1`, `post_evar2`, `post_evar3` | `join_key` |
| `Name` | `string` | no | 92 | 98.9% | `Time Stamp`, `New/Repeat Visitors`, `Visit Number` | `join_key` |
| `Status` | `string` | no | 1 | 1.1% | `Enabled`, `Enabled`, `Enabled` | `dimension` |
| `Notes` | `string` | yes | 1 | 12.5% | `Anomaly Detection`, `Anomaly Detection`, `Anomaly Detection` | `dimension` |

#### 3. Time profile

_No timestamp candidates detected._

#### 4. Metric profile

_No columns matched the metric-name heuristic in this sample._ (Adobe hit-level feeds emit metrics implicitly via `post_event_list` — aggregation happens downstream.)

#### 5. Dimension profile

| Dimension | Cardinality | Missing % | Segmentation | Join key? |
|---|---:|---:|:---:|:---:|
| `Status` | 1 | 0% | low | — |
| `Notes` | 1 | 91.4% | low | — |

#### 6. Data quality profile

- Duplicate rows in sample: **0**
- Constant-valued populated columns (distinct=1): **2**
- High-cardinality (>1000 distinct): **0**
- Numeric columns with ≥50% zero values: **0**
- Numeric columns with negative values: **0**
- Numeric columns flagged as outlier-bearing: **0**
- Sensitive columns (names only): **0**

#### 7. Joinability profile

| Direction | Other file | Via | Match | Coverage | Type | Confidence |
|---|---|---|---:|---:|---|---|
| outbound | `Canada Retirement.xlsx#sheet=result` | `Column` → headers | 93 | 100.0% | `slot_expansion` | High |
| outbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | `Name` → headers | 1 | 1.09% | `slot_expansion` | Low |
| inbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_event_list` | headers ← `Friendly Name` | 1 | 0.7% | `slot_expansion` | Low |
| outbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | `Column` → headers | 1 | — | `exact_name` | Medium |
| outbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | `Name` → headers | 1 | — | `exact_name` | Medium |

**Join keys not yet in the corpus** but expected once other sources arrive: `url`, `page_path`, `deployment_id`, `release_id`, `campaign_id`, `content_id`, `tag_name`, `owner`, `site_id`, `brand`, `region`.

#### 8. Anomaly-detection readiness

| Property | Value |
|---|---|
| Relevance score (1–10) | **4** |
| Readiness score (1–10) | **6** |
| Confidence | **Low** |
| Recommended role | 🔵 **enrichment/dimension data** |

**Rationale**

- parse_status=ok
- row_count=93
- no timestamp candidates
- no metric candidates matched via name heuristic
- dimension_candidates=2
- row volume below 1k — insufficient for stable baselines

---

### 4. CoverMe Adobe Analytics Data Dictionary Web - Data Feed — sheet `post_prop`

#### 1. Dataset overview

| Property | Value |
|---|---|
| Path | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` |
| Sheet | `post_prop` |
| Format | `.xlsx` |
| Business domain | `web_analytics` |
| Parse status | `ok` |
| Row count (true) | 26 |
| Sampled rows | 26 |
| Column count | 3 |
| Duplicate rows | 0 |
| Apparent grain | `reference/lookup` |

#### 2. Schema profile

- Total columns: **3** (populated: **3**, empty in sample: **0**)
- Sensitive-flagged columns: **0** (values are never displayed in this document)

Populated columns — masked / abstracted examples only:

| Column | Type | Nullable | Distinct | Cardinality | Example pattern | Semantic |
|---|---|---:|---:|---|---|---|
| `Column` | `string` | no | 26 | 100% | `post_prop1`, `post_prop2`, `post_prop3` | `join_key` |
| `Name` | `string` | no | 26 | 100% | `Product Category`, `Product ID`, `<text:31>` | `join_key` |
| `Status` | `string` | no | 1 | 3.8% | `Enabled`, `Enabled`, `Enabled` | `dimension` |

#### 3. Time profile

_No timestamp candidates detected._

#### 4. Metric profile

_No columns matched the metric-name heuristic in this sample._ (Adobe hit-level feeds emit metrics implicitly via `post_event_list` — aggregation happens downstream.)

#### 5. Dimension profile

| Dimension | Cardinality | Missing % | Segmentation | Join key? |
|---|---:|---:|:---:|:---:|
| `Column` | 26 | 0% | medium | ✅ |
| `Name` | 26 | 0% | medium | ✅ |
| `Status` | 1 | 0% | low | — |

#### 6. Data quality profile

- Duplicate rows in sample: **0**
- Constant-valued populated columns (distinct=1): **1**
- High-cardinality (>1000 distinct): **0**
- Numeric columns with ≥50% zero values: **0**
- Numeric columns with negative values: **0**
- Numeric columns flagged as outlier-bearing: **0**
- Sensitive columns (names only): **0**

#### 7. Joinability profile

| Direction | Other file | Via | Match | Coverage | Type | Confidence |
|---|---|---|---:|---:|---|---|
| inbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | headers ← `Name` | 1 | 1.09% | `slot_expansion` | Low |
| outbound | `Canada Retirement.xlsx#sheet=result` | `Column` → headers | 26 | 100.0% | `slot_expansion` | High |
| inbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_event_list` | headers ← `Friendly Name` | 1 | 0.7% | `slot_expansion` | Low |
| inbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | headers ← `Column` | 1 | — | `exact_name` | Medium |
| inbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | headers ← `Name` | 1 | — | `exact_name` | Medium |

**Join keys not yet in the corpus** but expected once other sources arrive: `url`, `page_path`, `deployment_id`, `release_id`, `campaign_id`, `content_id`, `tag_name`, `owner`, `site_id`, `brand`, `region`.

#### 8. Anomaly-detection readiness

| Property | Value |
|---|---|
| Relevance score (1–10) | **4** |
| Readiness score (1–10) | **6** |
| Confidence | **Low** |
| Recommended role | 🔵 **enrichment/dimension data** |

**Rationale**

- parse_status=ok
- row_count=26
- no timestamp candidates
- no metric candidates matched via name heuristic
- dimension_candidates=3
- row volume below 1k — insufficient for stable baselines

---

### 5. CoverMe Adobe Analytics Data Dictionary Web - Data Feed — sheet `post_event_list`

#### 1. Dataset overview

| Property | Value |
|---|---|
| Path | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_event_list` |
| Sheet | `post_event_list` |
| Format | `.xlsx` |
| Business domain | `web_analytics` |
| Parse status | `ok` |
| Row count (true) | 156 |
| Sampled rows | 156 |
| Column count | 5 |
| Duplicate rows | 0 |
| Apparent grain | `reference/lookup` |

#### 2. Schema profile

- Total columns: **5** (populated: **5**, empty in sample: **0**)
- Sensitive-flagged columns: **0** (values are never displayed in this document)

Populated columns — masked / abstracted examples only:

| Column | Type | Nullable | Distinct | Cardinality | Example pattern | Semantic |
|---|---|---:|---:|---|---|---|
| `post_event_list number` | `integer` | no | 156 | 100% | `1`, `2`, `10` | `join_key` |
| `Event` | `string` | no | 156 | 100% | `Purchase`, `Product View`, `Cart Open` | `join_key` |
| `Friendly Name` | `string` | yes | 143 | 97.9% | `Time Stamp`, `New/Repeat Visitors`, `Visit Number` | `join_key` |
| `Status` | `string` | yes | 2 | 1.4% | `Enabled`, `Enabled`, `Enabled` | `dimension` |
| `Notes` | `string` | yes | 1 | 8.3% | `Anomaly Detection`, `Anomaly Detection`, `Anomaly Detection` | `dimension` |

#### 3. Time profile

_No timestamp candidates detected._

#### 4. Metric profile

_No columns matched the metric-name heuristic in this sample._ (Adobe hit-level feeds emit metrics implicitly via `post_event_list` — aggregation happens downstream.)

#### 5. Dimension profile

| Dimension | Cardinality | Missing % | Segmentation | Join key? |
|---|---:|---:|:---:|:---:|
| `Status` | 2 | 5.1% | high | — |
| `Notes` | 1 | 92.3% | low | — |

#### 6. Data quality profile

- Duplicate rows in sample: **0**
- Constant-valued populated columns (distinct=1): **1**
- High-cardinality (>1000 distinct): **0**
- Numeric columns with ≥50% zero values: **0**
- Numeric columns with negative values: **0**
- Numeric columns flagged as outlier-bearing: **1**
- Sensitive columns (names only): **0**

#### 7. Joinability profile

| Direction | Other file | Via | Match | Coverage | Type | Confidence |
|---|---|---|---:|---:|---|---|
| outbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | `Friendly Name` → headers | 1 | 0.7% | `slot_expansion` | Low |
| outbound | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | `Friendly Name` → headers | 1 | 0.7% | `slot_expansion` | Low |

**Join keys not yet in the corpus** but expected once other sources arrive: `url`, `page_path`, `deployment_id`, `release_id`, `campaign_id`, `content_id`, `tag_name`, `owner`, `site_id`, `brand`, `region`.

#### 8. Anomaly-detection readiness

| Property | Value |
|---|---|
| Relevance score (1–10) | **4** |
| Readiness score (1–10) | **6** |
| Confidence | **Low** |
| Recommended role | 🔵 **enrichment/dimension data** |

**Rationale**

- parse_status=ok
- row_count=156
- no timestamp candidates
- no metric candidates matched via name heuristic
- dimension_candidates=2
- row volume below 1k — insufficient for stable baselines

---

## Corpus-wide joinability

| # | Source | Column | Target | Join type | Match | Coverage | Confidence |
|---:|---|---|---|---|---:|---:|---|
| 1 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=data_feed_columns` | `Column name` | `Canada Retirement.xlsx#sheet=result` | `dictionary_lookup` | 215 | 95.98% | High |
| 2 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | `Column` | `Canada Retirement.xlsx#sheet=result` | `slot_expansion` | 93 | 100.0% | High |
| 3 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | `Name` | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | `slot_expansion` | 1 | 1.09% | Low |
| 4 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | `Column` | `Canada Retirement.xlsx#sheet=result` | `slot_expansion` | 26 | 100.0% | High |
| 5 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_event_list` | `Friendly Name` | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | `slot_expansion` | 1 | 0.7% | Low |
| 6 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_event_list` | `Friendly Name` | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | `slot_expansion` | 1 | 0.7% | Low |
| 7 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | `Column` | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | `exact_name` | 1 | — | Medium |
| 8 | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_eVar` | `Name` | `CoverMe Adobe Analytics Data Dictionary Web - Data Feed.xlsx#sheet=post_prop` | `exact_name` | 1 | — | Medium |

**Missing but expected join keys.** These join keys are not present in the current corpus but the request calls them out as likely correlation keys once other sources arrive: `url`, `page_path`, `deployment_id`, `release_id`, `campaign_id`, `content_id`, `tag_name`, `owner`, `site_id`, `brand`, `region`.

## Gaps and next data pulls

For a prioritised, business-facing list of gaps (real Adobe feed at production volume, tag-manager change history, deployment event stream, campaign calendar, SEO/GEO ranks, data-classification review, provenance of the sample extract), see [data_inventory.md — Gaps & recommended next data pulls](data_inventory.md#gaps--recommended-next-data-pulls). The profiler cannot infer those from the current on-disk sample; they require external inputs.

## Appendix — methodology

- **Sensitive-column detection.** Union of a name-based regex (`ip`, `cookies`, `visid_*`, `geo_zip`, `post_tnt`, etc.) and a value sniffer (email / IPv4 / IPv6). Any column with >50% sniffer hits is treated as sensitive.
- **Timestamp detection.** Column-name regex (`date|time|timestamp|datetime|_dt$|_ts$|hit_time|…`) OR successful parse of ≥90% of the head-200 non-null values as datetimes.
- **Metric detection.** Numeric column whose name matches the metric regex (`count|amount|value|total|sum|revenue|visits|hits|views|clicks|orders|events|spend|conversions|impressions|sessions`). Hit-level Adobe columns are int-typed lookup IDs and will *not* match — by design.
- **Metric direction hint.** Name-regex table: `revenue|conversion|purchase|…` → `higher_is_good`; `error|latency|bounce|…` → `higher_is_bad`; `spend|cost|budget|…` → `context_dependent`; else `unknown`. Deliberately Adobe-flavoured; extend when SEO/GEO or contact-center feeds arrive.
- **Metric stability hint.** Coefficient of variation (`|std / mean|`); `stable` when CV < 0.5, `noisy` when ≥ 0.5, `insufficient_data` when non-null count < 30 or mean rounds to zero.
- **Outlier flag.** Union of IQR-1.5 (`x < Q1 - 1.5·IQR` or `x > Q3 + 1.5·IQR`) and MAD-6 (`|x - median| > 6·MAD`); requires ≥5 non-null values.
- **Joinability.** Two edge families: (1) *value-to-header* — values of a small dictionary-key column in file A appear as column names of file B (`dictionary_lookup`, `slot_expansion`); (2) *exact-name* — the same column name is flagged as a join key in both files. Neither uses raw row-level content.
- **Confidence.** `High` = parse ok + ≥10k rows + timestamps; `Medium` = parse ok + (≥1k rows OR timestamps); `Low` otherwise.
- **Recommended role.** Rule table over the four flag families (timestamps, metrics, dimensions, row-volume). See `_ad_readiness_block` in [src/gmai_pulse/profiling/data_profiler.py](../../src/gmai_pulse/profiling/data_profiler.py).
