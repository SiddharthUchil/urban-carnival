# Local synthetic profile — full-raw comprehensive EDA (demonstration)

> Generated locally with pandas over `data/synth/clean.parquet` (the synthetic
> replica) to demonstrate the ADR-0007 §5 **full-raw** approach the revised
> Databricks notebook now uses: every column profiled, **raw values, no masking
> and no shape-only carve-out** — identifiers included. Values are synthetic, so
> nothing here is real PII. The real all-197-column profile comes from running
> `eda/gwam_canada_retirement_eda.py` on Databricks against the production table.


**Source:** `data/synth/clean.parquet` — 1,142,361 rows × 120 columns (120 populated).


## Former shape-only identifiers — now RAW

These columns were emitted shape-only (cardinality/null only, no values) under the old `is_sensitive()` gate. Under full-raw they profile with real (synthetic) values:


| column | non-blank % | cardinality | top raw values |
|---|---|---|---|
| `user_agent` | 100.0 | 1,276 | `synthetic-ua/0194 (shape-only)` (0.09%), `synthetic-ua/0658 (shape-only)` (0.09%), `synthetic-ua/0429 (shape-only)` (0.09%) |
| `user_hash` | 100.0 | 1 | `synhash001` (100.0%) |
| `userid` | 100.0 | 1 | `syn000001` (100.0%) |
| `username` | 100.0 | 1 | `syn_user_00000001` (100.0%) |
| `visid_high` | 100.0 | 1 | `0` (100.0%) |
| `visid_low` | 100.0 | 1 | `0` (100.0%) |
| `post_visid_high` | 100.0 | 51,076 | `6070448471704377923` (0.0%), `2575870100782204383` (0.0%), `1697282755762164728` (0.0%) |
| `post_visid_low` | 100.0 | 51,076 | `4716573603881010470` (0.0%), `9306950187724859729` (0.0%), `6981574150711297676` (0.0%) |
| `post_zip` | 100.0 | 99,997 | `syn-zip-97757` (0.0%), `syn-zip-92679` (0.0%), `syn-zip-90045` (0.0%) |
| `cookies` | 99.667 | 2 | `1` (100.0%), `` (0.33%) |
| `mcvisid` | 100.0 | 51,076 | `07881008158964910732793287634141365402` (0.0%), `16155264325108768884902353090551228876` (0.0%), `00810641337143696854085156564220239904` (0.0%) |

## All populated columns (comprehensive census)

| column | non-blank % | cardinality | top raw value |
|---|---|---|---|
| `As_of_date` | 100.0 | 156 | `2026-02-18` (1.74%) |
| `Source_filename` | 100.0 | 151 | `Source_filename_v0` (17.81%) |
| `browser` | 99.333 | 290 | `<redacted:longnum>` (10.89%) |
| `browser_height` | 99.665 | 1,182 | `browser_height_v0` (13.04%) |
| `browser_width` | 99.664 | 1,666 | `browser_width_v0` (12.51%) |
| `business_unit` | 100.0 | 1 | `business_unit_v0` (100.0%) |
| `click_action_type` | 99.679 | 2 | `click_action_type_v0` (100.0%) |
| `click_context_type` | 99.662 | 2 | `click_context_type_v0` (100.0%) |
| `click_sourceid` | 99.665 | 2 | `click_sourceid_v0` (100.0%) |
| `code_ver` | 99.67 | 3 | `code_ver_v0` (66.73%) |
| `color` | 99.678 | 2 | `color_v0` (100.0%) |
| `connection_type` | 99.326 | 3 | `2` (91.07%) |
| `cookies` 🔓 | 99.667 | 2 | `1` (100.0%) |
| `country` | 99.672 | 80 | `country_v0` (20.21%) |
| `curr_factor` | 99.671 | 2 | `curr_factor_v0` (100.0%) |
| `curr_rate` | 99.677 | 2 | `curr_rate_v0` (100.0%) |
| `cust_hit_time_gmt` | 99.678 | 1,032,481 | `` (0.32%) |
| `customer_perspective` | 100.0 | 1 | `customer_perspective_v0` (100.0%) |
| `daily_visitor` | 99.664 | 3 | `daily_visitor_v0` (66.6%) |
| `date_time` | 99.663 | 1,032,272 | `` (0.34%) |
| `duplicate_purchase` | 99.33 | 2 | `0` (100.0%) |
| `evar105` | 99.666 | 6 | `<masked:bb540fd7>` (90.93%) |
| `evar106` | 99.666 | 5 | `<masked:77aa2f13>` (90.94%) |
| `evar107` | 99.654 | 715 | `<masked:3e2780a9>` (59.72%) |
| `evar108` | 99.672 | 1,344 | `<masked:38c16292>` (6.75%) |
| `evar109` | 99.66 | 2 | `<masked:094b0fe0>` (100.0%) |
| `evar137` | 99.667 | 2 | `<masked:131c006f>` (100.0%) |
| `evar138` | 99.668 | 2 | `<masked:4ed04285>` (100.0%) |
| `evar145` | 99.68 | 3 | `<masked:c0ac4842>` (51.89%) |
| `evar200` | 99.668 | 49,016 | `<syn:evar200:00000:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>` (8.68%) |
| `event_list` | 99.665 | 451 | `10004,10005,10006,10007,10008,10036,10037,10044,10099,10043,10000,10001,10002,10...<trunc>` (30.99%) |
| `exclude_hit` | 99.675 | 2 | `0` (100.0%) |
| `first_hit_ref_type` | 99.669 | 7 | `first_hit_ref_type_v0` (40.87%) |
| `first_hit_time_gmt` | 99.675 | 1,032,407 | `` (0.33%) |
| `geo_dma` | 99.666 | 452 | `geo_dma_v0` (15.03%) |
| `hit_source` | 99.669 | 2 | `1` (100.0%) |
| `hit_time_gmt` | 99.671 | 1,032,442 | `` (0.33%) |
| `hitid_high` | 99.665 | 1,138,530 | `` (0.34%) |
| `hitid_low` | 99.665 | 1,138,533 | `` (0.34%) |
| `homepage` | 99.67 | 2 | `homepage_v0` (100.0%) |
| `hourly_visitor` | 99.673 | 3 | `hourly_visitor_v0` (66.7%) |
| `j_jscript` | 99.662 | 2 | `j_jscript_v0` (100.0%) |
| `java_enabled` | 99.666 | 2 | `java_enabled_v0` (100.0%) |
| `javascript` | 99.674 | 2 | `javascript_v0` (100.0%) |
| `language` | 99.332 | 66 | `45` (63.18%) |
| `last_hit_time_gmt` | 99.671 | 1,032,419 | `` (0.33%) |
| `last_purchase_num` | 99.653 | 2 | `last_purchase_num_v0` (100.0%) |
| `last_purchase_time_gmt` | 99.66 | 2 | `last_purchase_time_gmt_v0` (100.0%) |
| `mcvisid` 🔓 | 100.0 | 51,076 | `07881008158964910732793287634141365402` (0.0%) |
| `post_customer_perspective` | 100.0 | 1 | `post_customer_perspective_v0` (100.0%) |
| `post_evar131` | 100.0 | 45,594 | `<syn:post_evar131:00000:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>` (8.78%) |
| `post_evar137` | 100.0 | 1 | `<masked:131c006f>` (100.0%) |
| `post_evar138` | 100.0 | 1 | `<masked:4ed04285>` (100.0%) |
| `post_evar144` | 100.0 | 1,774 | `<masked:9d56b8db>` (55.51%) |
| `post_evar145` | 100.0 | 2 | `<masked:c0ac4842>` (51.85%) |
| `post_evar200` | 100.0 | 49,053 | `<syn:post_evar200:00000:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>` (8.69%) |
| `post_event_list` | 100.0 | 451 | `10004,10005,10006,10007,10008,10036,10037,10044,10099,10043,10000,10001,10002,10...<trunc>` (30.98%) |
| `post_java_enabled` | 100.0 | 1 | `post_java_enabled_v0` (100.0%) |
| `post_page_event` | 100.0 | 1 | `0` (100.0%) |
| `post_page_url` | 100.0 | 39 | `www.manulife.com | depth=6 | /ca` (65.18%) |
| `post_pagename` | 100.0 | 73 | `ca-ret:personal:overview` (57.49%) |
| `post_pagename_no_url` | 100.0 | 72 | `post_pagename_no_url_v0` (20.65%) |
| `post_persistent_cookie` | 100.0 | 1 | `1` (100.0%) |
| `post_product_list` | 100.0 | 1 | `post_product_list_v0` (100.0%) |
| `post_prop51` | 100.0 | 66 | `<masked:82855007>` (61.67%) |
| `post_prop52` | 100.0 | 664 | `<masked:f3b4acce>` (59.31%) |
| `post_prop54` | 100.0 | 1 | `<masked:094b0fe0>` (100.0%) |
| `post_prop56` | 100.0 | 64 | `<masked:594fd161>` (96.75%) |
| `post_prop57` | 100.0 | 2 | `<masked:7cb6efb9>` (51.8%) |
| `post_search_engine` | 100.0 | 15 | `post_search_engine_v0` (30.2%) |
| `post_t_time_info` | 100.0 | 5,000 | `post_t_time_info_v0` (11.03%) |
| `post_visid_high` 🔓 | 100.0 | 51,076 | `6070448471704377923` (0.0%) |
| `post_visid_low` 🔓 | 100.0 | 51,076 | `4716573603881010470` (0.0%) |
| `post_visid_type` | 100.0 | 1 | `0` (100.0%) |
| `post_zip` 🔓 | 100.0 | 99,997 | `syn-zip-97757` (0.0%) |
| `prev_page` | 100.0 | 228 | `prev_page_v0` (16.64%) |
| `process_date` | 100.0 | 156 | `2026-02-18` (1.74%) |
| `process_timestamp` | 100.0 | 156 | `2026-02-18 04:00:00` (1.74%) |
| `prop51` | 100.0 | 66 | `<masked:82855007>` (61.68%) |
| `prop52` | 100.0 | 664 | `<masked:f3b4acce>` (59.27%) |
| `prop54` | 100.0 | 1 | `<masked:094b0fe0>` (100.0%) |
| `prop56` | 100.0 | 64 | `<masked:594fd161>` (96.71%) |
| `prop57` | 100.0 | 2 | `<masked:7cb6efb9>` (51.75%) |
| `quarterly_visitor` | 100.0 | 2 | `quarterly_visitor_v0` (66.66%) |
| `ref_type` | 100.0 | 6 | `6` (60.84%) |
| `resolution` | 100.0 | 164 | `resolution_v0` (17.6%) |
| `rsid` | 100.0 | 1 | `manulifeglobalprod` (100.0%) |
| `s_resolution` | 100.0 | 1,133 | `s_resolution_v0` (13.12%) |
| `sampled_hit` | 100.0 | 1 | `sampled_hit_v0` (100.0%) |
| `search_engine` | 100.0 | 17 | `search_engine_v0` (29.03%) |
| `search_page_num` | 100.0 | 2 | `search_page_num_v0` (66.74%) |
| `secondary_hit` | 100.0 | 1 | `secondary_hit_v0` (100.0%) |
| `service` | 100.0 | 1 | `service_v0` (100.0%) |
| `site_name` | 100.0 | 1 | `site_name_v0` (100.0%) |
| `site_url` | 100.0 | 1 | `site_url_v0` (100.0%) |
| `sourcefile_name` | 100.0 | 151 | `sourcefile_name_v0` (17.86%) |
| `sourceid` | 100.0 | 1 | `sourceid_v0` (100.0%) |
| `stats_server` | 100.0 | 5,000 | `stats_server_v0` (10.98%) |
| `t_time_info` | 100.0 | 5,000 | `t_time_info_v0` (11.0%) |
| `truncated_hit` | 100.0 | 1 | `truncated_hit_v0` (100.0%) |
| `user_agent` 🔓 | 100.0 | 1,276 | `synthetic-ua/0194 (shape-only)` (0.09%) |
| `user_hash` 🔓 | 100.0 | 1 | `synhash001` (100.0%) |
| `userid` 🔓 | 100.0 | 1 | `syn000001` (100.0%) |
| `username` 🔓 | 100.0 | 1 | `syn_user_00000001` (100.0%) |
| `va_closer_id` | 100.0 | 15 | `<masked:f1abd670>` (21.98%) |
| `va_finder_id` | 100.0 | 15 | `va_finder_id_v0` (30.06%) |
| `va_instance_event` | 100.0 | 2 | `va_instance_event_v0` (66.66%) |
| `va_new_engagement` | 100.0 | 2 | `va_new_engagement_v0` (66.58%) |
| `visid_high` 🔓 | 100.0 | 1 | `0` (100.0%) |
| `visid_low` 🔓 | 100.0 | 1 | `0` (100.0%) |
| `visid_new` | 100.0 | 1 | `1` (100.0%) |
| `visid_timestamp` | 100.0 | 780,929 | `1771403594` (0.0%) |
| `visid_type` | 100.0 | 1 | `0` (100.0%) |
| `visit_num` | 100.0 | 34 | `2` (6.54%) |
| `visit_page_num` | 100.0 | 10 | `1` (73.5%) |
| `visit_ref_type` | 100.0 | 6 | `visit_ref_type_v0` (40.73%) |
| `visit_search_engine` | 100.0 | 16 | `visit_search_engine_v0` (29.57%) |
| `visit_start_time_gmt` | 100.0 | 780,929 | `1771403594` (0.0%) |
| `weekly_visitor` | 100.0 | 2 | `weekly_visitor_v0` (66.74%) |
| `yearly_visitor` | 100.0 | 2 | `yearly_visitor_v0` (66.69%) |

_🔓 = column the old identifier gate suppressed; now raw. 11 such columns in this synthetic slice._
