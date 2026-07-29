"""CoverMe metric registry for the GMAI-Pulse gold KPI build (config-not-code).

Python stand-in for research/claude/metric-registry.yaml **v0.4.0** (CoverMe entries
SME-confirmed at v0.3.0, 2026-07-27),
which remains the governed source of truth -- metric_ids for the 12 business-flagged
post_event_list entries are taken from it verbatim. Kept separate from detect/registry.py
because the two report suites expose different event-id spaces (the YAML's
report_suite_caveat: CoverMe ids do not transfer to GWAM's suite).

Metric semantics (Adobe datafeeds-calculate, link supplied by Kerrian 2026-07-27, plus the
SME rulings of the same date):
  - visits    = distinct(post_visid_high, post_visid_low, visit_num, visit_start_time_gmt)
                -- the 4-part key; Adobe recommends visit_start_time_gmt against
                visit_num reuse (EDA measured 0.29% duplicates on the adjacent 3-part key)
  - visitors  = distinct(post_visid_high, post_visid_low) -- all Adobe ID methods collapse
                into this pair; NOT mcvisid
  - event counts are VISIT-DISTINCT (visits containing the event), per the SME ruling.
    The funnel is non-monotonic: saved-quote resume means App Start/Confirm can fire in
    visits with zero quote events. Never compute within-visit sequences.
  - funnel ratios are population proxies (daily visit-count ratios), not per-visitor
    conversion; 0-safe (0.0 when the denominator is 0).
  - language is the silver-derived domain language (en/fr/unknown), not eVar8.

    python detect/cm_registry.py     # print the registered series

Not promoted (SME 2026-07-27): events 510-514 (pending naming -- the highest-frequency
events on the site); event 20100 (val_mean ~ -6.7e10, doc-17 E4) never joins a
value-based KPI.
"""
from __future__ import annotations

from dataclasses import dataclass

from registry import slug

# research/claude/metric-registry.yaml pin. This is the single pin for the whole registry,
# so a GWAM-only change bumps it too: v0.4.0 added the gwam_channel_metrics section and
# touched nothing CoverMe. The CoverMe entries below are unchanged from v0.3.0.
REGISTRY_VERSION = "0.4.0"

# --- Gold build parameters (consumed by cm_03_gold_kpis via gold_lib) ---
DATE_COL = "hit_date"
VISIT_KEY_COLS = ("post_visid_high", "post_visid_low", "visit_num", "visit_start_time_gmt")
VISITOR_KEY_COLS = ("post_visid_high", "post_visid_low")
EVENT_BASIS = "visits"       # visit-distinct event counts (SME ruling 2026-07-27)
NULL_SAFE_KEYS = True        # positional NULLs in composite keys must not collide, and a
                             # fully-NULL visitor pair is skipped, not counted as ""

# Columns the gold build reads from silver.
NEEDED_COLS = ["hit_date", "post_event_list", "language", "post_evar4",
               "post_visid_high", "post_visid_low", "visit_num", "visit_start_time_gmt"]

# The 12 business-flagged post_event_list ids (CoverMeDataMap.xlsx Notes col; registry
# v0.3.0): the 5 quote/app funnel events + 7 Instance-of-eVar counters.
FUNNEL_EVENT_IDS = ["228", "229", "232", "269", "240"]   # logical funnel order
EVENT_IDS = ["103", "104", "105", "110", "115", "151", "10047"] + FUNNEL_EVENT_IDS
# TODO(SME-pending): events 510-514 (~43.5% of hits each; doc 18 Q10) are unidentified
# and stay excluded from EVENT_IDS until Kerrian names them.

# metric_id + display name per event, verbatim from metric-registry.yaml v0.3.0.
EVENT_METRICS = {
    "103": ("pel_103_product_category_inst", "Instance of eVar4 (Product Category)"),
    "104": ("pel_104_product_id_inst", "Instance of eVar5 (Product ID)"),
    "105": ("pel_105_sponsor_distributor_inst",
            "Instance of eVar6 (Sponsor/Distributor/Association)"),
    "110": ("pel_110_quote_session_inst", "Instance of eVar11 (Quote Session ID)"),
    "115": ("pel_115_transaction_inst", "Instance of eVar16 (Transaction ID)"),
    "151": ("pel_151_current_page_inst", "Instance of eVar52 (Current Page)"),
    "10047": ("pel_10047_bot_detector_inst", "Instance of eVar148 (Bot Detector)"),
    "228": ("pel_228_quote_start", "Quote Start (Custom Event 29)"),
    "229": ("pel_229_quote_complete", "Quote Complete (Custom Event 30)"),
    "232": ("pel_232_save_quote", "Save Quote (Custom Event 33)"),
    "240": ("pel_240_app_confirm", "App Confirm (Custom Event 41)"),
    "269": ("pel_269_app_start", "App Start (Custom Event 70)"),
}
EVENT_NAMES = {eid: name for eid, (_, name) in EVENT_METRICS.items()}

# Governance per event, verbatim from metric-registry.yaml v0.3.0 (status, direction,
# owner). The 5 funnel events are `active` with Kerrian as owner (her stated priority);
# the 7 instance counters stay `candidate`. direction is detector polarity -- e.g. rising
# Bot Detector instances are a data-quality anomaly (higher_is_bad).
EVENT_GOVERNANCE = {
    "103": ("candidate", "context_dependent", "TBD"),
    "104": ("candidate", "context_dependent", "TBD"),
    "105": ("candidate", "context_dependent", "TBD"),
    "110": ("candidate", "higher_is_good", "TBD"),
    "115": ("candidate", "higher_is_good", "TBD"),
    "151": ("candidate", "context_dependent", "TBD"),
    "10047": ("candidate", "higher_is_bad", "TBD"),
    "228": ("active", "higher_is_good", "Kerrian (Business SME)"),
    "229": ("active", "higher_is_good", "Kerrian (Business SME)"),
    "232": ("active", "higher_is_good", "Kerrian (Business SME)"),
    "240": ("active", "higher_is_good", "Kerrian (Business SME)"),
    "269": ("active", "higher_is_good", "Kerrian (Business SME)"),
}

# SME-priority breakdown dim: funnel events x Product Category (post_evar4). Fixed top-5
# values from the verified EDA census (hd 44.9%, travel 37.1%, life 6.6%, home 4.9%,
# affinity-travel booking 2.5%); everything else -- including blank -- lands in the
# implicit "other" bucket so daily totals stay conserved.
TOP_PRODUCT_CATEGORIES = [
    "hd", "travel", "life", "home", "content:affinity-travel:en-ca:coverme:booking",
]

TOP_LANGUAGES = ["en", "fr", "unknown"]   # silver-derived domain language, ~50/50 EN/FR
# TODO(SME-pending): language field of record (eVar8 vs eVar149 vs prop5, doc 18 Q4);
# interim rule = domain-derived. Rebuild silver + these shares when Kerrian rules.


@dataclass(frozen=True)
class CmSeriesSpec:
    """One daily KPI time series. kind drives both construction and transform.

    kind=ratio references two sibling metric_ids (numerator/denominator). An event_dim
    spec with dim_value=None is the "other" bucket: dim values not claimed by any sibling
    spec sharing the same (event_id-independent) dim.
    """
    metric_id: str
    kind: str            # count | share | ratio
    source: str          # hits | visits | visitors | event | event_dim | language
    event_id: str | None = None
    dim: str | None = None
    dim_value: str | None = None
    numerator: str | None = None
    denominator: str | None = None
    # Governance (metric-registry.yaml v0.3.0): status in {active, candidate, deferred},
    # direction in {higher_is_good, higher_is_bad, context_dependent}. Event series copy
    # the yaml verbatim; derived series follow the conventions noted in _build_series.
    status: str = "candidate"
    direction: str = "context_dependent"
    owner: str = "TBD"

    @property
    def log_transform(self) -> bool:
        # Counts are modelled in log space (multiplicative anomalies -> additive
        # residuals); shares and ratios are already on a bounded scale and used raw.
        return self.kind == "count"


def _build_series() -> list[CmSeriesSpec]:
    # Totals have no metric-registry.yaml entry (the data_feed_columns sheet tracks
    # column population rates, not volume totals) -- governance defaults apply.
    series: list[CmSeriesSpec] = [
        CmSeriesSpec("hits_total", "count", "hits"),
        CmSeriesSpec("visits_total", "count", "visits"),
        CmSeriesSpec("visitors_total", "count", "visitors"),
    ]
    for eid in EVENT_IDS:
        st, dr, ow = EVENT_GOVERNANCE[eid]
        series.append(CmSeriesSpec(EVENT_METRICS[eid][0], "count", "event", event_id=eid,
                                   status=st, direction=dr, owner=ow))
    # Funnel ratios -- names mirror the EDA S6b conversion keys. Governance: ratios of
    # two active funnel events inherit active/Kerrian (proposed convention -- the yaml
    # registers only the raw events, not the derived ratios; flagged for SME review).
    for name, num, den in [
        ("funnel_quote_complete_over_quote_start", "229", "228"),
        ("funnel_save_quote_over_quote_start", "232", "228"),
        ("funnel_app_start_over_quote_start", "269", "228"),
        ("funnel_app_confirm_over_app_start", "240", "269"),
        ("funnel_app_confirm_over_quote_start", "240", "228"),
    ]:
        series.append(CmSeriesSpec(name, "ratio", "event",
                                   numerator=EVENT_METRICS[num][0],
                                   denominator=EVENT_METRICS[den][0],
                                   status="active", direction="higher_is_good",
                                   owner="Kerrian (Business SME)"))
    for value in TOP_LANGUAGES:
        series.append(CmSeriesSpec(f"language_share_{slug(value)}", "share", "language",
                                   dim="language", dim_value=value))
    # Funnel x Product Category cube (visit-distinct, like the flat event counts).
    # Governance inherited from the parent funnel event -- the cube operationalizes
    # Kerrian's "pair funnel events with eVar breakdowns" priority.
    for eid in FUNNEL_EVENT_IDS:
        base = EVENT_METRICS[eid][0]
        st, dr, ow = EVENT_GOVERNANCE[eid]
        for value in TOP_PRODUCT_CATEGORIES:
            series.append(CmSeriesSpec(f"{base}__evar4_{slug(value)}", "count", "event_dim",
                                       event_id=eid, dim="post_evar4", dim_value=value,
                                       status=st, direction=dr, owner=ow))
        series.append(CmSeriesSpec(f"{base}__evar4_other", "count", "event_dim",
                                   event_id=eid, dim="post_evar4", dim_value=None,
                                   status=st, direction=dr, owner=ow))
    return series


SERIES: list[CmSeriesSpec] = _build_series()


def main(argv=None):
    print(f"cm_registry v{REGISTRY_VERSION}: {len(SERIES)} registered series:")
    for s in SERIES:
        name = f"  ({EVENT_NAMES[s.event_id]})" if s.event_id else ""
        print(f"  {s.metric_id:<56} kind={s.kind:<6} source={s.source}{name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
