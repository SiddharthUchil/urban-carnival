"""Local metric registry for the Phase 1 anomaly detector (config-not-code).

This is the single place that declares which KPI series get built from the hit-level
parquet, which operational rules run, and the scoring thresholds. It is the local
stand-in for research/claude/metric-registry.yaml: production reads a governed YAML
registry; the prototype keeps it as a Python module so the detector core stays
dependency-light and the same SeriesSpec objects drive both KPI construction
(detect/kpis.py) and detection (detect/univariate.py, detect/multivariate.py).

    python detect/registry.py        # print the registered series and rules
"""
from __future__ import annotations

from dataclasses import dataclass

# The 23 event ids that appear in post_event_list (confirmed against the generated data).
EVENT_IDS = [
    "20", "500", "501", "502", "503", "504",
    "10000", "10001", "10002", "10003", "10004", "10005", "10006", "10007", "10008",
    "10020", "10030", "10036", "10037", "10039", "10043", "10044", "10099",
]

# Resolved names for the tracked event ids. Sourced from new_data/event.tsv, which the S6
# EDA run reproduces algorithmically from Adobe's default numbering (10000-10149 = Instance
# of eVar101-250; 500-504 = ClickMap/Target; 20 = Campaign View). Used only to make emitted
# anomalies human-readable; carries no detection behaviour. Note 10036 (Instance of eVar137)
# is an always-on tagging flag present on ~100% of hits -> its coverage going to zero is the
# event_drop scenario, caught deterministically by the event_coverage rule.
EVENT_NAMES = {
    "20": "Campaign View",
    "500": "Instance of clickmappage",
    "501": "Instance of clickmaplink",
    "502": "Instance of clickmapregion",
    "503": "Instance of clickmaplinkbyregion",
    "504": "Instance of targetsessionid",
    "10000": "Instance of eVar101", "10001": "Instance of eVar102",
    "10002": "Instance of eVar103", "10003": "Instance of eVar104",
    "10004": "Instance of eVar105", "10005": "Instance of eVar106",
    "10006": "Instance of eVar107", "10007": "Instance of eVar108",
    "10008": "Instance of eVar109", "10020": "Instance of eVar121",
    "10030": "Instance of eVar131", "10036": "Instance of eVar137",
    "10037": "Instance of eVar138", "10039": "Instance of eVar140",
    "10043": "Instance of eVar144", "10044": "Instance of eVar145",
    "10099": "Instance of eVar200",
}

# Top dimension values whose daily share is tracked as a univariate series. The first
# post_pagename value is the dim_mix injection target; keeping the top handful lets the
# share detector see the mix shift without exploding the feature space.
TOP_PAGENAMES = [
    "ca-ret:personal:overview",
    "ca-ret:personal:account:enrol-now",
    "manulife:ca:en:personal:group-retirement:overview",
    "ca-ret:personal:overview:hidden:webinars",
    "ca-ret:personal:prepare:plans:find-an-advisor",
]
TOP_LANGUAGES = ["45", "39", "38"]

# Per-visit page-view buckets. Labels and edges match probe C12's c12_visit_shape()
# (eda/gwam_channel_discovery.py) so the emitted shares stay comparable to the published
# baselines: 0pv 3.3% / 1pv 78.8% / 2pv 11.7% / 3-5pv 5.2% / 6+ 1.1%.
#
# Two of these back governed metrics (research/claude/metric-registry.yaml): bucket "0" is
# gwam_pw_pv_per_visit's real signal -- the SME's "page views per visit < 1" test cannot fire
# (C12: 88-day floor 1.2236), because visits with zero page views are diluted by the 78.8%
# with exactly one; counting them directly is the detectable form. Bucket "2" is
# gwam_pw_pv_per_visit_dup2, the duplication indicator. The other three are emitted because
# the five shares must sum to 1.0, which is a cheap and strong parity invariant.
PV_BUCKETS = ["0", "1", "2", "3-5", "6+"]
# Explicit, not slug(): slug("6+") collapses to "6", which reads as the 6-page bucket.
PV_BUCKET_SUFFIX = {"0": "0", "1": "1", "2": "2", "3-5": "3_5", "6+": "6plus"}


def slug(value: str) -> str:
    """Stable column-safe slug of a dimension value (matches evaluate.py's mapping)."""
    s = "".join(ch if ch.isalnum() else "-" for ch in value.lower())
    while "--" in s:
        s = s.replace("--", "-")
    return s.strip("-")


@dataclass(frozen=True)
class SeriesSpec:
    """One daily KPI time series. kind drives both construction and transform.

    kind=ratio references two sibling metric_ids (numerator/denominator), mirroring
    CmSeriesSpec. Both builders resolve it in a second pass -- kpis.build_kpis and
    gold_lib.build_kpis_spark -- and yield 0.0 where the denominator is 0.

    kind=point_mass (source=visit_pv_bucket) is the share of VISITS whose page-view count
    falls in the bucket named by dim_value. It is not kind=share: share divides a hit count
    by hits_total on a dimension value, whereas this aggregates to visit grain first. Kept a
    separate kind so the share path stays untouched.
    """
    metric_id: str
    kind: str            # count | rate | share | ratio | point_mass
    source: str          # hits | visits | visitors | event | pagename | language
                         #   | page_views | visit_pv_bucket
    event_id: str | None = None
    dim: str | None = None
    dim_value: str | None = None
    numerator: str | None = None
    denominator: str | None = None
    # Governance (metric-registry.yaml v0.6.0): status in {active, candidate, deferred},
    # direction in {higher_is_good, higher_is_bad, context_dependent}. Ported from
    # CmSeriesSpec 2026-07-30 (doc 19 G2); GWAM entries are all owner: TBD today.
    status: str = "candidate"
    direction: str = "context_dependent"
    owner: str = "TBD"

    @property
    def log_transform(self) -> bool:
        # Counts are modelled in log space (multiplicative anomalies -> additive residuals);
        # rates, shares and ratios are already on a bounded scale and used raw.
        return self.kind == "count"


def _build_series() -> list[SeriesSpec]:
    series: list[SeriesSpec] = [
        SeriesSpec("hits_total", "count", "hits"),
        SeriesSpec("visits_total", "count", "visits"),
        SeriesSpec("visitors_total", "count", "visitors"),
    ]
    for eid in EVENT_IDS:
        series.append(SeriesSpec(f"event_{eid}_count", "count", "event", event_id=eid))
    # event rate is the coverage signal the event_drop scenario moves; 10036 is always-on.
    for eid in ["10036"]:
        series.append(SeriesSpec(f"event_{eid}_rate", "rate", "event", event_id=eid))
    for value in TOP_PAGENAMES:
        series.append(SeriesSpec(f"pagename_share_{slug(value)}", "share", "pagename",
                                 dim="post_pagename", dim_value=value))
    for value in TOP_LANGUAGES:
        series.append(SeriesSpec(f"language_share_{slug(value)}", "share", "language",
                                 dim="language", dim_value=value))
    # Page-view basis series (doc 20 Q6). APPENDED, never interleaved: SERIES order is
    # gold_lib's column order, so keeping the 35 originals in place is what lets the
    # "all_hits is inert" check compare column-for-column against the pre-change baseline.
    series.append(SeriesSpec("page_views_total", "count", "page_views"))
    series.append(SeriesSpec("pv_per_visit", "ratio", "page_views",
                             numerator="page_views_total", denominator="visits_total"))
    for b in PV_BUCKETS:
        series.append(SeriesSpec(f"pv_bucket_share_{PV_BUCKET_SUFFIX[b]}",
                                 "point_mass", "visit_pv_bucket", dim_value=b))
    return series


SERIES: list[SeriesSpec] = _build_series()

# Series the detectors actually SCORE. The page-view family is built into every KPI frame --
# gold carries it, so answering doc 20 Q6 is a config flip rather than a schema change -- but
# it is deliberately not scored yet, for two independent reasons:
#
#   1. Governance. research/claude/metric-registry.yaml holds all three page-view entries at
#      status=candidate precisely because Q6 (which basis the SME means) is unanswered, and a
#      candidate metric must not raise alerts.
#   2. Arithmetic. While PAGE_VIEW_BASIS is "all_hits", page_views_total is IDENTICAL to
#      hits_total by construction. Scoring it would fire every hits anomaly a second time and
#      pad the false-positive denominator with a perfectly correlated duplicate -- the FP rate
#      would look better while the alert stream got worse.
#
# Promoting them is the Q6 follow-up: once the basis is settled, drop the source from
# PAGE_VIEW_SOURCES and re-run the calibration.
PAGE_VIEW_SOURCES = ("page_views", "visit_pv_bucket")
SCORED_SERIES: list[SeriesSpec] = [s for s in SERIES if s.source not in PAGE_VIEW_SOURCES]

# Dimensions checked by the coverage rule for blank-rate jumps and unseen high-share values.
RULE_DIMS = ["post_pagename", "language", "ref_type", "connection_type", "browser", "va_closer_id"]

RULES = [
    {"name": "zero_volume", "kind": "zero_volume"},
    {"name": "event_coverage", "kind": "event_coverage", "min_active_frac": 0.90},
    {"name": "dim_coverage", "kind": "dim_coverage", "dims": RULE_DIMS,
     "blank_jump_pp": 10.0, "new_value_share": 0.01, "trailing_days": 28},
]

# Adaptive-threshold config. Severity tiers map |robust-z| of the residual to the
# warn/minor/major/critical ladder from research/claude/03-phase1-anomaly-detection.md.
THRESHOLDS = {
    "z_flag": 3.5,          # robust-z (MAD) flag threshold
    "high_quantile": 0.99,  # empirical high-quantile gate on |residual|
    "quantile_z_floor": 3.0,  # quantile flags only count if |z| also clears this floor
    "start_frac": 0.20,     # warm-up fraction before the first scored day
    "min_history": 21,      # minimum scored points before a series is eligible
    "dow_window": 4,        # trailing same-weekday occurrences in the seasonal baseline median
    "dow_min_history": 2,   # min same-weekday priors before a day can be scored
    # Level-shift detector: adjacent short(post) vs long(pre) window mean-shift on log volume.
    # Both windows are weekday-balanced (multiples of 7) so weekly seasonality cancels in the
    # difference, and comparing to the *immediately preceding* window ignores slow seasonal
    # trend -- unlike a trailing baseline (which absorbs the shift) or a global reference
    # (which flags the normal seasonal decline).
    "ls_post_window": 7,    # short trailing window (the candidate shifted level)
    "ls_pre_window": 21,    # reference window immediately before it (the prior level)
    "ls_z": 4.0,            # standardized mean-shift threshold
    "severity": {"warn": 3.5, "minor": 5.0, "major": 8.0, "critical": 12.0},
}


def severity_for(z_abs: float) -> str:
    tiers = THRESHOLDS["severity"]
    if z_abs >= tiers["critical"]:
        return "critical"
    if z_abs >= tiers["major"]:
        return "major"
    if z_abs >= tiers["minor"]:
        return "minor"
    return "warn"


def main(argv=None):
    print(f"{len(SERIES)} registered series:")
    for s in SERIES:
        name = f"  ({EVENT_NAMES[s.event_id]})" if s.event_id else ""
        print(f"  {s.metric_id:<48} kind={s.kind:<6} source={s.source}{name}")
    print(f"\n{len(RULES)} operational rules: {[r['name'] for r in RULES]}")
    print(f"thresholds: {THRESHOLDS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
