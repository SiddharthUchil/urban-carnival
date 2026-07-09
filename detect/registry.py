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


def slug(value: str) -> str:
    """Stable column-safe slug of a dimension value (matches evaluate.py's mapping)."""
    s = "".join(ch if ch.isalnum() else "-" for ch in value.lower())
    while "--" in s:
        s = s.replace("--", "-")
    return s.strip("-")


@dataclass(frozen=True)
class SeriesSpec:
    """One daily KPI time series. kind drives both construction and transform."""
    metric_id: str
    kind: str            # count | rate | share
    source: str          # hits | visits | visitors | event | pagename | language
    event_id: str | None = None
    dim: str | None = None
    dim_value: str | None = None

    @property
    def log_transform(self) -> bool:
        # Counts are modelled in log space (multiplicative anomalies -> additive residuals);
        # rates and shares are already on [0, 1] and used raw.
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
    return series


SERIES: list[SeriesSpec] = _build_series()

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
