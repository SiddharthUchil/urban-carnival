"""Load and normalize the extracted EDA spec into a generator-friendly ``Spec`` object.

The raw JSON in ``synth/spec/`` is the verbatim EDA output. This module turns it into
clamped, ready-to-sample structures:

* the exact in-window daily hit curve (from ts_daily, trailing partial day dropped),
* a per-day-of-week hour-of-day probability matrix (from volume.hour_matrix),
* per-column generation specs (pop_pct clamped to [0,100], masked-token pools),
* per-dimension categorical samplers (top values + a synthetic long tail + blank mass),
* the event inclusion model (per-id Bernoulli probability), and
* identity cardinality/length shapes + visit ratios.

Sampling artifacts in the EDA (pop_pct > 100, negative null_blank_pct) are clamped here so
nothing downstream has to know about them.
"""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SPEC_DIR = REPO_ROOT / "synth" / "spec"


def clamp_pct(x: float) -> float:
    """EDA percentages can exceed 100 or go slightly negative (approx-count sampling noise)."""
    return max(0.0, min(100.0, float(x)))


@dataclass
class ColumnSpec:
    col: str
    dtype: str                       # 'string' | 'timestamp' | 'date'
    pop: float                       # populated fraction in [0,1]
    apx_distinct: int
    sensitive: bool
    len_p50: int | None = None
    len_max: int | None = None
    top_values: list = field(default_factory=list)   # [{v,len,pct}, ...] verbatim


@dataclass
class DimSpec:
    dim: str
    mode: str
    coverage: float                  # fraction of rows that are non-blank, in [0,1]
    apx_distinct: int
    values: list[str]                # top value labels (verbatim, may be masked)
    # Categorical over [BLANK, *values, *tail_labels]; probs sum to 1.
    choices: list[str] = field(default_factory=list)
    probs: list[float] = field(default_factory=list)


@dataclass
class EventSpec:
    ids: list[str]
    incl_prob: dict[str, float]      # per-id Bernoulli inclusion probability
    always_on: list[str]             # ids with prob == 1.0
    p50: int
    p95: int
    max: int


@dataclass
class Spec:
    meta: dict
    days: list[tuple[dt.date, int, int, int]]  # (date, hits, visits, visitors), in-window only
    hour_probs: list[list[float]]    # 7 (Mon..Sun) x 24, each row sums to 1
    dow_index: list[float]
    columns: list[ColumnSpec]
    dims: dict[str, DimSpec]
    events: EventSpec
    identity_cols: dict[str, dict]   # name -> {distinct, len_min, len_avg, len_max}
    hits_per_visit: float
    visits_per_visitor: float
    clock_skew: list[int]            # [p5, p50, p95] seconds
    level_shift_dates: list[str]

    @property
    def total_rows(self) -> int:
        return sum(d[1] for d in self.days)

    @property
    def col_names(self) -> list[str]:
        return [c.col for c in self.columns]


def _parse_days(ts_daily: dict, date_max: str) -> list[tuple[dt.date, int, int, int]]:
    """ts_daily csv rows are 'date,hits,visits,visitors,clean_hits'; keep date<=date_max."""
    dmax = dt.date.fromisoformat(date_max)
    out: list[tuple[dt.date, int, int, int]] = []
    for row in ts_daily["csv"]:
        parts = row.split(",")
        d = dt.date.fromisoformat(parts[0])
        if d <= dmax:
            # (date, hits, visits, visitors); visits/visitors are approx_count_distinct.
            out.append((d, int(parts[1]), int(parts[2]), int(parts[3])))
    return out


def _normalize_hour_matrix(hm: list[list[float]]) -> list[list[float]]:
    probs = []
    for row in hm:
        total = sum(row)
        probs.append([v / total for v in row] if total > 0 else [1 / 24] * 24)
    return probs


def _dedup_labels(values: list[str]) -> list[str]:
    """Some dims (browser, os) redact several distinct real values to the same
    '<redacted:longnum>' placeholder. Keep them as distinct categories so cardinality
    and per-value share stay faithful instead of collapsing into one."""
    seen: dict[str, int] = {}
    out = []
    for v in values:
        if v in seen:
            seen[v] += 1
            out.append(f"{v}#{seen[v]}")
        else:
            seen[v] = 0
            out.append(v)
    return out


def _build_dim(d: dict) -> DimSpec:
    coverage = clamp_pct(d["coverage_pct"]) / 100.0
    tops = d.get("top", [])
    values = _dedup_labels([t["v"] for t in tops])
    top_masses = [clamp_pct(t["pct"]) / 100.0 for t in tops]
    top_mass = sum(top_masses)

    # Reconcile top mass against coverage (sampling noise can push it over).
    if top_mass > coverage:
        scale = coverage / top_mass if top_mass > 0 else 0.0
        top_masses = [m * scale for m in top_masses]
        top_mass = coverage
    tail_mass = coverage - top_mass

    n_tail = max(0, int(d["apx_distinct"]) - len(values))
    # When the published top values already sum to ~coverage there is no leftover mass,
    # but the column genuinely has apx_distinct values. Reserve a small floor so the
    # measured long tail is represented; tops shrink negligibly (< top-share tolerance).
    if n_tail > 0:
        tail_floor = 0.004
        if tail_mass < tail_floor and top_mass > tail_floor:
            take = tail_floor - tail_mass
            scale = (top_mass - take) / top_mass
            top_masses = [m * scale for m in top_masses]
            tail_mass = tail_floor

    tail_labels, tail_masses = [], []
    if n_tail > 0 and tail_mass > 1e-12:
        # Mild Zipf so the synthetic tail is not perfectly flat.
        weights = [1.0 / (i + 1) for i in range(n_tail)]
        wsum = sum(weights)
        tail_labels = [f"{d['dim']}~tail{i:04d}" for i in range(n_tail)]
        tail_masses = [tail_mass * w / wsum for w in weights]
    elif tail_mass > 1e-12:
        # No room for distinct tail labels; fold leftover mass into the top values.
        extra = tail_mass / len(top_masses) if top_masses else 0.0
        top_masses = [m + extra for m in top_masses]

    blank_mass = max(0.0, 1.0 - coverage)
    choices = ["", *values, *tail_labels]
    probs = [blank_mass, *top_masses, *tail_masses]
    # Renormalize defensively so probs sum to exactly 1.
    s = sum(probs)
    probs = [p / s for p in probs]
    return DimSpec(
        dim=d["dim"],
        mode=d["mode"],
        coverage=coverage,
        apx_distinct=int(d["apx_distinct"]),
        values=values,
        choices=choices,
        probs=probs,
    )


def _build_events(ev: dict) -> EventSpec:
    ids, incl, always = [], {}, []
    for f in ev["event_freq"]:
        eid = str(f["event_id"])
        p = clamp_pct(f["hits_with_pct"]) / 100.0
        ids.append(eid)
        incl[eid] = p
        if p >= 1.0:
            always.append(eid)
    p50, p95 = ev["events_per_hit_p50_p95"]
    return EventSpec(ids=ids, incl_prob=incl, always_on=always,
                     p50=int(p50), p95=int(p95), max=int(ev["events_per_hit_max"]))


def load_spec(spec_dir: Path = SPEC_DIR) -> Spec:
    spec = json.loads((spec_dir / "synthesis_spec.json").read_text(encoding="utf-8"))
    ts_daily = json.loads((spec_dir / "ts_daily.json").read_text(encoding="utf-8"))

    vol = spec["volume"]
    days = _parse_days(ts_daily, vol["date_max"])

    columns = []
    for e in spec["schema"]:
        ln = e.get("len") or {}
        columns.append(ColumnSpec(
            col=e["col"],
            dtype=e["dtype"],
            pop=clamp_pct(e["pop_pct"]) / 100.0,
            apx_distinct=int(e["apx_distinct"]),
            sensitive=bool(e["sensitive_shape_only"]),
            len_p50=ln.get("p50"),
            len_max=ln.get("max"),
            # ADR-0007 §5 renamed this to top_values/`v` when EDA stopped masking
            # business dimensions. Older spec JSON still carries top_masked/`m`,
            # so accept both and normalize to `v`.
            top_values=[{"v": t.get("v", t.get("m")), "len": t.get("len"), "pct": t.get("pct")}
                        for t in (e.get("top_values") or e.get("top_masked") or [])],
        ))

    dims = {d["dim"]: _build_dim(d) for d in spec["dims"]}

    identity_cols = {}
    for c in spec["identity"]["columns"]:
        ln = c.get("len") or {}
        identity_cols[c["col"]] = {
            "distinct": int(c["apx_distinct"]),
            "len_min": ln.get("min"),
            "len_avg": ln.get("avg"),
            "len_max": ln.get("max"),
            "null_blank": clamp_pct(c["null_blank_pct"]) / 100.0,
        }
    ratios = spec["identity"]["daily_ratios"]

    return Spec(
        meta=spec["meta"],
        days=days,
        hour_probs=_normalize_hour_matrix(vol["hour_matrix"]),
        dow_index=vol["dow_index"],
        columns=columns,
        dims=dims,
        events=_build_events(spec["events"]),
        identity_cols=identity_cols,
        hits_per_visit=float(ratios["mean_hits_per_visit"]),
        visits_per_visitor=float(ratios["mean_visits_per_visitor_daily"]),
        clock_skew=[int(x) for x in spec["dq"]["clock_skew"]["p5_p50_p95_seconds"]],
        level_shift_dates=[ls["date"] for ls in vol["level_shifts"]],
    )


if __name__ == "__main__":
    s = load_spec()
    print(f"days={len(s.days)}  total_rows={s.total_rows:,}  "
          f"range={s.days[0][0]}..{s.days[-1][0]}")
    print(f"columns={len(s.columns)}  dims={len(s.dims)}  events={len(s.events.ids)} "
          f"(always_on={len(s.events.always_on)})")
    print(f"identity_cols={len(s.identity_cols)}  hits/visit={s.hits_per_visit} "
          f"visits/visitor={s.visits_per_visitor}  clock_skew={s.clock_skew}")
