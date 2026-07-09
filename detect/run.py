"""Phase 1 detection orchestrator: hit-level parquet -> unified anomaly table.

Runs the full local prototype in one shot: builds the daily KPI frame, then applies
all three detector classes (operational rules, business univariate forecast-residual,
business multivariate pyod) and concatenates their findings into one row-per-metric-day
table in the doc-8 schema. Also emits run_meta_{label}.json recording the eligible scored
metric-day denominators per detector class, so detect/evaluate.py can compute a false-
positive rate without re-running detection.

    python detect/run.py --input data/synth/injected.parquet --label injected --seed 7
    python detect/run.py --input data/synth/clean.parquet --label clean
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _bootstrap  # noqa: F401,E402  OpenMP guard: must precede numpy/pyarrow/darts/pyod

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from registry import SERIES, THRESHOLDS, RULES, EVENT_IDS  # noqa: E402
from kpis import build_kpis  # noqa: E402
from univariate import detect_univariate, detect_level_shifts, VOLUME_SOURCES  # noqa: E402
from multivariate import detect_multivariate  # noqa: E402
from rules import run_rules  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = REPO_ROOT / "data" / "synth" / "injected.parquet"
OUT_DIR = REPO_ROOT / "data" / "detect"

# Canonical output column order (doc section 8, plus date/detector for evaluation).
COLS = [
    "anomaly_id", "detected_at", "date", "plane", "domain", "metric_id", "segment",
    "observed", "expected", "score", "threshold", "severity", "class", "model_uri",
    "detector", "reconciled", "status",
]


def _min_active_frac(rules=RULES) -> float:
    for r in rules:
        if r["kind"] == "event_coverage":
            return r["min_active_frac"]
    return 0.90


def _denominators(kpis: pd.DataFrame, series=SERIES, thresholds=THRESHOLDS, rules=RULES):
    """Eligible scored metric-days per detector class -> FP-rate denominators.

    Univariate: same eligibility gate as detect_univariate (min_history, non-constant),
    each eligible series scored on the post-warmup region (start_frac of the calendar).
    Multivariate: every day scored. Operational: per-rule cell counts.
    """
    n_days = len(kpis)
    warmup = int(thresholds["start_frac"] * n_days)
    scored_per_series = n_days - warmup

    eligible = []
    for spec in series:
        v = kpis[spec.metric_id].to_numpy()
        if len(v) >= thresholds["min_history"] and not np.all(v == v[0]):
            eligible.append(spec.metric_id)

    maf = _min_active_frac(rules)
    n_always_on = sum(
        1 for eid in EVENT_IDS
        if float((kpis[f"event_{eid}_count"].to_numpy() > 0).mean()) >= maf
    )
    dim_rule = next((r for r in rules if r["kind"] == "dim_coverage"), None)
    n_dims = len(dim_rule["dims"]) if dim_rule else 0
    n_volume = sum(1 for s in series if s.source in VOLUME_SOURCES)

    return {
        "n_days": n_days,
        "warmup_days": warmup,
        "scored_days_per_series": scored_per_series,
        "eligible_univariate_series": eligible,
        "denominators": {
            "business": {
                "univariate": len(eligible) * scored_per_series,
                "level_shift": n_volume * scored_per_series,
                "multivariate": n_days,
            },
            "operational": {
                "zero_volume": n_days,
                "event_coverage": n_always_on * n_days,
                "dim_coverage": n_dims * max(0, n_days - 14),
            },
        },
    }


def run_detection(input_path: Path, method: str = "ecod", seed: int = 7):
    """Build KPIs and run all detectors. Returns (kpis, anomalies_df, meta)."""
    kpis = build_kpis(input_path)

    uni = detect_univariate(kpis)
    shifts = detect_level_shifts(kpis)
    mv = detect_multivariate(kpis, method=method, seed=seed)
    rul = run_rules(input_path, kpis)

    stamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    frames = []
    for part in (uni, shifts, mv, rul):
        if len(part):
            part = part.copy()
            part["detected_at"] = stamp
            frames.append(part.reindex(columns=COLS))
    anomalies = (pd.concat(frames, ignore_index=True) if frames
                 else pd.DataFrame(columns=COLS))
    anomalies = anomalies.sort_values(["date", "metric_id", "detector"]).reset_index(drop=True)

    meta = _denominators(kpis)
    meta["method"] = method
    meta["seed"] = seed
    meta["series"] = [s.metric_id for s in SERIES]
    meta["counts_by_detector"] = (
        anomalies["detector"].value_counts().to_dict() if len(anomalies) else {}
    )
    return kpis, anomalies, meta


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--label", default=None, help="output suffix (default: input file stem)")
    ap.add_argument("--method", default="ecod", choices=["ecod", "copod", "iforest"])
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = ap.parse_args(argv)

    label = args.label or args.input.stem
    kpis, anomalies, meta = run_detection(args.input, method=args.method, seed=args.seed)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    kpis.to_parquet(args.out_dir / f"kpis_{label}.parquet", index=False)
    anomalies.to_parquet(args.out_dir / f"anomalies_{label}.parquet", index=False)
    anomalies.to_csv(args.out_dir / f"anomalies_{label}.csv", index=False)

    meta["label"] = label
    meta["input"] = str(args.input)
    meta["date_min"] = str(kpis["process_date"].min().date())
    meta["date_max"] = str(kpis["process_date"].max().date())
    with open(args.out_dir / f"run_meta_{label}.json", "w") as fh:
        json.dump(meta, fh, indent=2)

    print(f"=== detection: {args.input.name} -> anomalies_{label} ===")
    print(f"{len(kpis)} days x {len(kpis.columns) - 1} series | "
          f"{len(anomalies)} flagged metric-days")
    for det, n in sorted(meta["counts_by_detector"].items()):
        days = anomalies[anomalies["detector"] == det]["date"].nunique()
        print(f"  {det:<13} {int(n):>4} rows / {days} days")
    print(f"denominators: {meta['denominators']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
