"""Multivariate business-anomaly detection: pyod outlier scoring on daily KPI vectors.

Each day is a feature vector (all registered series; counts in log space, rates/shares
raw). pyod ECOD (default) scores every day by how extreme its joint tail position is;
COPOD and IForest are selectable. ECOD/COPOD are ECDF-based and scale-free, so no
standardisation or contamination parameter is needed. A day is flagged when the robust-z
of its outlier score clears the same z threshold the univariate detector uses.

On n=156 days this signal is noisy and treated as corroborating, not authoritative.

    python detect/multivariate.py --kpis data/detect/kpis_injected.parquet --method ecod
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _bootstrap  # noqa: F401,E402  (must precede pyod import)
from pyod.models.ecod import ECOD  # noqa: E402
from pyod.models.copod import COPOD  # noqa: E402
from pyod.models.iforest import IForest  # noqa: E402

from registry import SERIES, THRESHOLDS, severity_for  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent


def _robust_z(x: np.ndarray) -> np.ndarray:
    med = np.median(x)
    scale = 1.4826 * np.median(np.abs(x - med))
    if scale <= 1e-12:
        scale = x.std()
    if scale <= 1e-12:
        return np.zeros_like(x)
    return (x - med) / scale


def _feature_matrix(kpis: pd.DataFrame, series=SERIES) -> np.ndarray:
    cols = []
    for spec in series:
        v = kpis[spec.metric_id].to_numpy(dtype=float)
        cols.append(np.log1p(v) if spec.log_transform else v)
    return np.column_stack(cols)


def _model(method: str, seed: int):
    if method == "ecod":
        return ECOD()
    if method == "copod":
        return COPOD()
    if method == "iforest":
        return IForest(random_state=seed)
    raise ValueError(f"unknown method: {method}")


def detect_multivariate(kpis: pd.DataFrame, method: str = "ecod", seed: int = 7,
                        series=SERIES, thresholds=THRESHOLDS) -> pd.DataFrame:
    th = thresholds
    X = _feature_matrix(kpis, series)
    clf = _model(method, seed)
    clf.fit(X)
    scores = np.asarray(clf.decision_scores_, dtype=float)
    z = _robust_z(scores)
    dates = pd.to_datetime(kpis["process_date"])

    model_uri = f"local:pyod.{method.upper()}"
    rows = []
    for i in np.nonzero(z >= th["z_flag"])[0]:
        za = float(z[i])
        day = str(pd.Timestamp(dates.iloc[i]).date())
        rows.append({
            "anomaly_id": f"mv_daily_kpi_vector:{day}:multivariate",
            "date": day,
            "plane": "batch-rescan",
            "domain": "gwam_retirement",
            "metric_id": "mv_daily_kpi_vector",
            "segment": None,
            "observed": round(float(scores[i]), 4),
            "expected": None,
            "score": round(za, 3),
            "threshold": th["z_flag"],
            "severity": severity_for(za),
            "class": "business",
            "model_uri": model_uri,
            "reconciled": False,
            "status": "open",
            "detector": "multivariate",
        })
    return pd.DataFrame(rows)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--kpis", type=Path,
                    default=REPO_ROOT / "data" / "detect" / "kpis_injected.parquet")
    ap.add_argument("--method", default="ecod", choices=["ecod", "copod", "iforest"])
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args(argv)
    kpis = pd.read_parquet(args.kpis)
    out = detect_multivariate(kpis, method=args.method, seed=args.seed)
    print(f"[multivariate:{args.method}] {len(out)} flagged days")
    if len(out):
        print(out[["date", "observed", "score", "severity"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
