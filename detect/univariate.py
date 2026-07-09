"""Univariate business-anomaly detection: robust seasonal-baseline residual scoring.

Each daily series is compared against a robust day-of-week seasonal baseline -- the median
of its trailing same-weekday values. This removes weekly seasonality without a fitted
forecaster: darts ExponentialSmoothing produced unstable one-step forecasts on these noisy
count series (forecasts nearly anti-correlated with reality), so residuals were dominated
by forecast error rather than anomalies -- it both over-flagged normal days and missed a
clean x0.4 dip. The trailing same-weekday median is echo-resistant (a single anomaly is one
of several values in the median) and gives residuals that cleanly separate the anomalies.

Two per-day signals fire and are OR-combined:
  1. robust-z: z = (r - median(r)) / (1.4826 * MAD(r)) over the scored region. MAD's 50%
     breakdown keeps a contaminated window from inflating the scale.
  2. an empirical high-quantile gate on |r|, gated by a softer z-floor so it refines the
     tail rather than always flagging the top 1% of clean noise.

detect_level_shifts runs a two-sided CUSUM over the same seasonal residuals of the top-line
volume series. A sustained shift too small to clear the per-day z (x1.35 is ~1 MAD/day but
many MADs in aggregate) accumulates until the CUSUM statistic crosses its threshold.

Counts are scored in log space (multiplicative anomalies -> additive residuals); rates and
shares raw. Severity comes from |z| via registry.severity_for.

    python detect/univariate.py --kpis data/detect/kpis_injected.parquet
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from registry import SERIES, THRESHOLDS, severity_for  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
MODEL_URI = "local:seasonal_dow_median"
LEVEL_SHIFT_URI = "local:cusum.seasonal_resid"
VOLUME_SOURCES = ("hits", "visits", "visitors")


def _robust_z(resid: np.ndarray) -> np.ndarray:
    med = np.median(resid)
    scale = 1.4826 * np.median(np.abs(resid - med))
    if scale <= 1e-12:                # constant-ish residuals: fall back to std
        scale = resid.std()
    if scale <= 1e-12:
        return np.zeros_like(resid)
    return (resid - med) / scale


def _mad(resid: np.ndarray) -> float:
    med = np.median(resid)
    scale = 1.4826 * np.median(np.abs(resid - med))
    return float(scale) if scale > 1e-12 else float(resid.std())


def _seasonal_baseline(v: np.ndarray, dow: np.ndarray, window: int, min_history: int) -> np.ndarray:
    """Trailing same-weekday median. NaN until min_history same-weekday priors exist."""
    exp = np.full(len(v), np.nan)
    for t in range(len(v)):
        prior = v[:t][dow[:t] == dow[t]]
        if len(prior) >= min_history:
            exp[t] = np.median(prior[-window:])
    return exp


def _scored_residuals(dates, values, log: bool, th: dict):
    """Deseasonalise a series -> (scored_idx, transformed_values, expected_full)."""
    v = np.log1p(values) if log else values.astype(float)
    n = len(v)
    dow = pd.DatetimeIndex(dates).dayofweek.to_numpy()
    exp_full = _seasonal_baseline(v, dow, th["dow_window"], th["dow_min_history"])
    warmup = int(th["start_frac"] * n)
    idx = np.array([t for t in range(warmup, n) if not np.isnan(exp_full[t])], dtype=int)
    return idx, v, exp_full


def _score_series(dates, values, log: bool, th: dict):
    """One series -> (dates, observed, expected, z, flag) over the scored region."""
    idx, v, exp_full = _scored_residuals(dates, values, log, th)
    if len(idx) == 0:
        empty = np.array([])
        return pd.DatetimeIndex([]), empty, empty, empty, np.array([], dtype=bool)
    fdates = pd.DatetimeIndex(dates)[idx]
    a = v[idx]
    f = exp_full[idx]
    resid = a - f
    z = _robust_z(resid)

    absr = np.abs(resid)
    qthresh = np.quantile(absr, th["high_quantile"])
    qflag = absr >= qthresh
    z_flag = np.abs(z) >= th["z_flag"]
    q_gated = qflag & (np.abs(z) >= th["quantile_z_floor"])
    flag = z_flag | q_gated

    obs = np.expm1(a) if log else a
    exp = np.expm1(f) if log else f
    return fdates, obs, exp, z, flag


def _row(spec, day, detector, observed, expected, score, threshold, severity, model_uri):
    return {
        "anomaly_id": f"{spec.metric_id}:{day}:{detector}",
        "date": day,
        "plane": "batch-rescan",
        "domain": "gwam_retirement",
        "metric_id": spec.metric_id,
        "segment": spec.dim_value,
        "observed": round(float(observed), 4),
        "expected": None if expected is None else round(float(expected), 4),
        "score": round(float(score), 3),
        "threshold": threshold,
        "severity": severity,
        "class": "business",
        "model_uri": model_uri,
        "reconciled": False,
        "status": "open",
        "detector": detector,
    }


def detect_univariate(kpis: pd.DataFrame, series=SERIES, thresholds=THRESHOLDS) -> pd.DataFrame:
    th = thresholds
    dates = pd.to_datetime(kpis["process_date"])
    rows = []
    for spec in series:
        values = kpis[spec.metric_id].to_numpy()
        if len(values) < th["min_history"] or np.all(values == values[0]):
            continue
        try:
            fdates, obs, exp, z, flag = _score_series(dates, values, spec.log_transform, th)
        except Exception as exc:      # a single ill-conditioned series must not sink the run
            print(f"[univariate] skipped {spec.metric_id}: {type(exc).__name__}: {exc}")
            continue
        for i in np.nonzero(flag)[0]:
            za = abs(float(z[i]))
            day = str(pd.Timestamp(fdates[i]).date())
            rows.append(_row(spec, day, "univariate", obs[i], exp[i], za,
                             th["z_flag"], severity_for(za), MODEL_URI))
    return pd.DataFrame(rows)


def detect_level_shifts(kpis: pd.DataFrame, series=SERIES, thresholds=THRESHOLDS) -> pd.DataFrame:
    """Adjacent-window mean-shift on the top-line volume series.

    For each day t, the mean log-level of the trailing ls_post_window days is compared with
    the mean of the ls_pre_window days immediately before it. Both windows are weekday-balanced
    (multiples of 7) so weekly seasonality cancels in the difference, and using the immediately
    preceding window as reference ignores slow seasonal trend while still exposing an abrupt
    sustained shift. The difference is standardized by the daily deseasonalized-residual scale;
    |z| >= ls_z flags a level change.

    A trailing baseline absorbs a sustained shift (it becomes the new normal within days) and
    a global reference flags the normal seasonal decline -- the adjacent-window contrast avoids
    both. Detection sensitivity is bounded by the series' own noise: on this curve a x1.35 shift
    is below the natural 14-day-window envelope and stays unflagged by design; x1.9 clears it.
    """
    th = thresholds
    dates = pd.to_datetime(kpis["process_date"])
    post_w, pre_w = th["ls_post_window"], th["ls_pre_window"]
    se_factor = np.sqrt(1.0 / post_w + 1.0 / pre_w)
    vol = [s for s in series if s.source in VOLUME_SOURCES]
    rows = []
    for spec in vol:
        values = kpis[spec.metric_id].to_numpy()
        if len(values) < th["min_history"]:
            continue
        v = np.log1p(values) if spec.log_transform else values.astype(float)
        n = len(v)
        idx, _, exp_full = _scored_residuals(dates, values, spec.log_transform, th)
        if len(idx) == 0:
            continue
        sigma = _mad(v[idx] - exp_full[idx])     # daily deseasonalized noise scale
        if sigma <= 1e-12:
            continue
        se = sigma * se_factor
        start = max(post_w + pre_w, int(th["start_frac"] * n))
        for t in range(start, n):
            post = float(v[t - post_w + 1: t + 1].mean())
            pre = float(v[t - post_w - pre_w + 1: t - post_w + 1].mean())
            z = (post - pre) / se
            if abs(z) < th["ls_z"]:
                continue
            day = str(pd.Timestamp(dates.iloc[t]).date())
            obs = np.expm1(post) if spec.log_transform else post
            exp = np.expm1(pre) if spec.log_transform else pre
            rows.append(_row(spec, day, "level_shift", obs, exp, abs(z),
                             th["ls_z"], severity_for(abs(z)), LEVEL_SHIFT_URI))
    return pd.DataFrame(rows)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--kpis", type=Path,
                    default=REPO_ROOT / "data" / "detect" / "kpis_injected.parquet")
    args = ap.parse_args(argv)
    kpis = pd.read_parquet(args.kpis)
    out = detect_univariate(kpis)
    shifts = detect_level_shifts(kpis)
    print(f"[univariate] {len(out)} flagged metric-days across "
          f"{out['metric_id'].nunique() if len(out) else 0} series")
    if len(out):
        print(out[["date", "metric_id", "observed", "expected", "score", "severity"]]
              .to_string(index=False))
    print(f"[level_shift] {len(shifts)} flagged metric-days")
    if len(shifts):
        print(shifts[["date", "metric_id", "observed", "expected", "score"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
