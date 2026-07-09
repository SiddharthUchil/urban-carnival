"""Operational / data-quality rules: deterministic coverage and volume checks.

Local stand-in for Databricks Lakehouse Monitoring's freshness/completeness anomaly
detection. Three declarative rules, all class "operational":

  zero_volume     day with no hits at all.
  event_coverage  an always-on event id (fires on >= min_active_frac of days) drops to
                  zero on a day -> tagging loss. This deterministically catches the
                  event_drop scenario.
  dim_coverage    a dimension's blank rate jumps vs its trailing median, or a new value
                  with material share appears that was unseen in the trailing window.

    python detect/rules.py --input data/synth/injected.parquet --kpis data/detect/kpis_injected.parquet
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from registry import EVENT_IDS, RULES  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent


def _row(metric_id, day, detector, observed, expected, severity, segment=None):
    return {
        "anomaly_id": f"{metric_id}:{day}:{detector}",
        "date": day,
        "plane": "batch-rescan",
        "domain": "gwam_retirement",
        "metric_id": metric_id,
        "segment": segment,
        "observed": None if observed is None else round(float(observed), 4),
        "expected": None if expected is None else round(float(expected), 4),
        "score": None,
        "threshold": None,
        "severity": severity,
        "class": "operational",
        "model_uri": f"local:rule.{detector}",
        "reconciled": False,
        "status": "open",
        "detector": detector,
    }


def _zero_volume(kpis: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in kpis.iterrows():
        if r["hits_total"] == 0:
            day = str(pd.Timestamp(r["process_date"]).date())
            rows.append(_row("hits_total", day, "zero_volume", 0, None, "critical"))
    return rows


def _event_coverage(kpis: pd.DataFrame, min_active_frac: float) -> list[dict]:
    days = pd.to_datetime(kpis["process_date"])
    rows = []
    for eid in EVENT_IDS:
        col = f"event_{eid}_count"
        counts = kpis[col].to_numpy()
        active_frac = float((counts > 0).mean())
        if active_frac < min_active_frac:
            continue                       # not an always-on event; zeros are normal
        for i in np.nonzero(counts == 0)[0]:
            day = str(pd.Timestamp(days.iloc[i]).date())
            rows.append(_row(col, day, "event_coverage", 0, None, "critical"))
    return rows


def _dim_coverage(parquet_path: Path, dims, blank_jump_pp, new_value_share, trailing_days) -> list[dict]:
    cols = ["process_date"] + list(dims)
    df = pq.read_table(parquet_path, columns=cols).to_pandas()
    df["date"] = pd.to_datetime(df["process_date"]).dt.normalize()
    all_days = np.sort(df["date"].unique())
    rows = []
    for dim in dims:
        val = df[dim].astype("string").fillna("")
        tab = pd.crosstab(df["date"], val).reindex(index=all_days, fill_value=0)
        totals = tab.sum(axis=1).replace(0, np.nan)
        blank_share = (tab[""] / totals if "" in tab.columns
                       else pd.Series(0.0, index=all_days)).fillna(0.0)
        value_cols = [c for c in tab.columns if c != ""]
        share = tab[value_cols].div(totals, axis=0).fillna(0.0)
        present = tab[value_cols] > 0

        for k in range(len(all_days)):
            day = str(pd.Timestamp(all_days[k]).date())
            if k >= 14:                    # blank-rate jump vs trailing median
                base = float(np.median(blank_share.iloc[max(0, k - trailing_days):k]))
                if blank_share.iloc[k] - base > blank_jump_pp / 100.0:
                    rows.append(_row(f"{dim}_blank_rate", day, "dim_coverage",
                                     blank_share.iloc[k], base, "major"))
            if k >= trailing_days:         # new high-share value unseen in trailing window
                seen = present.iloc[k - trailing_days:k].any(axis=0)
                for v in value_cols:
                    if share.iloc[k][v] > new_value_share and not seen[v]:
                        rows.append(_row(f"{dim}_new_value", day, "dim_coverage",
                                         share.iloc[k][v], 0.0, "major", segment=v))
    return rows


def run_rules(parquet_path: Path, kpis: pd.DataFrame, rules=RULES) -> pd.DataFrame:
    out = []
    for rule in rules:
        if rule["kind"] == "zero_volume":
            out += _zero_volume(kpis)
        elif rule["kind"] == "event_coverage":
            out += _event_coverage(kpis, rule["min_active_frac"])
        elif rule["kind"] == "dim_coverage":
            out += _dim_coverage(parquet_path, rule["dims"], rule["blank_jump_pp"],
                                 rule["new_value_share"], rule["trailing_days"])
    return pd.DataFrame(out)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=REPO_ROOT / "data" / "synth" / "injected.parquet")
    ap.add_argument("--kpis", type=Path, default=REPO_ROOT / "data" / "detect" / "kpis_injected.parquet")
    args = ap.parse_args(argv)
    kpis = pd.read_parquet(args.kpis)
    out = run_rules(args.input, kpis)
    print(f"[rules] {len(out)} flagged")
    if len(out):
        print(out[["date", "metric_id", "detector", "observed", "severity"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
