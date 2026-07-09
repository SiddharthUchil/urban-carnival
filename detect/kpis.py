"""Aggregate the hit-level parquet into a daily KPI table for detection.

Reads only the columns the registry needs, collapses ~1.1M hit rows to one row per
calendar day, and emits one column per registered SeriesSpec (counts, rates, dimension
shares). The frame is reindexed to a gap-free daily calendar so a missing day surfaces as
zero volume rather than a silently dropped row. Output feeds detect/univariate.py,
detect/multivariate.py, and detect/rules.py.

    python detect/kpis.py --input data/synth/injected.parquet
    python detect/kpis.py --input data/synth/clean.parquet --label clean
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from registry import EVENT_IDS, SERIES  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = REPO_ROOT / "data" / "synth" / "injected.parquet"
OUT_DIR = REPO_ROOT / "data" / "detect"

NEEDED_COLS = [
    "process_date", "post_event_list", "post_pagename", "language",
    "mcvisid", "post_visid_high", "post_visid_low", "visit_num",
]


def _event_counts(df: pd.DataFrame, full_index: pd.DatetimeIndex) -> pd.DataFrame:
    """Per-day count of hits whose post_event_list contains each event id."""
    ev = df[["date", "post_event_list"]].copy()
    ev["ev"] = ev["post_event_list"].str.split(",")
    ev = ev.explode("ev")
    ev = ev[ev["ev"].notna() & (ev["ev"] != "")]
    counts = ev.groupby(["date", "ev"]).size().unstack(fill_value=0)
    return counts.reindex(index=full_index, columns=EVENT_IDS, fill_value=0)


def build_kpis(parquet_path: Path, series=SERIES) -> pd.DataFrame:
    df = pq.read_table(parquet_path, columns=NEEDED_COLS).to_pandas()
    df["date"] = pd.to_datetime(df["process_date"]).dt.normalize()
    full_index = pd.date_range(df["date"].min(), df["date"].max(), freq="D")

    hits = df.groupby("date").size().reindex(full_index, fill_value=0)
    vkey = (df["post_visid_high"].astype(str) + "_"
            + df["post_visid_low"].astype(str) + "_" + df["visit_num"].astype(str))
    visits = df.assign(_vk=vkey).groupby("date")["_vk"].nunique().reindex(full_index, fill_value=0)
    visitors = df.groupby("date")["mcvisid"].nunique().reindex(full_index, fill_value=0)
    ev_counts = _event_counts(df, full_index)

    out = pd.DataFrame(index=full_index)
    for spec in series:
        if spec.source == "hits":
            col = hits
        elif spec.source == "visits":
            col = visits
        elif spec.source == "visitors":
            col = visitors
        elif spec.source == "event" and spec.kind == "count":
            col = ev_counts[spec.event_id]
        elif spec.source == "event" and spec.kind == "rate":
            col = (ev_counts[spec.event_id] / hits).fillna(0.0)
        elif spec.source in ("pagename", "language") and spec.kind == "share":
            cnt = df[df[spec.dim] == spec.dim_value].groupby("date").size().reindex(full_index, fill_value=0)
            col = (cnt / hits).fillna(0.0)
        else:
            raise ValueError(f"unhandled series spec: {spec}")
        out[spec.metric_id] = col

    out.index.name = "process_date"
    return out.reset_index()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--label", default=None, help="output suffix (default: input file stem)")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = ap.parse_args(argv)

    label = args.label or args.input.stem
    kpis = build_kpis(args.input)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / f"kpis_{label}.parquet"
    kpis.to_parquet(out_path, index=False)
    print(f"[kpis] {args.input.name} -> {out_path.name}: "
          f"{len(kpis)} days x {len(kpis.columns) - 1} series")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
