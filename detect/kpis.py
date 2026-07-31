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

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from registry import EVENT_IDS, PV_BUCKETS, SERIES  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = REPO_ROOT / "data" / "synth" / "injected.parquet"
OUT_DIR = REPO_ROOT / "data" / "detect"

NEEDED_COLS = [
    "process_date", "post_event_list", "post_pagename", "post_page_event", "language",
    "mcvisid", "post_visid_high", "post_visid_low", "visit_num",
]

PAGE_VIEW_BASES = ("all_hits", "adobe_pv")


def _pv_flag(df: pd.DataFrame, basis: str) -> pd.Series:
    """Per-hit boolean: does this hit count as a page view under ``basis``?

    "adobe_pv" mirrors Spark's try_cast(post_page_event as int) == 0 -- errors="coerce"
    turns a non-numeric value into NaN, which fails the == 0 test instead of raising. That
    is deliberate ANSI parity: a bare cast would kill the Databricks job (the CoverMe E1
    defect), so neither side may raise on dirty input.
    """
    if basis == "all_hits":
        return pd.Series(True, index=df.index)
    return pd.to_numeric(df["post_page_event"], errors="coerce") == 0


def _bucket_labels(counts: np.ndarray) -> np.ndarray:
    """Per-visit page-view count -> C12 bucket label."""
    return np.select(
        [counts == 0, counts == 1, counts == 2, counts <= 5],
        ["0", "1", "2", "3-5"], default="6+")


def _event_counts(df: pd.DataFrame, full_index: pd.DatetimeIndex) -> pd.DataFrame:
    """Per-day count of hits whose post_event_list contains each event id."""
    ev = df[["date", "post_event_list"]].copy()
    ev["ev"] = ev["post_event_list"].str.split(",")
    ev = ev.explode("ev")
    ev = ev[ev["ev"].notna() & (ev["ev"] != "")]
    counts = ev.groupby(["date", "ev"]).size().unstack(fill_value=0)
    return counts.reindex(index=full_index, columns=EVENT_IDS, fill_value=0)


def build_kpis(parquet_path: Path, series=SERIES,
               page_view_basis="all_hits") -> pd.DataFrame:
    if page_view_basis not in PAGE_VIEW_BASES:
        raise ValueError(f"unknown page_view_basis: {page_view_basis!r}")
    df = pq.read_table(parquet_path, columns=NEEDED_COLS).to_pandas()
    df["date"] = pd.to_datetime(df["process_date"]).dt.normalize()
    full_index = pd.date_range(df["date"].min(), df["date"].max(), freq="D")

    hits = df.groupby("date").size().reindex(full_index, fill_value=0)
    vkey = (df["post_visid_high"].astype(str) + "_"
            + df["post_visid_low"].astype(str) + "_" + df["visit_num"].astype(str))
    visits = df.assign(_vk=vkey).groupby("date")["_vk"].nunique().reindex(full_index, fill_value=0)
    visitors = df.groupby("date")["mcvisid"].nunique().reindex(full_index, fill_value=0)
    ev_counts = _event_counts(df, full_index)

    pv = _pv_flag(df, page_view_basis)
    page_views = (df.assign(_pv=pv.astype("int64")).groupby("date")["_pv"].sum()
                  .reindex(full_index, fill_value=0))

    # Per-visit page-view buckets, DATE-FIRST: group by (date, visit key) so the bucket
    # denominator is exactly visits_total. Probe C12 instead grouped by visit key alone and
    # assigned each visit to min(process_date); visits spanning midnight therefore land in
    # both grains differently, so these shares deviate slightly from C12's published
    # figures BY DESIGN. Do not "correct" them toward C12 -- that would desynchronise the
    # shares from the visits_total they divide by.
    per_visit = (df.assign(_vk=vkey, _pv=pv.astype("int64"))
                   .groupby(["date", "_vk"], sort=False)["_pv"].sum())
    bcounts = (per_visit.reset_index()
               .assign(_b=lambda t: _bucket_labels(t["_pv"].to_numpy()))
               .groupby(["date", "_b"]).size().unstack(fill_value=0)
               .reindex(index=full_index, columns=PV_BUCKETS, fill_value=0))
    bucket_shares = bcounts.div(visits, axis=0).where(visits > 0, 0.0)

    # Two passes: ratios reference sibling metric expressions, so build those first
    # (mirrors gold_lib.build_kpis_spark).
    exprs: dict[str, pd.Series] = {}
    for spec in series:
        if spec.kind == "ratio":
            continue
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
        elif spec.source == "page_views" and spec.kind == "count":
            col = page_views
        elif spec.source == "visit_pv_bucket" and spec.kind == "point_mass":
            col = bucket_shares[spec.dim_value]
        elif spec.source in ("pagename", "language") and spec.kind == "share":
            cnt = df[df[spec.dim] == spec.dim_value].groupby("date").size().reindex(full_index, fill_value=0)
            col = (cnt / hits).fillna(0.0)
        else:
            raise ValueError(f"unhandled series spec: {spec}")
        exprs[spec.metric_id] = col
    for spec in series:
        if spec.kind != "ratio":
            continue
        if spec.numerator not in exprs or spec.denominator not in exprs:
            raise ValueError(f"ratio {spec.metric_id} references unknown metric_ids: "
                             f"{spec.numerator} / {spec.denominator}")
        num, den = exprs[spec.numerator], exprs[spec.denominator]
        # .where(den > 0, 0.0) rather than fillna: a 0 denominator with a non-zero
        # numerator divides to inf, which fillna would not catch. Matches gold_lib's
        # when(den > 0, ...).otherwise(0.0) -- never NULL/inf.
        exprs[spec.metric_id] = num.div(den).where(den > 0, 0.0)

    out = pd.DataFrame(index=full_index)
    for spec in series:                      # series order == gold_lib's column order
        out[spec.metric_id] = exprs[spec.metric_id]

    out.index.name = "process_date"
    return out.reset_index()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--label", default=None, help="output suffix (default: input file stem)")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    ap.add_argument("--page-view-basis", choices=PAGE_VIEW_BASES, default="all_hits",
                    help="what 'page views' counts (doc 20 Q6); production passes "
                         "databricks/conf/settings.PAGE_VIEW_BASIS")
    args = ap.parse_args(argv)

    label = args.label or args.input.stem
    kpis = build_kpis(args.input, page_view_basis=args.page_view_basis)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / f"kpis_{label}.parquet"
    kpis.to_parquet(out_path, index=False)
    print(f"[kpis] {args.input.name} -> {out_path.name}: "
          f"{len(kpis)} days x {len(kpis.columns) - 1} series")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
