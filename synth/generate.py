"""Generate a privacy-safe synthetic replica of the GWAM Canada-Retirement hit table.

Input:  synth/spec/*.json  (extracted EDA output; the sole source of truth)
Output: data/synth/clean.parquet   (row-level, all 120 schema columns)
        data/synth/known_events.json (real anomalies baked into the curve: the
                                       2026-07-06 spike + level-shift dates)

Every value is synthetic. Sensitive columns are shape-only fakes; masked evar/prop
values reuse the already-anonymized "<masked:...>" tokens from the EDA. No real PII,
user agents, postal codes, or URLs are ever emitted.

Generation is per-day (one chunk per calendar day) and fully seeded, so the same
--seed reproduces byte-identical output. Run:

    python synth/generate.py --seed 42 --out data/synth
    python synth/generate.py --seed 42 --limit-days 5      # fast dev subset
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spec import ColumnSpec, Spec, load_spec  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO_ROOT / "data" / "synth"

# US/Canada DST 2026: EST (-5h) before Mar 8, EDT (-4h) from Mar 8. Matches the EDA's
# clock_skew percentiles [-18000, -14400, -14400] without hand-tuning a fraction.
DST_START = dt.date(2026, 3, 8)
EST_OFFSET, EDT_OFFSET = -18000, -14400

# Columns filled by bespoke logic; everything else falls through to _generic_series.
DIM_COLS = [
    "post_pagename", "post_page_url", "post_page_event", "ref_type", "connection_type",
    "language", "hit_source", "exclude_hit", "duplicate_purchase", "browser", "va_closer_id",
]
IDENTITY_LINKED = {"mcvisid", "post_visid_high", "post_visid_low"}
CONST_LOW_CARD = {  # cardinality-1 shape-only fakes (length echoes identity/schema shape)
    "visid_high": "0", "visid_low": "0", "visid_new": "1", "visid_type": "0",
    "cookies": "1", "userid": "syn000001", "username": "syn_user_00000001",
    "user_hash": "synhash001", "post_visid_type": "0", "post_persistent_cookie": "1",
}


def _tz_offset(day: dt.date) -> int:
    return EST_OFFSET if day < DST_START else EDT_OFFSET


def _rand_digit_pool(rng: np.random.Generator, n: int, length: int) -> np.ndarray:
    """n distinct-ish fixed-length digit strings, vectorized via a byte view."""
    codes = rng.integers(48, 58, size=(n, length), dtype=np.uint8)  # ascii '0'..'9'
    return np.ascontiguousarray(codes).view(f"S{length}").ravel().astype(str)


class IdentityPool:
    """Global fake-visitor pool. Pool size == mcvisid distinct so cross-day reuse
    reproduces the observed visitor cardinality (~51k)."""

    def __init__(self, spec: Spec, rng: np.random.Generator):
        self.size = spec.identity_cols["mcvisid"]["distinct"]
        self.mcvisid = _rand_digit_pool(rng, self.size, 38)
        self.pvh = _rand_digit_pool(rng, self.size, 19)
        self.pvl = _rand_digit_pool(rng, self.size, 19)
        # Lifetime visit counter per visitor -> makes (pvh,pvl,visit_num,visit_page_num) unique.
        self.visit_counter = np.zeros(self.size, dtype=np.int64)


def _assign_visits(rng, n_hits, n_visits, n_visitors):
    """Return per-visit (visitor_local, visit_num_rank, size) and per-hit (visit_idx, page_num).

    visits split across visitors and hits split across visits by multinomial, so every
    visitor has >=1 visit and every visit has >=1 hit; sums land exactly on the targets.
    """
    n_visits = min(n_visits, n_hits)
    n_visitors = min(n_visitors, n_visits)

    vpv = 1 + rng.multinomial(n_visits - n_visitors, np.full(n_visitors, 1 / n_visitors))
    sizes = 1 + rng.multinomial(n_hits - n_visits, np.full(n_visits, 1 / n_visits))

    visitor_of_visit = np.repeat(np.arange(n_visitors), vpv)           # len n_visits
    within_visitor_rank = (np.arange(n_visits)
                           - np.repeat(np.cumsum(vpv) - vpv, vpv) + 1)  # 1..vpv per visitor

    hit_of_visit = np.repeat(np.arange(n_visits), sizes)              # len n_hits
    page_num = (np.arange(n_hits)
                - np.repeat(np.cumsum(sizes) - sizes, sizes) + 1)      # 1..size per visit
    return visitor_of_visit, within_visitor_rank, sizes, hit_of_visit, page_num, vpv


# Per-visit page-view mix measured by probe C12 (public website, 88 days, doc 20 Q6):
# 0pv 3.3% / 1pv 78.8% / 2pv 11.7% / 3-5pv 5.2% / 6+ 1.1%. The spec profile records
# post_page_event as "0" on 100% of rows, which would make the adobe_pv basis identical to
# all_hits and pin share_pv_eq_0 at exactly zero -- i.e. every page-view test would pass
# vacuously. These shares exist so the fixture can falsify the basis code.
#
# MEASURED CEILING -- the "6+" target is not reachable and is not a bug. Synthetic visit
# sizes come from a multinomial split of hits across visits, whose tail is far thinner than
# real traffic: P(size>=6) is 0.006% and the largest visit is 6 hits, so at most ~0.01% of
# visits can hold 6 page views against the 1.1% asked for. Achieved mix is
# 3.3 / 78.7 / 12.6 / 5.4 / 0.01 -- bucket 2 runs slightly rich because visits assigned a
# high target get clipped down into it. Both GOVERNED buckets (0 and 2, per
# research/claude/metric-registry.yaml) land on target; only the ungoverned tail misses.
# Do not "fix" this by widening the visit-size distribution: that would move all 35 existing
# series and destroy the property that regenerating changes post_page_event and nothing else.
PV_MIX = {"0": 0.033, "2": 0.117, "3-5": 0.052, "6+": 0.011}   # "1" takes the remainder
# Dedicated seed: page-view assignment draws from its OWN generator so it does not advance
# the main per-day RNG stream. That keeps every other synthetic column bit-identical to the
# pre-change fixture, so regenerating changes post_page_event and nothing else.
PV_SEED = 20260730


def _page_view_counts(day, sizes):
    """Per-visit page-view count reproducing PV_MIX as closely as the visit sizes allow.

    A visit cannot have more page views than hits, so the target counts are allocated to
    the LARGEST visits first (random within equal sizes) rather than drawn independently
    and clipped -- independent draws would collapse toward bucket 1, because most synthetic
    visits are 1-2 hits, and would silently undershoot the mix.

    Zero-page-view visits are allocated from the remainder: those are visits that record
    activity but no page, which is the real signal behind the SME's "page views per visit
    < 1" suggestion (doc 20 Part 4 item 2).
    """
    rng = np.random.default_rng(PV_SEED + day.toordinal())
    n = len(sizes)
    order = np.lexsort((rng.random(n), -sizes))    # size desc, random tiebreak
    pv = np.ones(n, dtype=np.int64)

    i = 0
    for label, hi, lo in (("6+", 9, 6), ("3-5", 6, 3)):
        take = order[i:i + int(round(PV_MIX[label] * n))]
        pv[take] = rng.integers(lo, hi, size=len(take))
        i += len(take)
    take = order[i:i + int(round(PV_MIX["2"] * n))]
    pv[take] = 2
    i += len(take)

    rest = order[i:]
    n0 = min(int(round(PV_MIX["0"] * n)), len(rest))
    pv[rest[rng.choice(len(rest), size=n0, replace=False)]] = 0
    return np.minimum(pv, sizes)


def _dim_series(rng, dim, n):
    idx = rng.choice(len(dim.choices), size=n, p=dim.probs)
    return np.asarray(dim.choices, dtype=object)[idx]


def _masked_series(rng, col: ColumnSpec, n):
    """Reproduce masked evar/prop values: published tokens at their pct, plus a
    length-matched synthetic tail to reach apx_distinct."""
    toks = [t["v"] for t in col.top_values]
    masses = [max(0.0, t["pct"]) / 100.0 for t in col.top_values]
    top_mass = sum(masses)
    if top_mass > 1.0:
        masses = [m / top_mass for m in masses]
        top_mass = 1.0
    n_tail = max(0, col.apx_distinct - len(toks))
    tail_mass = max(0.0, 1.0 - top_mass)
    if n_tail > 0 and tail_mass > 1e-9:
        tlen = col.len_p50 or (col.top_values[0]["len"] if col.top_values else 8)
        tail_labels = [f"<syn:{col.col}:{i:05d}:{'x' * int(tlen)}>" for i in range(n_tail)]
        w = np.array([1.0 / (i + 1) for i in range(n_tail)])
        tail_probs = tail_mass * w / w.sum()
        choices = toks + tail_labels
        probs = np.array(masses + list(tail_probs))
    else:
        choices = toks
        probs = np.array(masses)
    probs = probs / probs.sum()
    idx = rng.choice(len(choices), size=n, p=probs)
    return np.asarray(choices, dtype=object)[idx]


def _generic_series(rng, col: ColumnSpec, n, row_offset):
    """Fallback for non-special columns: Zipf pool sized to apx_distinct."""
    if col.col in ("hitid_high", "hitid_low"):
        # Unique per row across the whole run -> monotone global index as a digit string.
        base = 10**14 if col.col == "hitid_high" else 5 * 10**14
        return (base + row_offset + np.arange(n)).astype(str).astype(object)
    if col.apx_distinct <= 1:
        return np.full(n, f"{col.col}_v0", dtype=object)
    k = min(col.apx_distinct, 5000)
    w = 1.0 / (np.arange(1, k + 1))
    p = w / w.sum()
    idx = rng.choice(k, size=n, p=p)
    pool = np.array([f"{col.col}_v{i}" for i in range(k)], dtype=object)
    return pool[idx]


def _apply_blanks(rng, values, pop):
    """Blank out (1-pop) of the rows to honor the column's populated fraction."""
    if pop >= 0.99995:
        return values
    blank = rng.random(len(values)) >= pop
    values[blank] = ""
    return values


_EVENT_CACHE: dict = {}


def _event_list(rng, spec, n):
    ev = spec.events
    if "tokens" not in _EVENT_CACHE:
        _EVENT_CACHE["tokens"] = np.array([e + "," for e in ev.ids], dtype=object)
        _EVENT_CACHE["probs"] = np.array([ev.incl_prob[e] for e in ev.ids])
        _EVENT_CACHE["always"] = _EVENT_CACHE["probs"] >= 1.0
    present = rng.random((n, len(ev.ids))) < _EVENT_CACHE["probs"][None, :]
    present[:, _EVENT_CACHE["always"]] = True
    tokens = np.where(present, _EVENT_CACHE["tokens"][None, :], "")
    joined = tokens.sum(axis=1).astype(str)
    return np.char.rstrip(joined, ",").astype(object)


def generate_day(spec, day, hits, visits, visitors, rng, pool, ua_pool, row_offset):
    dow = day.weekday()
    (visitor_of_visit, within_rank, sizes,
     hit_of_visit, page_num, vpv) = _assign_visits(rng, hits, visits, visitors)
    n_visits = len(sizes)
    n_visitors = len(vpv)
    n = hits
    pv_of_visit = _page_view_counts(day, sizes)

    # Map local visitors -> distinct global pool indices; advance lifetime visit counters.
    selected = rng.choice(pool.size, size=n_visitors, replace=False)
    base_visit_num = pool.visit_counter[selected][visitor_of_visit]
    visit_num_visit = base_visit_num + within_rank
    np.add.at(pool.visit_counter, selected, vpv)
    visitor_global_visit = selected[visitor_of_visit]

    # Per-visit time: one hour drawn from the day-of-week hour profile; hits step within it.
    visit_hour = rng.choice(24, size=n_visits, p=spec.hour_probs[dow])
    visit_base_sec = visit_hour * 3600 + rng.integers(0, 3000, size=n_visits)

    # Broadcast visit-level arrays to hit level.
    visitor_hit = visitor_global_visit[hit_of_visit]
    visit_num_hit = visit_num_visit[hit_of_visit]
    sec = visit_base_sec[hit_of_visit] + (page_num - 1) * 20
    sec = np.minimum(sec, 86399)

    midnight = int((day - dt.date(1970, 1, 1)).days) * 86400   # pure-date UTC epoch
    local_epoch = midnight + sec
    offset = _tz_offset(day)
    hit_gmt = local_epoch + offset
    visit_start_gmt = (midnight + visit_base_sec + offset)[hit_of_visit]

    ts = pd.to_datetime(local_epoch, unit="s")
    cols: dict[str, np.ndarray] = {}

    for c in spec.columns:
        name = c.col
        if name in IDENTITY_LINKED:
            arr = {"mcvisid": pool.mcvisid, "post_visid_high": pool.pvh,
                   "post_visid_low": pool.pvl}[name][visitor_hit]
            cols[name] = arr.astype(object)
        elif name in CONST_LOW_CARD:
            cols[name] = np.full(n, CONST_LOW_CARD[name], dtype=object)
        elif name == "visit_num":
            cols[name] = visit_num_hit.astype(str).astype(object)
        elif name == "visit_page_num":
            cols[name] = page_num.astype(str).astype(object)
        elif name == "visit_start_time_gmt":
            cols[name] = visit_start_gmt.astype(str).astype(object)
        elif name == "date_time":
            cols[name] = ts.strftime("%Y-%m-%d %H:%M:%S").astype(object).to_numpy()
        elif name in ("hit_time_gmt", "cust_hit_time_gmt", "first_hit_time_gmt",
                      "last_hit_time_gmt"):
            cols[name] = hit_gmt.astype(str).astype(object)
        elif name == "visid_timestamp":
            cols[name] = visit_start_gmt.astype(str).astype(object)
        elif name == "process_timestamp":
            cols[name] = np.full(n, pd.Timestamp(day) + pd.Timedelta(hours=4))
        elif name in ("process_date", "As_of_date"):
            cols[name] = np.full(n, pd.Timestamp(day))
        elif name in ("post_event_list", "event_list"):
            cols[name] = _event_list(rng, spec, n)
        elif name in DIM_COLS:
            if name == "exclude_hit":
                cols[name] = np.full(n, "0", dtype=object)
            elif name == "hit_source":
                cols[name] = np.full(n, "1", dtype=object)
            elif name == "post_page_event":
                # The discarded draw is deliberate, NOT dead code: it advances the shared
                # per-day RNG exactly as before so every column generated after this one is
                # bit-identical to the pre-change fixture. Without it the stream shifts and
                # regenerating silently perturbs all 35 existing series.
                _dim_series(rng, spec.dims[name], n)
                # Visit-level, not an iid per-hit draw: the first pv hits of each visit are
                # page views ("0"), the rest link-tracks ("1"). An iid draw could not
                # produce zero-page-view VISITS, which is the whole point (see PV_MIX).
                cols[name] = np.where(page_num <= pv_of_visit[hit_of_visit],
                                      "0", "1").astype(object)
            else:
                cols[name] = _dim_series(rng, spec.dims[name], n)
        elif c.top_values:
            cols[name] = _masked_series(rng, c, n)
        elif name == "user_agent":
            cols[name] = ua_pool[rng.integers(0, len(ua_pool), size=n)]
        elif name == "post_zip":
            cols[name] = np.char.add("syn-zip-",
                                     rng.integers(0, 99999, size=n).astype(str)).astype(object)
        elif name == "rsid":
            cols[name] = np.full(n, spec.meta["scope"]["rsid"], dtype=object)
        else:
            cols[name] = _generic_series(rng, c, n, row_offset)

        # Honor populated fraction for string columns (dates/timestamps stay full).
        if c.dtype == "string" and name not in IDENTITY_LINKED:
            cols[name] = _apply_blanks(rng, cols[name], c.pop)

    df = pd.DataFrame(cols, columns=spec.col_names)
    for c in spec.columns:
        if c.dtype == "string":
            df[c.col] = df[c.col].astype("string")
        else:  # timestamp / date
            df[c.col] = pd.to_datetime(df[c.col])
    return df


def _ua_pool(rng, n_distinct):
    return np.array([f"synthetic-ua/{i:04d} (shape-only)" for i in range(n_distinct)],
                    dtype=object)


def _known_events(spec) -> dict:
    hits = {str(d): h for d, h, *_ in spec.days}
    return {
        "note": "Real anomalies present in the clean curve; exclude these windows when scoring injected recall.",
        "spike": {"date": "2026-07-06", "hits": hits.get("2026-07-06")},
        "level_shift_dates": spec.level_shift_dates,
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--limit-days", type=int, default=None, help="generate only first N days (dev)")
    args = ap.parse_args(argv)

    spec = load_spec()
    days = spec.days[: args.limit_days] if args.limit_days else spec.days
    args.out.mkdir(parents=True, exist_ok=True)

    pool_rng = np.random.default_rng(args.seed)
    pool = IdentityPool(spec, pool_rng)
    ua_pool = _ua_pool(pool_rng,
                       next(c.apx_distinct for c in spec.columns if c.col == "user_agent"))

    writer = None
    schema = None
    row_offset = 0
    total = sum(d[1] for d in days)
    print(f"[generate] {len(days)} days, {total:,} rows, pool={pool.size:,} visitors, seed={args.seed}")
    for i, (day, hits, visits, visitors) in enumerate(days):
        rng = np.random.default_rng([args.seed, day.toordinal()])
        df = generate_day(spec, day, hits, visits, visitors, rng, pool, ua_pool, row_offset)
        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            schema = table.schema
            writer = pq.ParquetWriter(args.out / "clean.parquet", schema)
        writer.write_table(table.cast(schema))
        row_offset += hits
        if (i + 1) % 20 == 0 or i + 1 == len(days):
            print(f"[generate]  {i + 1}/{len(days)} days  {row_offset:,} rows")
    writer.close()

    (args.out / "known_events.json").write_text(
        json.dumps(_known_events(spec), indent=2), encoding="utf-8")
    print(f"[generate] wrote {args.out / 'clean.parquet'}  ({row_offset:,} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
