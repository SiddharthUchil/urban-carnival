"""Statistical-fidelity checks of clean.parquet against the EDA spec.

Each check returns a Result (name, status PASS/FAIL/SKIP, detail). Checks that need the
full 156-day curve (day-of-week index, global visitor cardinality, the EST->EDT clock
skew) are SKIPped — not failed — when run on a --limit-days subset. main() exits non-zero
if any enforced check FAILs.

    python synth/verify.py                 # verify data/synth/clean.parquet
    python synth/verify.py --data data/synth/injected.parquet
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spec import Spec, load_spec  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA = REPO_ROOT / "data" / "synth" / "clean.parquet"
FULL_DAYS = 156


@dataclass
class Result:
    name: str
    status: str          # PASS | FAIL | SKIP
    detail: str

    def __str__(self):
        icon = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "skip"}[self.status]
        return f"[{icon}] {self.name}: {self.detail}"


def _col(table, name) -> np.ndarray:
    return table.column(name).to_numpy(zero_copy_only=False)


def check_daily_counts(table, spec) -> Result:
    dates = _col(table, "process_date").astype("datetime64[D]").astype(str)
    uniq, counts = np.unique(dates, return_counts=True)
    gen = dict(zip(uniq, counts))
    exp = {str(d): h for d, h, *_ in spec.days}
    mism = [(k, int(gen.get(k)), exp.get(k)) for k in gen if gen.get(k) != exp.get(str(k))]
    if mism:
        return Result("daily_counts", "FAIL", f"{len(mism)} day(s) differ, e.g. {mism[:3]}")
    return Result("daily_counts", "PASS", f"{len(gen)} days match ts_daily exactly")


def check_dow_index(table, spec, full) -> Result:
    if not full:
        return Result("dow_index", "SKIP", "needs full 156-day curve")
    dates = _col(table, "process_date").astype("datetime64[D]")
    uniq, counts = np.unique(dates, return_counts=True)
    dow = (uniq.astype("int64") + 3) % 7                # 1970-01-01 was Thursday -> Mon=0
    per = np.zeros(7)
    cnt = np.zeros(7)
    for d, c in zip(dow, counts):
        per[d] += c
        cnt[d] += 1
    mean_by_dow = per / cnt
    gen_index = mean_by_dow / mean_by_dow.mean()
    diff = np.abs(gen_index - np.array(spec.dow_index))
    status = "PASS" if diff.max() < 0.05 else "FAIL"
    return Result("dow_index", status, f"max|delta|={diff.max():.4f} (<0.05)")


def check_hour_shares(table, spec, full) -> Result:
    dt = pd.to_datetime(pd.Series(_col(table, "date_time")),
                        format="%Y-%m-%d %H:%M:%S", errors="coerce").dropna()
    hours = dt.dt.hour.to_numpy()
    dow = dt.dt.dayofweek.to_numpy()
    worst = -99.0
    for k in range(7):
        mask = dow == k
        if mask.sum() == 0:
            continue
        h = np.bincount(hours[mask], minlength=24).astype(float)
        share = h / h.sum()
        delta = np.abs(share - np.array(spec.hour_probs[k])).mean() * 100
        tol = 1.5 if k >= 5 else 1.0
        worst = max(worst, delta - tol)
    status = "PASS" if worst <= 0 else "FAIL"
    return Result("hour_shares", status, f"worst mean|delta|-tol={worst:+.3f}pp (<=0)")


def check_population(table, spec) -> Result:
    worst = ("", 0.0)
    for c in spec.columns:
        if c.dtype != "string":
            continue
        arr = _col(table, c.col)
        nonblank = np.array([x is not None and x != "" for x in arr])
        gen = nonblank.mean() * 100
        delta = abs(gen - c.pop * 100)
        if delta > worst[1]:
            worst = (c.col, delta)
    status = "PASS" if worst[1] < 1.0 else "FAIL"
    return Result("population", status, f"worst |delta|={worst[1]:.3f}pp on {worst[0]} (<1.0pp)")


def check_dims(table, spec, full) -> Result:
    fails = []
    for name in ["post_pagename", "post_page_url", "ref_type", "browser", "language",
                 "connection_type", "va_closer_id"]:
        dim = spec.dims[name]
        arr = _col(table, name).astype(str)
        n = len(arr)
        vals, cnts = np.unique(arr, return_counts=True)
        share = dict(zip(vals, cnts / n * 100))
        for i, t in enumerate(dim.values[:10]):
            exp_pct = dim.probs[i + 1] * 100        # probs[0] is blank mass
            g = share.get(str(t), 0.0)
            if abs(g - exp_pct) > 1.5:
                fails.append(f"{name}:{str(t)[:16]} {g:.2f}vs{exp_pct:.2f}")
        cov_gen = sum(v for k, v in share.items() if k != "")
        if abs(cov_gen - dim.coverage * 100) > 1.0:
            fails.append(f"{name}:cov {cov_gen:.2f}vs{dim.coverage*100:.2f}")
        if full:
            distinct = len([v for v in vals if v != ""])
            if distinct < 0.8 * dim.apx_distinct:
                fails.append(f"{name}:distinct {distinct}vs{dim.apx_distinct}")
    status = "PASS" if not fails else "FAIL"
    return Result("dims", status, "ok" if not fails else f"{len(fails)} issues: {fails[:4]}")


def check_events(table, spec) -> Result:
    from collections import Counter
    arr = _col(table, "post_event_list").astype(str)
    lists = [s.split(",") if s else [] for s in arr]
    counts = np.array([len(x) for x in lists])
    p50, p95, mx = np.percentile(counts, 50), np.percentile(counts, 95), int(counts.max())
    fails = []
    if abs(p50 - spec.events.p50) > 1:
        fails.append(f"p50={p50}")
    if abs(p95 - spec.events.p95) > 1:
        fails.append(f"p95={p95}")
    if mx > spec.events.max:
        fails.append(f"max={mx}>{spec.events.max}")
    n = len(arr)
    flat = Counter()
    for x in lists:
        flat.update(set(x))
    for eid, p in spec.events.incl_prob.items():
        gen = flat.get(eid, 0) / n * 100
        if abs(gen - p * 100) > 1.0:
            fails.append(f"ev{eid} {gen:.2f}vs{p*100:.2f}")
    status = "PASS" if not fails else "FAIL"
    return Result("events", status,
                  f"p50={p50:.0f} p95={p95:.0f} max={mx}" +
                  ("" if not fails else f" ISSUES {fails[:4]}"))


def check_identity(table, spec, full) -> Result:
    fails = []
    pvh = _col(table, "post_visid_high").astype(str)
    pvl = _col(table, "post_visid_low").astype(str)
    vn = _col(table, "visit_num").astype(str)
    vpn = _col(table, "visit_page_num").astype(str)
    keys = np.char.add(np.char.add(pvh, "|"),
                       np.char.add(np.char.add(pvl, "|"),
                                   np.char.add(np.char.add(vn, "|"), vpn)))
    dup = len(keys) - len(np.unique(keys))
    if dup != 0:
        fails.append(f"dup_key={dup}")

    mc = _col(table, "mcvisid").astype(str)
    hits_per_visitor = len(mc) / len(np.unique(mc))
    if full:
        distinct = len(np.unique(mc))
        if not (0.9 * 51076 <= distinct <= 1.1 * 51076):
            fails.append(f"mcvisid_distinct={distinct}")
    if not re.fullmatch(r"\d{38}", mc[0]):
        fails.append(f"mcvisid_shape={mc[0][:12]}")
    ua = _col(table, "user_agent").astype(str)
    if any(bad in " ".join(ua[:200]) for bad in ("Mozilla", "Windows NT", "AppleWebKit")):
        fails.append("ua_realistic")
    pz = _col(table, "post_zip").astype(str)
    if any(re.fullmatch(r"[A-Za-z]\d[A-Za-z] ?\d[A-Za-z]\d", z) for z in pz[:200]):
        fails.append("zip_realistic")
    status = "PASS" if not fails else "FAIL"
    return Result("identity", status,
                  f"0 dup keys, hits/visitor={hits_per_visitor:.2f}" +
                  ("" if not fails else f" ISSUES {fails[:4]}"))


def check_clock_skew(table, spec, full) -> Result:
    if not full:
        return Result("clock_skew", "SKIP", "needs EST+EDT days (full run)")
    dts = pd.Series(_col(table, "date_time"))
    gmts = pd.Series(_col(table, "hit_time_gmt"))
    mask = dts.notna() & (dts != "") & gmts.notna() & (gmts != "")
    dtv = pd.to_datetime(dts[mask], format="%Y-%m-%d %H:%M:%S").astype("int64") // 10**9
    gmtv = gmts[mask].astype("int64")
    diff = (gmtv - dtv).to_numpy()
    p = [int(np.percentile(diff, q)) for q in (5, 50, 95)]
    status = "PASS" if p == spec.clock_skew else "FAIL"
    return Result("clock_skew", status, f"p5/p50/p95={p} (want {spec.clock_skew})")


def run(data: Path, spec: Spec) -> list[Result]:
    table = pq.read_table(data)
    n_days = len(np.unique(_col(table, "process_date").astype("datetime64[D]")))
    full = n_days >= FULL_DAYS
    return [
        check_daily_counts(table, spec),
        check_dow_index(table, spec, full),
        check_hour_shares(table, spec, full),
        check_population(table, spec),
        check_dims(table, spec, full),
        check_events(table, spec),
        check_identity(table, spec, full),
        check_clock_skew(table, spec, full),
    ]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, default=DEFAULT_DATA)
    args = ap.parse_args(argv)
    spec = load_spec()
    results = run(args.data, spec)
    print(f"=== fidelity: {args.data} ===")
    for r in results:
        print(r)
    failed = [r for r in results if r.status == "FAIL"]
    print(f"=== {sum(r.status=='PASS' for r in results)} pass, "
          f"{len(failed)} fail, {sum(r.status=='SKIP' for r in results)} skip ===")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
