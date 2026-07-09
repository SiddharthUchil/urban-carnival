"""Inject labeled anomalies into clean.parquet for detection-recall evaluation.

Reads the clean baseline and writes:
  data/synth/injected.parquet  (baseline + injected anomalies)
  data/synth/manifest.json     (ground truth: type, metric, scope, window, magnitude, realized)

Anomaly types (all target schema-resident columns):
  spike        volume x m on a single day (append resampled rows with fresh hit ids)
  dip          volume x m (<1) over a window (drop rows)
  level_shift  sustained volume x m over a multi-day window
  dim_mix_shift  force a delta-share of a dimension's rows to one value (volume unchanged)
  event_drop   remove one event id from post_event_list over a window (tagging loss)

The clean file is never modified. Scenarios default to windows that avoid the real
anomalies recorded in known_events.json, so injected recall can be scored cleanly.

    python synth/inject.py --seed 7
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DIR = REPO_ROOT / "data" / "synth"

# Windows chosen to avoid known_events.json (07-06 spike; level shifts 02-11/12, 03-31, 04-01/02).
DEFAULT_SCENARIOS = [
    {"id": "spike_2026-03-10", "type": "spike", "metric": "hits",
     "start": "2026-03-10", "end": "2026-03-10", "magnitude": 2.5},
    {"id": "dip_2026-05-20", "type": "dip", "metric": "hits",
     "start": "2026-05-20", "end": "2026-05-21", "magnitude": 0.4},
    # magnitude 1.9x, not 1.35x: a x1.35 sustained shift sits below this curve's natural
    # noise floor -- ~32 clean 14-day windows swing wider than x1.35, so no windowed
    # detector can flag it without flooding false positives (see detect/univariate.py
    # detect_level_shifts). x1.9 clears the natural envelope and is genuinely detectable.
    {"id": "level_shift_2026-04-20", "type": "level_shift", "metric": "hits",
     "start": "2026-04-20", "end": "2026-05-03", "magnitude": 1.9},
    {"id": "dim_mix_2026-06-10", "type": "dim_mix_shift", "metric": "post_pagename_share",
     "start": "2026-06-10", "end": "2026-06-12", "magnitude": 0.20,
     "dim": "post_pagename", "value": "ca-ret:personal:overview"},
    {"id": "event_drop_2026-03-24", "type": "event_drop", "metric": "event_10036_rate",
     "start": "2026-03-24", "end": "2026-03-26", "magnitude": 1.0, "event_id": "10036"},
]


def _dates(table) -> np.ndarray:
    return table.column("process_date").to_numpy(zero_copy_only=False).astype("datetime64[D]")


def _window_mask(dates, start, end) -> np.ndarray:
    return (dates >= np.datetime64(start)) & (dates <= np.datetime64(end))


def _fresh_hitids(n, rng):
    """Distinct hit ids for appended rows so they don't collide with baseline rows."""
    hi = (9 * 10**14 + rng.integers(0, 10**14, size=n)).astype(str)
    lo = (8 * 10**14 + rng.integers(0, 10**14, size=n)).astype(str)
    return hi, lo


def _set_column(table, name, values) -> pa.Table:
    idx = table.schema.get_field_index(name)
    field = table.schema.field(idx)
    return table.set_column(idx, field, pa.array(values, type=field.type))


def apply_spike_or_shift(table, scen, rng):
    """Append round(window_rows * (m-1)) resampled rows (m>1)."""
    dates = _dates(table)
    mask = _window_mask(dates, scen["start"], scen["end"])
    win_idx = np.nonzero(mask)[0]
    n_win = len(win_idx)
    extra = int(round(n_win * (scen["magnitude"] - 1)))
    take = win_idx[rng.integers(0, n_win, size=extra)]
    add = table.take(pa.array(take))
    hi, lo = _fresh_hitids(extra, rng)
    add = _set_column(_set_column(add, "hitid_high", hi), "hitid_low", lo)
    out = pa.concat_tables([table, add])
    return out, {"rows_added": extra, "rows_dropped": 0, "rows_modified": 0}


def apply_dip(table, scen, rng):
    dates = _dates(table)
    mask = _window_mask(dates, scen["start"], scen["end"])
    drop = mask & (rng.random(table.num_rows) >= scen["magnitude"])
    out = table.filter(pa.array(~drop))
    return out, {"rows_added": 0, "rows_dropped": int(drop.sum()), "rows_modified": 0}


def apply_dim_mix(table, scen, rng):
    dates = _dates(table)
    mask = _window_mask(dates, scen["start"], scen["end"])
    col = table.column(scen["dim"]).to_numpy(zero_copy_only=False).astype(object)
    win_idx = np.nonzero(mask)[0]
    n_force = int(round(len(win_idx) * scen["magnitude"]))
    force = win_idx[rng.choice(len(win_idx), size=n_force, replace=False)]
    col[force] = scen["value"]
    out = _set_column(table, scen["dim"], col.astype(str))
    return out, {"rows_added": 0, "rows_dropped": 0, "rows_modified": n_force}


def apply_event_drop(table, scen, rng):
    dates = _dates(table)
    mask = _window_mask(dates, scen["start"], scen["end"])
    ev = scen["event_id"]
    col = table.column("post_event_list").to_numpy(zero_copy_only=False).astype(object)
    modified = 0
    for i in np.nonzero(mask)[0]:
        parts = col[i].split(",") if col[i] else []
        if ev in parts:
            col[i] = ",".join(p for p in parts if p != ev)
            modified += 1
    out = _set_column(table, "post_event_list", col.astype(str))
    return out, {"rows_added": 0, "rows_dropped": 0, "rows_modified": modified}


DISPATCH = {
    "spike": apply_spike_or_shift,
    "level_shift": apply_spike_or_shift,
    "dip": apply_dip,
    "dim_mix_shift": apply_dim_mix,
    "event_drop": apply_event_drop,
}


def _realized(scen, before, after) -> dict:
    """Measure the actual effect in the window so recall can be scored against truth."""
    db, da = _dates(before), _dates(after)
    wb = _window_mask(db, scen["start"], scen["end"])
    wa = _window_mask(da, scen["start"], scen["end"])
    if scen["metric"] == "hits":
        b, a = int(wb.sum()), int(wa.sum())
        return {"window_hits_before": b, "window_hits_after": a,
                "realized_ratio": round(a / b, 4) if b else None}
    if scen["metric"] == "post_pagename_share":
        cb = before.column(scen["dim"]).to_numpy(zero_copy_only=False)[wb]
        ca = after.column(scen["dim"]).to_numpy(zero_copy_only=False)[wa]
        sb = float((cb == scen["value"]).mean())
        sa = float((ca == scen["value"]).mean())
        return {"share_before": round(sb, 4), "share_after": round(sa, 4),
                "realized_delta_pp": round((sa - sb) * 100, 2)}
    if scen["metric"].startswith("event_"):
        ev = scen["event_id"]
        cb = before.column("post_event_list").to_numpy(zero_copy_only=False)[wb]
        ca = after.column("post_event_list").to_numpy(zero_copy_only=False)[wa]
        rb = float(np.mean([ev in (s.split(",") if s else []) for s in cb]))
        ra = float(np.mean([ev in (s.split(",") if s else []) for s in ca]))
        return {"rate_before": round(rb, 4), "rate_after": round(ra, 4)}
    return {}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--dir", type=Path, default=DEFAULT_DIR)
    args = ap.parse_args(argv)

    clean = pq.read_table(args.dir / "clean.parquet")
    print(f"[inject] baseline {clean.num_rows:,} rows")
    table = clean
    manifest = []
    for i, scen in enumerate(DEFAULT_SCENARIOS):
        rng = np.random.default_rng([args.seed, i])
        before = table
        table, counts = DISPATCH[scen["type"]](table, scen, rng)
        entry = {
            "anomaly_id": scen["id"], "type": scen["type"], "metric": scen["metric"],
            "scope": {"dim": scen.get("dim"), "value": scen.get("value"),
                      "event_id": scen.get("event_id")},
            "date_start": scen["start"], "date_end": scen["end"],
            "magnitude": scen["magnitude"], "seed": args.seed,
            **counts, "realized": _realized(scen, before, table),
        }
        manifest.append(entry)
        print(f"[inject] {scen['id']:<24} {scen['type']:<14} "
              f"+{counts['rows_added']} -{counts['rows_dropped']} ~{counts['rows_modified']}")

    pq.write_table(table.cast(clean.schema), args.dir / "injected.parquet")
    (args.dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[inject] wrote injected.parquet ({table.num_rows:,} rows) and manifest.json "
          f"({len(manifest)} anomalies)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
