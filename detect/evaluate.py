"""Score detector output against the injected ground truth (range/event-based only).

Point-adjusted F1 and per-point accuracy are banned (02-solution-architecture.md 5.3)
because they reward trivial always-on flagging. Instead:

  Recall   a scenario is detected iff >=1 flagged day on a relevant metric falls inside
           its [date_start, date_end] window. We report which detectors caught it and the
           detection delay (first flagged day - window start).
  FP rate  flagged metric-days outside every injected window and known-event date, divided
           by the eligible scored metric-days from run_meta_{label}.json, per detector class.

--control scores the clean run: no injected windows, known real events (the 2026-07-06
spike, the level-shift days) count as expected detections and everything else is FP.

    python detect/evaluate.py --label injected --require-recall 5
    python detect/evaluate.py --label clean --control
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from registry import slug  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DETECT_DIR = REPO_ROOT / "data" / "detect"
SYNTH_DIR = REPO_ROOT / "data" / "synth"

VOLUME_METRICS = ["hits_total", "visits_total", "visitors_total", "mv_daily_kpi_vector"]


@dataclass
class Result:
    name: str
    status: str          # PASS | FAIL | INFO
    detail: str

    def __str__(self):
        return f"[{self.status:<4}] {self.name}: {self.detail}"


def _d(s) -> date:
    return date.fromisoformat(str(s)[:10])


def relevant_metrics(scenario: dict) -> list[str]:
    """Metrics whose flags count as detecting this scenario (from scope, not hardcoded)."""
    t = scenario["type"]
    scope = scenario.get("scope", {})
    if t in ("spike", "dip", "level_shift"):
        return list(VOLUME_METRICS)
    if t == "dim_mix_shift":
        return [f"pagename_share_{slug(scope['value'])}", "mv_daily_kpi_vector"]
    if t == "event_drop":
        eid = scope["event_id"]
        # event_coverage rule rows share metric_id event_{eid}_count, so they are included.
        return [f"event_{eid}_count", f"event_{eid}_rate"]
    raise ValueError(f"unknown scenario type: {t}")


def _windows(manifest: list[dict]) -> list[tuple[date, date]]:
    return [(_d(s["date_start"]), _d(s["date_end"])) for s in manifest]


def _in_any_window(d: date, windows) -> bool:
    return any(a <= d <= b for a, b in windows)


def score_recall(anomalies: pd.DataFrame, manifest: list[dict]) -> list[dict]:
    out = []
    for s in manifest:
        metrics = relevant_metrics(s)
        start, end = _d(s["date_start"]), _d(s["date_end"])
        hits = anomalies[anomalies["metric_id"].isin(metrics)].copy()
        if len(hits):
            hits["d"] = hits["date"].map(_d)
            hits = hits[(hits["d"] >= start) & (hits["d"] <= end)]
        detected = len(hits) > 0
        detectors = sorted(hits["detector"].unique().tolist()) if detected else []
        delay = (min(hits["d"]) - start).days if detected else None
        out.append({
            "anomaly_id": s["anomaly_id"], "type": s["type"],
            "window": [str(start), str(end)], "relevant_metrics": metrics,
            "detected": detected, "detectors": detectors,
            "delay_days": delay, "n_flagged_in_window": int(len(hits)),
        })
    return out


def score_fp(anomalies: pd.DataFrame, meta: dict, exclude_windows, exclude_dates) -> dict:
    denom = meta["denominators"]
    out = {}
    for cls, parts in denom.items():
        total = sum(parts.values())
        cls_rows = anomalies[anomalies["class"] == cls]
        fp = 0
        for _, r in cls_rows.iterrows():
            d = _d(r["date"])
            if not _in_any_window(d, exclude_windows) and d not in exclude_dates:
                fp += 1
        out[cls] = {
            "flagged_fp": int(fp), "denominator": int(total),
            "rate": round(fp / total, 5) if total else None,
        }
    return out


def evaluate(label: str, control: bool, detect_dir=DETECT_DIR, synth_dir=SYNTH_DIR):
    anomalies = pd.read_parquet(detect_dir / f"anomalies_{label}.parquet")
    meta = json.loads((detect_dir / f"run_meta_{label}.json").read_text())
    known = json.loads((synth_dir / "known_events.json").read_text())
    known_dates = {_d(known["spike"]["date"])} | {_d(x) for x in known["level_shift_dates"]}

    report = {"label": label, "mode": "control" if control else "injected"}

    if control:
        # Known real events are the expected detections; injected manifest is ignored.
        report["scenarios"] = []
        report["false_positives"] = score_fp(anomalies, meta,
                                              exclude_windows=[], exclude_dates=known_dates)
        vol = anomalies[anomalies["metric_id"].isin(VOLUME_METRICS)]
        flagged_known = sorted({str(d) for d in vol["date"].map(_d) if d in known_dates})
        report["known_events"] = {
            "expected_dates": sorted(str(d) for d in known_dates),
            "flagged_by_volume_metrics": flagged_known,
        }
        results = [Result(f"known_event {d}",
                          "PASS" if d in flagged_known else "INFO",
                          "flagged" if d in flagged_known else "not flagged")
                   for d in sorted(str(x) for x in known_dates)]
    else:
        manifest = json.loads((synth_dir / "manifest.json").read_text())
        scenarios = score_recall(anomalies, manifest)
        report["scenarios"] = scenarios
        report["n_scenarios"] = len(scenarios)
        report["n_detected"] = sum(s["detected"] for s in scenarios)
        report["false_positives"] = score_fp(
            anomalies, meta,
            exclude_windows=_windows(manifest), exclude_dates=known_dates)
        results = []
        for s in scenarios:
            det = (f"detectors={s['detectors']} delay={s['delay_days']}d "
                   f"({s['n_flagged_in_window']} flagged)"
                   if s["detected"] else "MISSED")
            results.append(Result(f"recall {s['anomaly_id']}",
                                  "PASS" if s["detected"] else "FAIL", det))

    for cls, fp in report["false_positives"].items():
        results.append(Result(f"fp_rate {cls}", "INFO",
                              f"{fp['flagged_fp']}/{fp['denominator']} = {fp['rate']}"))
    return report, results


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="injected")
    ap.add_argument("--control", action="store_true", help="score clean run vs known events")
    ap.add_argument("--require-recall", type=int, default=None,
                    help="exit non-zero if fewer scenarios detected (injected mode only)")
    ap.add_argument("--detect-dir", type=Path, default=DETECT_DIR)
    args = ap.parse_args(argv)

    report, results = evaluate(args.label, args.control, detect_dir=args.detect_dir)

    print(f"=== evaluate: {args.label} ({report['mode']}) ===")
    for r in results:
        print(r)
    if not args.control:
        print(f"=== recall {report['n_detected']}/{report['n_scenarios']} scenarios ===")

    (args.detect_dir / f"eval_report_{args.label}.json").write_text(json.dumps(report, indent=2))

    if args.require_recall is not None and not args.control:
        if report["n_detected"] < args.require_recall:
            print(f"FAIL: recall {report['n_detected']} < required {args.require_recall}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
