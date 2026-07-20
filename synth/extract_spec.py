"""Extract SHAREABLE JSON blocks from the EDA output markdown into synth/spec/*.json.

One-time (idempotent) step. The EDA notebook prints each machine-readable section
between ``===== BEGIN SHAREABLE: <name> =====`` / ``===== END SHAREABLE: <name> =====``
markers. We pull out the blocks the synthetic-data generator consumes and write them
verbatim as pretty-printed JSON so they are diffable and inspectable.

Run:  python synth/extract_spec.py
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = REPO_ROOT / "synth" / "gwam_databricks_eda_output.md"
SPEC_DIR = REPO_ROOT / "synth" / "spec"

# Blocks the generator/verifier need. synthesis_spec is the master; the ts_* blocks
# carry the exact daily curve and hour-of-day profile the master only summarizes.
WANTED = ["synthesis_spec", "ts_daily", "ts_events", "ts_profiles"]

BLOCK_RE = re.compile(
    r"===== BEGIN SHAREABLE: (?P<name>\w+) =====\n"
    r"(?P<body>.*?)\n"
    r"===== END SHAREABLE: (?P=name) =====",
    re.DOTALL,
)

# Contract checks so a malformed re-run fails loudly instead of silently generating garbage.
SYNTH_KEYS = {"meta", "volume", "series_ref", "schema", "events", "dims", "dq", "identity"}
EXPECTED_SCHEMA_COLS = 120
EXPECTED_TS_DAILY_ROWS = 157


def parse_blocks(text: str) -> dict[str, dict]:
    blocks: dict[str, dict] = {}
    for m in BLOCK_RE.finditer(text):
        name = m.group("name")
        if name not in WANTED:
            continue
        try:
            blocks[name] = json.loads(m.group("body").strip())
        except json.JSONDecodeError as e:
            raise SystemExit(f"[extract_spec] block '{name}' is not valid JSON: {e}")
    return blocks


def validate(blocks: dict[str, dict]) -> None:
    missing = [b for b in WANTED if b not in blocks]
    if missing:
        raise SystemExit(f"[extract_spec] missing SHAREABLE blocks: {missing}")

    spec = blocks["synthesis_spec"]
    keys = set(spec)
    if keys != SYNTH_KEYS:
        raise SystemExit(
            f"[extract_spec] synthesis_spec keys mismatch.\n"
            f"  expected: {sorted(SYNTH_KEYS)}\n  got:      {sorted(keys)}"
        )
    n_cols = len(spec["schema"])
    if n_cols != EXPECTED_SCHEMA_COLS:
        raise SystemExit(
            f"[extract_spec] schema has {n_cols} columns, expected {EXPECTED_SCHEMA_COLS}"
        )
    n_days = len(blocks["ts_daily"]["csv"])
    if n_days != EXPECTED_TS_DAILY_ROWS:
        raise SystemExit(
            f"[extract_spec] ts_daily has {n_days} rows, expected {EXPECTED_TS_DAILY_ROWS}"
        )


def main(argv: list[str]) -> int:
    input_path = Path(argv[1]) if len(argv) > 1 else DEFAULT_INPUT
    if not input_path.exists():
        raise SystemExit(f"[extract_spec] input not found: {input_path}")

    text = input_path.read_text(encoding="utf-8")
    blocks = parse_blocks(text)
    validate(blocks)

    SPEC_DIR.mkdir(parents=True, exist_ok=True)
    for name in WANTED:
        out = SPEC_DIR / f"{name}.json"
        out.write_text(
            json.dumps(blocks[name], indent=2, ensure_ascii=False), encoding="utf-8"
        )
        print(f"[extract_spec] wrote {out.relative_to(REPO_ROOT)}")

    spec = blocks["synthesis_spec"]
    print(
        f"[extract_spec] OK  schema={len(spec['schema'])} cols  "
        f"dims={len(spec['dims'])}  events={len(spec['events'].get('event_freq', []))}  "
        f"ts_daily={len(blocks['ts_daily']['csv'])} rows"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
