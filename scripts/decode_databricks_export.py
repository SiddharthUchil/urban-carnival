#!/usr/bin/env python3
"""Decode a Databricks notebook export and verify its SHAREABLE blocks.

Every EDA notebook in `eda/` prints its results as

    ===== BEGIN SHAREABLE: <section_id> =====
    {"...": "compact json"}
    ===== END SHAREABLE: <section_id> =====

and ends with a `run_manifest` block carrying the byte length + sha1 of each
payload. Until now that export was decoded by hand on every run, which is how
doc-17 defect E1 (a silently SKIPPED section) went unnoticed for days.

This does it mechanically. Stdlib only -- `nbformat` is not installed, and the
committed `.ipynb` exports are too large for line-oriented tools anyway.

Usage
-----
    python scripts/decode_databricks_export.py gwam_channel_discovery.html
    python scripts/decode_databricks_export.py export.html --expect-sections 12
    python scripts/decode_databricks_export.py export.html --grep http \
        --grep-sections cid_vs_campaign
    python scripts/decode_databricks_export.py export.html --dump out/

Exit code is 0 only when every check passes, so this is safe in a gate.

Why the checks are what they are
--------------------------------
* `skipped` must be `{}` -- a section that throws prints a SKIPPED line and the
  run *continues*. A partial run otherwise looks complete (doc-17 E1).
* Block count is always `n_sections + 1`: `run_manifest` counts `RESULTS` before
  emitting itself. Asserting equality reads as a vanished section.
* Payloads are re-hashed against the manifest because Databricks silently
  truncates large stdout mid-JSON -- it dropped 22 of 200 rollup rows on the
  2026-07-20 inventory run (doc-16 §0.5).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import pathlib
import re
import sys
import urllib.parse

MODEL_RE = re.compile(r"__DATABRICKS_NOTEBOOK_MODEL\s*=\s*'([^']+)'")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
BEGIN_RE = re.compile(r"^===== BEGIN SHAREABLE: (\S+) =====$")
END_RE = re.compile(r"^===== END SHAREABLE: (\S+) =====$")
PART_RE = re.compile(r"^----- part (\d+) of (\d+).*-----$")
SKIPPED_RE = re.compile(r"^===== SKIPPED: (\S+) \| (.*?) =====$")
TRUNC_MARKER = "max output size exceeded"


# ---------------------------------------------------------------- extraction

def _string_leaves(node, out):
    """Collect every string leaf. Databricks nests cell output differently across
    DBR versions (results.data, results.data[].text, arguments...), so walking
    for strings is more durable than pinning one path."""
    if isinstance(node, str):
        out.append(node)
    elif isinstance(node, dict):
        for value in node.values():
            _string_leaves(value, out)
    elif isinstance(node, list):
        for value in node:
            _string_leaves(value, out)


def read_html(path: pathlib.Path):
    """Databricks HTML export -> (stdout text, widget values).

    The whole notebook lives in one base64 line, so never read these with
    line-oriented shell tools.
    """
    raw = path.read_text(encoding="utf-8", errors="replace")
    match = MODEL_RE.search(raw)
    if not match:
        raise SystemExit(
            f"{path.name}: no __DATABRICKS_NOTEBOOK_MODEL blob found. "
            "Is this actually a Databricks HTML export?"
        )
    decoded = base64.b64decode(match.group(1)).decode("utf-8", errors="replace")
    model = json.loads(urllib.parse.unquote(decoded))

    chunks: list[str] = []
    for command in model.get("commands") or []:
        _string_leaves(command.get("results"), chunks)

    widgets = {}
    for name, spec in (model.get("inputWidgets") or {}).items():
        widgets[name] = spec.get("currentValue") if isinstance(spec, dict) else spec
    return "\n".join(chunks), widgets


def read_ipynb(path: pathlib.Path):
    """nbformat JSON -> (stdout text, widget values). No nbformat dependency."""
    nb = json.loads(path.read_text(encoding="utf-8", errors="replace"))

    chunks: list[str] = []
    for cell in nb.get("cells") or []:
        for output in cell.get("outputs") or []:
            text = output.get("text")
            if text is not None:
                chunks.append(text if isinstance(text, str) else "".join(text))
            data = (output.get("data") or {}).get("text/plain")
            if data is not None:
                chunks.append(data if isinstance(data, str) else "".join(data))

    dbx = (nb.get("metadata") or {}).get("application/vnd.databricks.v1+notebook") or {}
    widgets = {}
    for name, spec in (dbx.get("widgets") or {}).items():
        if isinstance(spec, dict):
            state = spec.get("state") or spec
            widgets[name] = state.get("currentValue", state.get("value"))
        else:
            widgets[name] = spec
    return "\n".join(chunks), widgets


def parse_blocks(text: str):
    """Scan stdout for SHAREABLE blocks, SKIPPED lines and truncation markers.

    Returns (sections, order, skipped_lines, duplicates, truncations).

    Multi-part payloads (`----- part i of N -----`) are concatenated with
    **nothing** between the parts, matching the notebook's own contract
    ("concatenate parts to reassemble"). Each part is one `print()` of a
    40,000-char slice, so it arrives as its own stdout line; joining those with
    a newline injects one spurious byte per boundary and both the sha1 and the
    JSON parse fail at the first part boundary.

    Duplicate ids resolve **last-wins** to mirror `RESULTS[id] = payload`. The
    probe's `manulifeid_split` emits from two code paths (an early return and
    the full path); only one fires per run, but first-wins here would re-hash
    the wrong body and report a phantom sha1 mismatch.
    """
    sections: dict[str, str] = {}
    order: list[str] = []
    skipped_lines: list[tuple[str, str]] = []
    duplicates: list[str] = []
    truncations = 0

    current: str | None = None
    body: list[str] = []
    saw_parts = False

    for line in ANSI_RE.sub("", text).splitlines():
        stripped = line.strip()

        if TRUNC_MARKER in stripped:
            truncations += 1
            if current is not None:
                body.append(line)
            continue

        skipped = SKIPPED_RE.match(stripped)
        if skipped and current is None:
            skipped_lines.append((skipped.group(1), skipped.group(2)))
            continue

        begin = BEGIN_RE.match(stripped)
        if begin and current is None:
            current, body, saw_parts = begin.group(1), [], False
            continue

        if current is not None:
            end = END_RE.match(stripped)
            if end and end.group(1) == current:
                if current in sections:
                    duplicates.append(current)
                else:
                    order.append(current)
                sections[current] = ("" if saw_parts else "\n").join(body)  # last-wins
                current = None
                continue
            if PART_RE.match(stripped):
                saw_parts = True
                continue  # separator, not payload
            body.append(line)

    if current is not None:
        # An unterminated block means the run died or stdout was cut mid-payload.
        sections[current] = ("" if saw_parts else "\n").join(body)
        if current not in order:
            order.append(current)
        print(f"  !! block '{current}' never closed -- output truncated or run aborted",
              file=sys.stderr)

    return sections, order, skipped_lines, duplicates, truncations


def norm_sha1(value) -> str:
    """Reduce a manifest sha1 to bare lowercase hex.

    Pre-D2 notebooks wrote the digest grouped with hyphens (`7d13be1a-9ea8-...`),
    so a literal string compare reports a mismatch against an identical hash.
    Byte lengths agreeing while the digest 'differs' is the tell.
    """
    return re.sub(r"[^0-9a-f]", "", str(value or "").lower())


def find_manifest(payloads: dict, order: list):
    """Locate the integrity manifest, whatever this notebook family calls it.

    Three conventions ship: `run_manifest` (EDA + discovery probes),
    `chart:manifest` (charts notebooks, via `share()`), and a numbered
    `s8_run_manifest` (the retired URL-scope inventory). Match the known names
    first, then fall back to any id ending in "manifest".
    """
    for candidate in ("run_manifest", "chart:manifest"):
        if isinstance(payloads.get(candidate), dict):
            return candidate, payloads[candidate]
    for sid in reversed(order):  # the manifest is emitted last
        if sid.endswith("manifest") and isinstance(payloads.get(sid), dict):
            return sid, payloads[sid]
    return None, None


def manifest_count(manifest: dict):
    """The manifest's own section/chart total, under either key name."""
    for key in ("n_sections", "n_charts"):
        if isinstance(manifest.get(key), int):
            return manifest[key]
    return None


def manifest_sections(manifest: dict):
    """Normalise the manifest's section map to {id: {bytes?, sha1?}}.

    The map lives under `sections` (EDA/probe), `charts` (charts notebooks) or
    `sections_emitted` (the retired URL-scope inventory).

    Three shapes ship in practice, so accept all of them rather than breaking on
    whichever export you happen to have:

    * ``{"id": {"bytes": n, "sha1": "..."}}`` -- current notebooks; full integrity.
    * ``[{"id": ..., "bytes": n, "sha1": "..."}, ...]`` -- list of entries.
    * ``["id", "id", ...]`` -- older manifests recorded ids only. The committed
      `gwam_channel_discovery.html` is this shape. Ids can still be checked for
      presence, but there is nothing to re-hash against, and the caller must say
      so instead of reporting a vacuous pass.
    """
    raw = manifest.get("sections")
    for alt in ("charts", "sections_emitted"):
        if raw is None:
            raw = manifest.get(alt)
    if isinstance(raw, dict):
        return {str(k): (v if isinstance(v, dict) else {}) for k, v in raw.items()}
    if isinstance(raw, list):
        out = {}
        for i, entry in enumerate(raw):
            if isinstance(entry, str):
                out[entry] = {}
                continue
            if not isinstance(entry, dict):
                continue
            key = entry.get("id") or entry.get("section") or entry.get("name")
            out[str(key) if key else f"<unnamed #{i}>"] = {
                k: v for k, v in entry.items() if k in ("bytes", "sha1")}
        return out
    return {}


# ------------------------------------------------------------------- reporting

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Decode and verify a Databricks notebook export's SHAREABLE blocks.")
    ap.add_argument("export", type=pathlib.Path, help="path to the .html or .ipynb export")
    ap.add_argument("--expect-sections", type=int, metavar="N",
                    help="assert run_manifest.n_sections == N (block count is N+1)")
    ap.add_argument("--expect-blocks", type=int, metavar="N",
                    help="assert exactly N BEGIN SHAREABLE blocks were found")
    ap.add_argument("--grep", action="append", default=[], metavar="PATTERN",
                    help="fail if this regex appears in a payload (privacy check; repeatable)")
    ap.add_argument("--grep-sections", metavar="IDS",
                    help="restrict --grep to these comma-separated section ids (default: all)")
    ap.add_argument("--allow-missing-manifest", action="store_true",
                    help="do not fail when the export has no manifest block (the retired "
                         "URL-scope inventory predates the convention)")
    ap.add_argument("--dump", type=pathlib.Path, metavar="DIR",
                    help="write each section payload to DIR/<id>.json")
    ap.add_argument("--quiet", action="store_true", help="only print problems and the verdict")
    args = ap.parse_args(argv)

    if not args.export.is_file():
        print(f"error: no such file: {args.export}", file=sys.stderr)
        return 2

    suffix = args.export.suffix.lower()
    if suffix == ".html":
        text, widgets = read_html(args.export)
    elif suffix == ".ipynb":
        text, widgets = read_ipynb(args.export)
    else:
        print(f"error: expected .html or .ipynb, got '{suffix}'", file=sys.stderr)
        return 2

    sections, order, skipped_lines, duplicates, truncations = parse_blocks(text)
    failures: list[str] = []
    verified_integrity = False  # True only once payloads are actually re-hashed

    print(f"== {args.export.name}")
    print(f"   blocks found : {len(order)}")
    if not args.quiet and order:
        print(f"   ids          : {', '.join(order)}")
    if widgets and not args.quiet:
        print("   widgets used :")
        for name in sorted(widgets):
            print(f"     {name} = {widgets[name]}")

    if not order:
        failures.append("no SHAREABLE blocks found at all")

    if duplicates:
        # Not necessarily wrong -- mirrors RESULTS last-wins -- but say so.
        print(f"   note         : duplicate ids resolved last-wins: {', '.join(sorted(set(duplicates)))}")

    if truncations:
        failures.append(f"{truncations} stdout truncation marker(s) "
                        f"('{TRUNC_MARKER}') -- payloads are incomplete")

    if skipped_lines:
        for sid, reason in skipped_lines:
            print(f"   SKIPPED      : {sid} | {reason}")
        failures.append(f"{len(skipped_lines)} section(s) printed a SKIPPED line")

    # ---- every payload must be valid JSON
    payloads: dict[str, object] = {}
    for sid in order:
        try:
            payloads[sid] = json.loads(sections[sid])
        except json.JSONDecodeError as exc:
            failures.append(f"section '{sid}' is not valid JSON ({exc})")

    # ---- manifest cross-check
    manifest_id, manifest = find_manifest(payloads, order)
    if manifest is None:
        message = ("no parseable manifest block (looked for run_manifest, chart:manifest, "
                   "*manifest) -- payload integrity cannot be verified")
        if args.allow_missing_manifest:
            print(f"   manifest     : ABSENT -- {message} (allowed by --allow-missing-manifest)")
        else:
            failures.append(message)
    else:
        n_sections = manifest_count(manifest)
        skipped_map = manifest.get("skipped")
        if skipped_map is None:
            skipped_map = manifest.get("sections_skipped")
        complete = manifest.get("complete")
        label = f"manifest '{manifest_id}'" if manifest_id != "run_manifest" else "manifest"
        print(f"   {label:<13}: n_sections={n_sections} "
              f"complete={complete if complete is not None else 'n/a'} "
              f"skipped={skipped_map if skipped_map else '{}'}")
        for label in ("notebook", "generated_at", "window", "table"):
            if label in manifest and not args.quiet:
                print(f"   {label:<13}: {manifest[label]}")

        if skipped_map:
            failures.append(f"run_manifest.skipped is not empty: {skipped_map}")
        if complete is False:
            failures.append("run_manifest.complete is false")

        # run_manifest counts RESULTS before adding itself, so blocks == n+1.
        if isinstance(n_sections, int) and n_sections + 1 != len(order):
            failures.append(f"block count {len(order)} != n_sections+1 "
                            f"({n_sections}+1) -- a block is missing or unclosed")

        if args.expect_sections is not None and n_sections != args.expect_sections:
            failures.append(f"expected n_sections={args.expect_sections}, got {n_sections}")

        # ---- re-hash each payload against the manifest
        declared = manifest_sections(manifest)
        if not declared:
            failures.append(f"manifest '{manifest_id}' carries no sections/charts map "
                            "-- nothing to verify against")
        hashed = 0          # payloads actually re-hashed and matched
        present = 0         # declared ids found in the export
        no_integrity = []   # declared ids with no bytes/sha1 recorded
        for sid, meta in declared.items():
            # Charts notebooks build `_manifest[cid]` from the bare id but emit
            # via share() as `chart:<cid>`, so resolve a namespaced block too.
            body = sections.get(sid)
            if body is None:
                for prefixed in (f"chart:{sid}",):
                    if prefixed in sections:
                        body = sections[prefixed]
                        break
            if body is None:
                failures.append(f"manifest lists '{sid}' but no such block is in the export")
                continue
            present += 1
            want_bytes, want_sha1 = meta.get("bytes"), meta.get("sha1")
            if want_sha1 is None and want_bytes is None:
                no_integrity.append(sid)
                continue
            actual_bytes = len(body)
            actual_sha1 = hashlib.sha1(body.encode("utf-8")).hexdigest()
            if want_sha1 and actual_sha1 != norm_sha1(want_sha1):
                failures.append(
                    f"sha1 mismatch on '{sid}': export {actual_sha1[:12]}… "
                    f"vs manifest {norm_sha1(want_sha1)[:12]}… "
                    f"({actual_bytes} vs {want_bytes} bytes) -- payload was altered or truncated")
            elif want_bytes is not None and actual_bytes != want_bytes:
                failures.append(f"byte-length mismatch on '{sid}': "
                                f"{actual_bytes} vs manifest {want_bytes}")
            else:
                hashed += 1

        # run_manifest itself is never listed in its own section map.
        print(f"   ids declared : {present}/{len(declared)} present in the export")
        if no_integrity:
            verified_integrity = False
            print(f"   integrity    : UNAVAILABLE -- this manifest records ids only, no "
                  f"bytes/sha1 ({len(no_integrity)} section(s)). Presence is checked; "
                  f"truncation within a payload is NOT detectable.")
        else:
            verified_integrity = bool(declared)
            print(f"   integrity    : {hashed}/{len(declared)} payloads match manifest bytes+sha1")

    if args.expect_blocks is not None and len(order) != args.expect_blocks:
        failures.append(f"expected {args.expect_blocks} blocks, found {len(order)}")

    # ---- privacy / content grep
    if args.grep:
        targets = order
        if args.grep_sections:
            wanted = {s.strip() for s in args.grep_sections.split(",") if s.strip()}
            missing = wanted - set(order)
            if missing:
                failures.append(f"--grep-sections named absent section(s): {', '.join(sorted(missing))}")
            targets = [s for s in order if s in wanted]
        for pattern in args.grep:
            rx = re.compile(pattern, re.I)
            hits = [sid for sid in targets if rx.search(sections[sid])]
            if hits:
                failures.append(f"grep '{pattern}' matched in: {', '.join(hits)}")
            else:
                print(f"   grep         : '{pattern}' clean across "
                      f"{len(targets)} section(s)")

    # ---- dump
    if args.dump:
        args.dump.mkdir(parents=True, exist_ok=True)
        for sid in order:
            target = args.dump / f"{sid}.json"
            payload = payloads.get(sid)
            if payload is not None:
                target.write_text(json.dumps(payload, indent=2, sort_keys=True),
                                  encoding="utf-8")
            else:  # unparseable -- keep the raw body so it can be inspected
                target.with_suffix(".raw.txt").write_text(sections[sid], encoding="utf-8")
        print(f"   dumped       : {len(order)} payload(s) -> {args.dump}")

    print()
    if failures:
        print(f"FAIL ({len(failures)} problem(s)):")
        for problem in failures:
            print(f"  - {problem}")
        return 1
    if verified_integrity:
        print("PASS -- run is complete and every payload matches the manifest bytes+sha1.")
    else:
        # Do not claim an integrity check that could not run.
        print("PASS (with caveat) -- run is complete and all declared sections are present, "
              "but this manifest carries no bytes/sha1, so payload integrity was NOT verified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
