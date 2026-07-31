"""Cut the code-only subset for github.com/JHDevOps/GMAI-PULSE-DATABRICKS.

Copies the runtime-necessary tree (55 git-tracked files) into a destination directory,
leaving behind the research docs, EDA notebooks, HTML run exports, the frontend concept
and the ~4 MB of binary artifacts.

    python scripts/cut_corporate_tree.py ../GMAI-PULSE-DATABRICKS

Then, in the destination: git init && git add -A && git commit && git remote add ... && push.

WHY THIS EXISTS RATHER THAN A HAND-PICKED COPY: research/claude/metric-registry.yaml is a
runtime dependency that does not look like one. detect/cm_registry.py pins
REGISTRY_VERSION and tests/test_registry_yaml.py reads that file to enforce the pin, so a
"code only" tree that drops all of research/ silently loses 20-odd tests. Everything else
in research/ is genuinely prose and stays behind.

Verified 2026-07-31: the resulting tree runs 110 passed / 4 skipped standalone. The 4 skips
are the fixtures that need generated data (data/synth/*.parquet, gitignored) -- regenerate
with `python -m synth.generate` if you want them, they are not a packaging failure.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

INCLUDE_DIRS = ("databricks/", "detect/", "tests/", "synth/")
INCLUDE_FILES = ("requirements.txt", ".gitignore", "research/claude/metric-registry.yaml")


def main(dest: Path) -> int:
    repo = Path(__file__).resolve().parent.parent
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.split()

    keep = [f for f in tracked
            if (f.startswith(INCLUDE_DIRS) or f in INCLUDE_FILES)
            and "__pycache__" not in f]

    if dest.exists() and any(dest.iterdir()):
        print(f"refusing to write into non-empty {dest}", file=sys.stderr)
        return 1

    for f in keep:
        out = dest / f
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repo / f, out)

    by_top: dict[str, int] = {}
    for f in keep:
        by_top[f.split("/")[0]] = by_top.get(f.split("/")[0], 0) + 1
    print(f"copied {len(keep)} files -> {dest}")
    for k in sorted(by_top):
        print(f"  {k:16s} {by_top[k]:3d}")
    print("\nnext: cd into it, git init, commit, and push to JHDevOps/GMAI-PULSE-DATABRICKS")
    print("then verify with: python -m pytest tests/ -q   (expect 110 passed, 4 skipped)")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(Path(sys.argv[1]).resolve()))
