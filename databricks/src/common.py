"""Shared helpers for the GMAI-Pulse Databricks notebooks.

Handles sys.path setup (so `conf.*`, the sibling libs, and the local `detect/` package all
import), watermark reads, the source schema contract, and the freshness gate. Import-safe:
no dbutils / spark referenced at import time.
"""
from __future__ import annotations

import os
import sys


def _notebook_path(dbutils):
    try:
        return (dbutils.notebook.entry_point.getDbutils()
                .notebook().getContext().notebookPath().get())
    except Exception:
        return None


def resolve_repo_root(dbutils, override=None):
    """Filesystem path of the repo root.

    Prefers an explicit override (the `repo_root` job parameter). Otherwise derives it from
    the running notebook's workspace path: notebooks live at
    ``<repo>/databricks/src/<name>`` and workspace files are readable under ``/Workspace``.
    """
    if override:
        return override
    nb = _notebook_path(dbutils)
    if nb and "/databricks/" in nb:
        ws_repo = nb.split("/databricks/")[0]   # e.g. /Repos/me/anomoly-detection
        return "/Workspace" + ws_repo
    return os.getcwd()


def setup_paths(dbutils, override=None):
    """Put repo_root, repo_root/detect, repo_root/databricks and .../databricks/src on
    sys.path so ``import conf.settings``, ``import gold_lib`` and
    ``from run import run_detection`` all resolve. Returns repo_root."""
    root = resolve_repo_root(dbutils, override)
    for p in (root,
              os.path.join(root, "detect"),
              os.path.join(root, "databricks"),
              os.path.join(root, "databricks", "src")):
        if p and p not in sys.path:
            sys.path.insert(0, p)
    return root


def read_watermark(spark, table, col="process_date"):
    """max(col) as a 'YYYY-MM-DD' string, or None if the table is absent / empty."""
    if not spark.catalog.tableExists(table):
        return None
    row = spark.table(table).agg({col: "max"}).first()
    v = row[0] if row is not None else None
    return None if v is None else str(v)[:10]


def assert_source_columns(available, required):
    """Fail fast on upstream schema drift (ADR-0006 consequence: schema contract in ingest)."""
    missing = [c for c in required if c not in set(available)]
    if missing:
        raise ValueError(
            f"Source schema contract violation (ADR-0006): missing columns {missing}. "
            "The upstream Adobe feed changed -- reconcile databricks/conf/bronze_columns.py "
            "before ingesting."
        )


def set_task_value(dbutils, key, value):
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass  # not running inside a job task (e.g. interactive) -- no-op


def gate(dbutils, guard_task="freshness_guard"):
    """True if the freshness guard flagged new data (or if not running under a job).

    Downstream notebooks call this first and ``dbutils.notebook.exit`` when it is False, so
    the linear DAG needs no separate condition task.
    """
    try:
        v = dbutils.jobs.taskValues.get(taskKey=guard_task, key="new_data",
                                        default="true", debugValue="true")
    except Exception:
        return True
    return str(v).lower() == "true"
