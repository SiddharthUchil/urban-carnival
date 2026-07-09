"""Pure Spark-column transforms for the silver layer.

Factored out of the silver notebook so they can be unit-tested without a running job.
Requires pyspark; no dbutils.
"""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.column import Column


def event_ts_expr(local_col: str = "date_time", gmt_col: str = "hit_time_gmt") -> Column:
    """Prefer the typed local timestamp; fall back to epoch-GMT (matches EDA ts_expr)."""
    return F.coalesce(
        F.col(local_col).cast("timestamp"),
        F.from_unixtime(F.col(gmt_col).cast("long")).cast("timestamp"),
    )


def normalize_event_list_expr(col: str = "post_event_list") -> Column:
    """Strip Adobe ``=value`` suffixes so tokens are bare event ids (plan D4).

    Real data tokens look like ``10036=1,20=1,...``. detect/kpis.py splits on ',' only and
    would keep the ``=value``, silently mis-counting. Normalizing here fixes it once for both
    the gold KPI build and the pandas detector, leaving detect/ untouched. Empty tokens are
    dropped; the result is a comma-joined string of ids (NULL preserved as NULL).
    """
    return F.when(F.col(col).isNull(), F.lit(None).cast("string")).otherwise(
        F.expr(
            f"array_join("
            f"  filter(transform(split({col}, ','), x -> trim(split(x, '=')[0])),"
            f"         x -> x is not null and x <> ''),"
            f"  ',')"
        )
    )


def pseudonymize_expr(col: str, key: str) -> Column:
    """Deterministic keyed SHA-256 pseudonym (ADR-0007).

    Deterministic, so distinct counts (visits / visitors) are preserved exactly; irreversible
    without the key. NULL-safe. Note this is a keyed hash (secret prepended), not RFC-2104
    HMAC -- Spark SQL has no ``hmac()`` builtin. Swap to a ``hashlib.hmac`` pandas_udf if
    strict HMAC is mandated by governance.
    """
    return F.when(F.col(col).isNull(), F.lit(None).cast("string")).otherwise(
        F.sha2(F.concat(F.lit(key), F.col(col).cast("string")), 256)
    )
