"""Import-order and OpenMP guard for the darts/pyod stack on Windows.

Importing `darts` before the compiled numba/xgboost/sklearn extensions have loaded
segfaults on this platform (duplicate OpenMP runtime, and darts's internal native-import
order). Importing this module first pre-loads that stack in a known-safe order and sets
the duplicate-lib guard, so `import darts` / `import pyod` afterwards is stable. Threads
are pinned to 1 for reproducible scores on the small daily series.

Any module that imports darts or pyod must `import _bootstrap` before those imports.
"""
import os

os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy  # noqa: F401,E402
import scipy  # noqa: F401,E402
import sklearn  # noqa: F401,E402
import numba  # noqa: F401,E402
import xgboost  # noqa: F401,E402
