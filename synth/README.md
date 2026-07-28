# Synthetic data generator + anomaly injection (GMAI-Pulse)

Privacy-safe local replica of the GWAM Canada-Retirement Adobe hit table, built **only**
from the EDA output (`synth/gwam_databricks_eda_output.md`). Real data never leaves Databricks;
this dataset lets us prototype anomaly detection and score recall by anomaly type locally.

Every value is synthetic. Sensitive columns are shape-only fakes; masked evar/prop values
reuse the already-anonymized `<masked:...>` tokens from the EDA. No real PII, user agents,
postal codes, or URLs are ever emitted.

## Pipeline

```
synth/gwam_databricks_eda_output.md
        │  extract_spec.py        (parse SHAREABLE blocks -> synth/spec/*.json)
        ▼
synth/spec/*.json
        │  spec.py                (load_spec: clamp artifacts, build samplers)
        ▼
generate.py  ──► data/synth/clean.parquet      (1,142,361 rows, 120 columns)
        │                          known_events.json  (real anomalies in the curve)
        ▼
inject.py    ──► data/synth/injected.parquet   (clean + labeled anomalies)
                                   manifest.json      (ground truth per anomaly)

verify.py    ──► fidelity report (8 checks) on clean or injected parquet
```

## Run

```bash
python synth/extract_spec.py                 # one-time: refresh synth/spec/*.json
python synth/generate.py --seed 42           # ~90s -> data/synth/clean.parquet
python synth/verify.py                        # fidelity report (expect 8 pass)
python synth/inject.py --seed 7              # -> injected.parquet + manifest.json
pytest -q                                     # loader + fidelity + reproducibility tests
```

`generate.py --limit-days N` produces only the first N days (fast dev subset). Output is
fully seeded: the same `--seed` reproduces byte-identical data.

## What is reproduced (fidelity, all verified by `verify.py`)

- Exact per-day hit curve (from `ts_daily`, 2026-02-01 .. 2026-07-06, 156 days).
- Day-of-week and hour-of-day shape (`dow_index`, `hour_matrix`).
- Per-column population rate and the 120-column schema (dtype, cardinality).
- Dimension value distributions + cardinality (top values + synthetic long tail).
- `post_event_list` per-event firing rates and per-hit count (p50=16, p95=18, max≤22).
- Visitor/visit mechanics: ~51k distinct `mcvisid`, 1.36 hits/visit, 0% duplicate keys
  on `(post_visid_high, post_visid_low, visit_num, visit_page_num)`.
- Clock skew: `hit_time_gmt - local` percentiles `[-18000, -14400, -14400]` (EST→EDT).

## Injected anomalies (`manifest.json`)

| type | metric | how it is realized | magnitude meaning |
|------|--------|--------------------|-------------------|
| `spike` | hits | append resampled rows on one day | volume × m |
| `dip` | hits | drop rows over a window | volume × m (m<1) |
| `level_shift` | hits | sustained resample over ≥14 days | volume × m |
| `dim_mix_shift` | `<dim>` share | force a fraction of rows to one value | fraction of window rows forced |
| `event_drop` | event rate | strip one event id from `post_event_list` | fraction stripped |

Each manifest entry carries the window, requested `magnitude`, measured `realized` effect,
and row-count deltas. Default windows avoid the real anomalies in `known_events.json`
(the 2026-07-06 spike and the level-shift dates) so injected recall can be scored cleanly.

## Non-goals

No yearly seasonality (only 5 months of data); event ids stay opaque (the EDA could not
resolve them); no cross-dimension correlation; only the 120 populated schema columns (not
the full 1,198-column physical table); no detector code (separate workstream). The 9 S9
dimensions not in the kept schema (`pagename`, `page_url`, `referrer`, `ref_domain`,
`geo_country/region/city`, `os`, `new_visit`) are profiled in the EDA but not emitted.
