# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — GWAM Canada Retirement · Interactive Charts
# MAGIC
# MAGIC A **modern, dark-theme, interactive** companion to the profiling notebook
# MAGIC (`gwam_canada_retirement_eda.py`). Where the EDA notebook emits machine-readable JSON,
# MAGIC this notebook renders the same meaningful observations as **Plotly** charts you can
# MAGIC hover, zoom, and filter — built for a **global team**.
# MAGIC
# MAGIC ### What makes it global-team friendly
# MAGIC - **Time-zone selector** — every time-of-day chart is recomputed in the reviewer's own
# MAGIC   timezone (hits are converted from `hit_time_gmt` epoch-GMT), so a viewer in Toronto,
# MAGIC   London, Hong Kong, or Manila sees activity aligned to *their* clock.
# MAGIC - **Geography selector** — filter to a country / region, or view the world choropleth and
# MAGIC   province bars, to see where CA-Retirement traffic originates (CAN ~85 %, USA ~11 %).
# MAGIC - **Date-range & granularity** — daily or weekly rollups over any sub-window.
# MAGIC
# MAGIC ### Privacy (ADR-0007)
# MAGIC Every panel is an **aggregate** (counts / rates by time or by an allow-listed dimension).
# MAGIC No visitor IDs, IPs, cookies, user-agents, or fine geo are ever read or displayed.
# MAGIC
# MAGIC ### Scope — both report suites
# MAGIC `rsid` IN (`manugrs`, `manulifeglobalprod`) AND a URL matching the `url_scope_mode`
# MAGIC include list — default `broad`: `%/group-retirement%`, `%/group-plans%`,
# MAGIC `%/regimes-collectifs%`. Those patterns are language- AND domain-agnostic, so one list
# MAGIC covers both suites (`manulifeim.com`, `manulife.com`) in EN and FR. Same contract as the
# MAGIC EDA notebook.
# MAGIC
# MAGIC Two exclusions: `url_scope_exclude` (AEM authoring/staging, non-CA `/ph/`) and
# MAGIC `login_host_exclude` — the six D8 member-auth hosts, dropped in every mode. That is an
# MAGIC explicit host list, NOT a `%portal%` pattern: four of the six carry no "portal"
# MAGIC substring (`id.manulife.ca` alone is 62.6M hits) and the FR spelling is "portail".
# MAGIC URL matching uses the D4 blank-guarded `coalesce(page_url, post_page_url)`.
# MAGIC
# MAGIC Panels aggregate across both suites; `scope.rsid_breakdown` reports per-suite row
# MAGIC counts and `traffic_ts.rows_by_rsid` carries the time series split by suite.
# MAGIC
# MAGIC ### How to run
# MAGIC Databricks → Workspace → Import → File → select this `.py` (it imports as a notebook —
# MAGIC the file is in Databricks "source" format). Attach to a cluster (DBR 13+; Plotly ships
# MAGIC with DBR-ML). Run the **Config** cell, adjust the widgets that appear at the top, then
# MAGIC Run All. Each chart cell is independent — re-run one after changing a widget. Colour
# MAGIC palette is CVD-validated (dataviz skill, dark surface).
# MAGIC
# MAGIC Each panel also prints a `===== BEGIN SHAREABLE: chart:<id> =====` block; copy those
# MAGIC back verbatim (multi-part blocks reassemble by concatenation — paste every part). If a
# MAGIC panel is empty, check the `scope` block first: 0 rows means the date range, geo filters,
# MAGIC or `rsid_list` / `url_scope_*` widgets excluded everything.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config, widgets, theme & helpers

# COMMAND ----------

from pyspark.sql import functions as F
import json, math, hashlib

# Plotly ships with Databricks Runtime for ML. If you are on a plain runtime, uncomment:
# %pip install -q plotly
import plotly.graph_objects as go
import plotly.io as pio

# ---------------------------------------------------------------- widgets ----
dbutils.widgets.text("table_fqn", "gwam_prod_catalog.inv_typed_common.adobe_hit_data", "1. Table (catalog.schema.table)")
dbutils.widgets.text("rsid_list", "manugrs,manulifeglobalprod", "2. rsid list (comma-sep, empty = off)")
dbutils.widgets.dropdown("url_scope_mode", "broad", ["broad", "en_only", "custom"], "3. URL scope mode")
dbutils.widgets.text("url_scope_list", "%/group-retirement%,%/group-plans%,%/regimes-collectifs%", "3b. URL patterns for custom mode (SQL LIKE)")
dbutils.widgets.text("url_scope_exclude", "%adobeaemcloud.com%,%/ph/%", "3c. URL patterns to exclude")
dbutils.widgets.text("login_host_exclude",
                     "%portal.manulife.ca%,%id.manulife.ca%,%grsmembers.manulife.com%,"
                     "%gsrs1.manulife.com%,%viproom.manulife.com%,%portail.manuvie.ca%",
                     "3d. Individual-login hosts to exclude (D8)")
dbutils.widgets.text("start_date", "2026-02-01", "4. Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2026-07-07", "5. End date (YYYY-MM-DD)")
dbutils.widgets.dropdown("geo_country", "ALL",
                         ["ALL", "can", "usa", "hkg", "phl", "ind", "gbr"], "6. Country (geo_country)")
dbutils.widgets.text("geo_region", "", "7. Region(s), comma-sep e.g. on,qc (empty = all)")
dbutils.widgets.dropdown("timezone", "America/Toronto",
                         ["America/Toronto", "America/Vancouver", "America/New_York",
                          "America/Sao_Paulo", "Europe/London", "Europe/Paris",
                          "Asia/Hong_Kong", "Asia/Manila", "Asia/Kolkata",
                          "Asia/Tokyo", "Australia/Sydney"], "8. Reviewer timezone")
dbutils.widgets.dropdown("granularity", "daily", ["daily", "weekly"], "9. Time granularity")
dbutils.widgets.text("top_n", "12", "10. Top-N for dimension bars")

TABLE_FQN   = dbutils.widgets.get("table_fqn").strip()
def _csv(widget):
    return [p.strip().lower() for p in dbutils.widgets.get(widget).split(",") if p.strip()]

RSID_LIST      = _csv("rsid_list")
URL_SCOPE_MODE = dbutils.widgets.get("url_scope_mode").strip().lower()
URL_EXCLUDE    = _csv("url_scope_exclude")
LOGIN_EXCLUDE  = _csv("login_host_exclude")

# Same scope contract as the EDA notebook (doc-16 D5). `broad` is the default: the
# three patterns are language- and domain-agnostic, so one list covers manugrs
# (manulifeim.com) and manulifeglobalprod (manulife.com), EN and FR.
URL_SCOPE_EN_ONLY = ["%manulife.com/ca/en/personal/group-plans/group-retirement%"]
URL_SCOPE_BROAD   = ["%/group-retirement%", "%/group-plans%", "%/regimes-collectifs%"]
URL_INCLUDE = {"en_only": URL_SCOPE_EN_ONLY,
               "broad":   URL_SCOPE_BROAD}.get(URL_SCOPE_MODE, _csv("url_scope_list"))
START_DATE  = dbutils.widgets.get("start_date").strip()
END_DATE    = dbutils.widgets.get("end_date").strip()
GEO_COUNTRY = dbutils.widgets.get("geo_country").strip().lower()
GEO_REGIONS = [r.strip().lower() for r in dbutils.widgets.get("geo_region").split(",") if r.strip()]
TIMEZONE    = dbutils.widgets.get("timezone").strip()
GRANULARITY = dbutils.widgets.get("granularity").strip().lower()
TOP_N       = int(dbutils.widgets.get("top_n"))

# ---------------------------------------------------- dark theme (dataviz) ----
# Categorical hues: the dataviz reference dark palette (validated CVD, surface #1a1a19).
# Fixed order — assigned by entity, never cycled.
CATEGORICAL = ["#3987e5", "#199e70", "#c98500", "#008300",
               "#9085e9", "#e66767", "#d55181", "#d95926"]
# Sequential blue for magnitude (heatmap / choropleth), stepped for the DARK surface:
# near-zero recedes toward the surface, high values brighten.
SEQ_BLUE = [[0.0, "#10233d"], [0.25, "#184f95"], [0.5, "#256abf"],
            [0.75, "#3987e5"], [1.0, "#86b6ef"]]
INK, INK2, SURFACE, PLANE, GRID = "#ffffff", "#c3c2b7", "#1a1a19", "#0d0d0d", "#2c2c2a"

pio.templates["gmai_dark"] = go.layout.Template(layout=dict(
    paper_bgcolor=PLANE, plot_bgcolor=SURFACE,
    font=dict(color=INK2, family='"Segoe UI", system-ui, sans-serif', size=13),
    colorway=CATEGORICAL,
    title=dict(font=dict(color=INK, size=18), x=0.01, xanchor="left"),
    xaxis=dict(gridcolor=GRID, zerolinecolor="#383835", linecolor="#383835", ticks="outside"),
    yaxis=dict(gridcolor=GRID, zerolinecolor="#383835", linecolor="#383835", rangemode="tozero"),
    legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", yanchor="bottom", y=1.02, x=0),
    margin=dict(l=60, r=30, t=70, b=50), hovermode="x unified",
))
pio.templates.default = "plotly_dark+gmai_dark"

def render(fig, height=460):
    """Render interactively in Databricks (displayHTML) or Jupyter (fig.show())."""
    fig.update_layout(height=height)
    try:
        displayHTML(fig.to_html(include_plotlyjs="cdn", full_html=False))  # noqa: F821
    except NameError:
        fig.show()

# ------------------------------------------------------------ data helpers ----
def nonblank(c):
    col = F.col(c)
    return col.isNotNull() & (F.trim(col.cast("string")) != "")

def local_ts():
    """hit_time_gmt is epoch-seconds GMT -> convert to the reviewer's TIMEZONE.
    Falls back to date_time (already Eastern local) if hit_time_gmt is absent."""
    cols = set(base_df.columns)
    if "hit_time_gmt" in cols:
        return F.from_utc_timestamp(F.timestamp_seconds(F.col("hit_time_gmt").cast("long")), TIMEZONE)
    return F.to_timestamp(F.col("date_time"))

def trunc_period(ts_col):
    return F.date_trunc("week", ts_col) if GRANULARITY == "weekly" else F.to_date(ts_col)

# --------------------------------------------------- shareable data emit ----
# Mirrors the EDA notebook's emit() protocol: the exported .ipynb carries the
# aggregate data behind every chart as machine-readable JSON, so a run can be
# analysed offline without re-querying. Aggregate-only per ADR-0007.
CHART_DATA = {}

def _scrub(obj):
    """Round floats (4dp), truncate long strings — parity with the EDA scrubber."""
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub(v) for v in obj]
    if isinstance(obj, str):
        return obj if len(obj) <= 160 else obj[:160] + "...<trunc>"
    if isinstance(obj, float):
        return round(obj, 4) if math.isfinite(obj) else None
    return obj

def _records(pdf, date_cols=()):
    """pandas frame -> list of plain-Python dicts (native int/float, ISO date strings).
    Round-tripping through to_json/loads drops numpy int64/float64 and Timestamps."""
    pdf = pdf.copy()
    for c in date_cols:
        if c in pdf.columns:
            pdf[c] = pdf[c].astype(str)
    return json.loads(pdf.to_json(orient="records"))

def share(chart_id, payload):
    """Print + register the aggregate behind a chart as a SHAREABLE JSON block."""
    payload = _scrub(payload)
    CHART_DATA[chart_id] = payload
    body = json.dumps(payload, separators=(",", ":"), default=str)
    sid = f"chart:{chart_id}"
    print(f"===== BEGIN SHAREABLE: {sid} =====")
    if len(body) <= 48000:
        print(body)
    else:
        n_parts = math.ceil(len(body) / 40000)
        for i in range(n_parts):
            print(f"----- part {i+1} of {n_parts} (concatenate parts to reassemble) -----")
            print(body[i * 40000:(i + 1) * 40000])
    print(f"===== END SHAREABLE: {sid} =====")

print(f"table={TABLE_FQN}  tz={TIMEZONE}  granularity={GRANULARITY}")
print(f"scope: rsid={RSID_LIST or '(off)'}  url_mode={URL_SCOPE_MODE}  "
      f"include={URL_INCLUDE or '(off)'}  login_hosts_excluded={len(LOGIN_EXCLUDE)}")
print(f"       country={GEO_COUNTRY}  regions={GEO_REGIONS or 'all'}  dates={START_DATE}..{END_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load & scope the CA-Retirement subset
# MAGIC Applies the report-suite + URL scope (same definition as the EDA notebook), then the
# MAGIC widget-driven country / region / date filters. `base_df` is reused by every chart.

# COMMAND ----------

_raw = spark.table(TABLE_FQN)
_cols = set(_raw.columns)

# --- scope: rsid IN list AND URL matches include list, minus both exclude lists ---
def _like_any(colexpr, patterns):
    """Null-safe OR of SQL LIKE patterns; None when `patterns` is empty. Blank/NULL
    input yields False, never NULL, so ~_like_any(...) keeps rather than drops."""
    if not patterns:
        return None
    m = None
    for p in patterns:
        m = colexpr.like(p) if m is None else (m | colexpr.like(p))
    return F.coalesce(m, F.lit(False))

_scope = None
if RSID_LIST and "rsid" in _cols:
    _scope = F.lower(F.trim(F.col("rsid").cast("string"))).isin(RSID_LIST)

# D4: blank-guarded coalesce(page_url, post_page_url) — page_url FIRST. This notebook
# used to prefer post_page_url, which is blank 36-46% of the time vs <=0.013% for
# page_url. Adobe writes empty strings, not NULLs, so blanks are mapped to NULL before
# the coalesce or it would never fall through.
_url_cols = [c for c in ("page_url", "post_page_url") if c in _cols]
_url_col = _url_cols[0] if _url_cols else None
_u = None
if _url_cols:
    _u = F.lower(F.coalesce(*[F.when(F.trim(F.col(c).cast("string")) != F.lit(""),
                                     F.trim(F.col(c).cast("string"))) for c in _url_cols],
                            F.lit("")))
    _inc = _like_any(_u, URL_INCLUDE)
    if _inc is not None:
        _scope = _inc if _scope is None else (_scope & _inc)
    # URL_EXCLUDE = AEM/staging + non-CA paths. LOGIN_EXCLUDE = the six D8 member-auth
    # hosts, subtracted in every mode — an explicit host list, not a %portal% pattern,
    # since four of the six carry no "portal" substring and FR uses "portail".
    for _pats in (URL_EXCLUDE, LOGIN_EXCLUDE):
        _m = _like_any(_u, _pats)
        if _m is not None:
            _scope = ~_m if _scope is None else (_scope & ~_m)
base_df = _raw.filter(_scope) if _scope is not None else _raw

# --- date range on the reviewer-local calendar date ---
base_df = base_df.filter((F.to_date(local_ts()) >= F.lit(START_DATE)) &
                         (F.to_date(local_ts()) <= F.lit(END_DATE)))

# --- geography filters ---
if GEO_COUNTRY != "all" and "geo_country" in _cols:
    base_df = base_df.filter(F.lower(F.col("geo_country")) == F.lit(GEO_COUNTRY))
if GEO_REGIONS and "geo_region" in _cols:
    base_df = base_df.filter(F.lower(F.col("geo_region")).isin(GEO_REGIONS))

base_df = base_df.cache()
_scoped_rows = base_df.count()
print(f"scoped rows in view: {_scoped_rows:,}")
if _scoped_rows == 0:
    print("!! 0 rows — widen the date range / clear the geo filters / check the scope widgets.")

# Per-suite counts: a suite at 0 means every chart below is silently single-suite.
_rsid_breakdown = []
if "rsid" in _cols:
    _rsid_breakdown = [{"rsid": r["rsid"], "rows": r["count"]}
                       for r in (base_df.groupBy("rsid").count()
                                        .orderBy(F.desc("count")).collect())]
    _missing = [s for s in RSID_LIST
                if s not in {str(b["rsid"] or "").lower() for b in _rsid_breakdown}]
    if _missing:
        print(f"!! 0 rows for rsid(s): {_missing} — charts below cover only the rest.")

share("scope", {"table": TABLE_FQN, "rsid_list": RSID_LIST or None,
                "url_scope_mode": URL_SCOPE_MODE, "url_include": URL_INCLUDE or None,
                "url_exclude": URL_EXCLUDE or None,
                "login_host_exclude": LOGIN_EXCLUDE or None,
                "url_cols_coalesced": _url_cols or None,
                "start_date": START_DATE,
                "end_date": END_DATE, "geo_country": GEO_COUNTRY,
                "geo_regions": GEO_REGIONS or None, "timezone": TIMEZONE,
                "granularity": GRANULARITY, "top_n": TOP_N,
                "scoped_rows": _scoped_rows, "rsid_breakdown": _rsid_breakdown})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Traffic over time — hits, visits, visitors
# MAGIC **Form:** multi-series line (change over time). Weekly seasonality is the dominant feature
# MAGIC (weekends ≈ 40 % of weekdays); RRSP season (Feb–Mar) is the volume peak. Drag on the range
# MAGIC slider to zoom; toggle a series in the legend.

# COMMAND ----------

vis_hi = "post_visid_high" if "post_visid_high" in _cols else ("visid_high" if "visid_high" in _cols else None)
vis_lo = "post_visid_low" if "post_visid_low" in _cols else ("visid_low" if "visid_low" in _cols else None)

_aggs = [F.count("*").alias("hits")]
if vis_hi and vis_lo and "visit_num" in _cols:
    _aggs.append(F.approx_count_distinct(F.concat_ws(":", vis_hi, vis_lo, "visit_num")).alias("visits"))
    _aggs.append(F.approx_count_distinct(F.concat_ws(":", vis_hi, vis_lo)).alias("visitors"))

_ts = (base_df.groupBy(trunc_period(local_ts()).alias("period")).agg(*_aggs)
              .orderBy("period").toPandas())

# Per-suite series alongside the combined one. The plot below draws the combined
# view; this keeps the suites separable in the shared payload, where a shift in one
# suite would otherwise be diluted by the other's volume.
_ts_rsid = None
if "rsid" in _cols and len(_rsid_breakdown) > 1:
    _ts_rsid = (base_df.groupBy(trunc_period(local_ts()).alias("period"), F.col("rsid"))
                       .agg(*_aggs).orderBy("period", "rsid").toPandas())

share("traffic_ts", {"tz": TIMEZONE, "granularity": GRANULARITY,
                     "rows": _records(_ts, date_cols=["period"]),
                     "rows_by_rsid": (_records(_ts_rsid, date_cols=["period"])
                                      if _ts_rsid is not None else None)})

fig = go.Figure()
_series = [("hits", CATEGORICAL[0]), ("visits", CATEGORICAL[1]), ("visitors", CATEGORICAL[3])]
for name, color in _series:
    if name in _ts.columns:
        fig.add_trace(go.Scatter(x=_ts["period"], y=_ts[name], name=name, mode="lines",
                                 line=dict(color=color, width=2),
                                 hovertemplate=f"%{{x|%a %Y-%m-%d}}<br>{name}: %{{y:,}}<extra></extra>"))
# direct end-labels (secondary encoding for the CVD floor band)
for name, color in _series:
    if name in _ts.columns and len(_ts):
        fig.add_annotation(x=_ts["period"].iloc[-1], y=_ts[name].iloc[-1], text=f" {name}",
                           showarrow=False, xanchor="left", font=dict(color=color, size=12))
fig.update_layout(title=f"CA-Retirement traffic ({GRANULARITY}) — {TIMEZONE}",
                  yaxis_title="count", xaxis_title=None)
fig.update_xaxes(rangeslider=dict(visible=True), rangeslider_thickness=0.06)
render(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · When are users active? — day-of-week × hour heatmap
# MAGIC **Form:** sequential heatmap (magnitude). Computed in the **selected timezone**, so the
# MAGIC peak-hour band shifts as you change the widget. Under Eastern time the peak is ~10:00 on
# MAGIC weekdays; switch to `Asia/Hong_Kong` to see the same hits on that clock.

# COMMAND ----------

_hh = (base_df.select(F.dayofweek(local_ts()).alias("dow"), F.hour(local_ts()).alias("hr"))
              .groupBy("dow", "hr").count().toPandas())
# Spark dayofweek: 1=Sun..7=Sat -> reorder to Mon..Sun
_dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_z = [[0] * 24 for _ in range(7)]
for _, r in _hh.iterrows():
    _z[(int(r["dow"]) + 5) % 7][int(r["hr"])] = int(r["count"])

share("dow_hour", {"tz": TIMEZONE, "dow_mon_to_sun": _dow_names,
                   "hours_0_23": [f"{h:02d}" for h in range(24)], "z": _z})

fig = go.Figure(go.Heatmap(
    z=_z, x=[f"{h:02d}" for h in range(24)], y=_dow_names, colorscale=SEQ_BLUE,
    colorbar=dict(title="hits", outlinewidth=0),
    hovertemplate="%{y} %{x}:00<br>hits: %{z:,}<extra></extra>"))
fig.update_layout(title=f"Activity by weekday × hour — {TIMEZONE}",
                  xaxis_title="hour of day", yaxis_title=None, hovermode="closest")
fig.update_yaxes(autorange="reversed")
render(fig, height=380)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Where does traffic come from? — country choropleth
# MAGIC **Form:** sequential choropleth (magnitude by place). CA-Retirement is ~85 % Canada, ~11 %
# MAGIC USA, with a long tail (HKG / PHL / IND). Adobe stores ISO-3 lowercase codes; we upper-case
# MAGIC for the map. Hover a country for its hit count.

# COMMAND ----------

if "geo_country" in _cols:
    _gc = (base_df.filter(nonblank("geo_country"))
                  .groupBy(F.upper(F.col("geo_country")).alias("iso3")).count()
                  .orderBy(F.desc("count")).toPandas())
    share("geo_country", {"rows": _records(_gc)})
    fig = go.Figure(go.Choropleth(
        locations=_gc["iso3"], z=_gc["count"], locationmode="ISO-3",
        colorscale=SEQ_BLUE, colorbar=dict(title="hits", outlinewidth=0),
        marker_line_color="#2c2c2a",
        hovertemplate="%{location}<br>hits: %{z:,}<extra></extra>"))
    fig.update_layout(title="Traffic by country (geo_country)",
                      geo=dict(bgcolor=SURFACE, lakecolor=SURFACE,
                               showframe=False, showcoastlines=False,
                               projection_type="natural earth"))
    render(fig, height=460)
else:
    print("geo_country not present in this table build — skipping country map.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Top regions & pages
# MAGIC **Form:** horizontal magnitude bars (single hue — one measure). Canadian provinces
# MAGIC (`geo_region`: ON ~47 %, AB ~15 %, BC ~12 %, QC ~4 %) and the busiest pages
# MAGIC (`ca-ret:personal:overview` dominates).

# COMMAND ----------

def top_bar(col, title):
    if col not in _cols:
        print(f"{col} not present — skipping.")
        return
    pdf = (base_df.filter(nonblank(col)).groupBy(col).count()
                  .orderBy(F.desc("count")).limit(TOP_N).toPandas().iloc[::-1])
    share(f"top_{col}", {"top_n": TOP_N, "rows": _records(pdf)})
    fig = go.Figure(go.Bar(
        x=pdf["count"], y=pdf[col].astype(str), orientation="h",
        marker=dict(color="#3987e5"),
        text=pdf["count"].map(lambda v: f"{v:,}"), textposition="outside",
        hovertemplate="%{y}<br>hits: %{x:,}<extra></extra>"))
    fig.update_layout(title=title, xaxis_title="hits", yaxis_title=None, margin=dict(l=180))
    render(fig, height=max(320, 26 * len(pdf) + 120))

top_bar("geo_region", f"Top {TOP_N} regions (geo_region)")
top_bar("pagename", f"Top {TOP_N} pages (pagename)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Language mix over time
# MAGIC **Form:** stacked area (composition over time). Adobe numeric codes: **45 ≈ English**
# MAGIC (~63 %), **39 ≈ French** (~30 %); everything else grouped as *Other*. A stable mix is the
# MAGIC expected state — a sudden swing is worth investigating.

# COMMAND ----------

_LANG = {"45": "English (45)", "39": "French (39)"}
if "language" in _cols:
    lang_expr = F.col("language").cast("string")
    label = F.when(lang_expr == "45", F.lit(_LANG["45"])) \
             .when(lang_expr == "39", F.lit(_LANG["39"])).otherwise(F.lit("Other"))
    _lm = (base_df.filter(nonblank("language"))
                  .groupBy(trunc_period(local_ts()).alias("period"), label.alias("lang"))
                  .count().orderBy("period").toPandas())
    share("language_mix", {"granularity": GRANULARITY,
                           "rows": _records(_lm, date_cols=["period"])})
    _piv = _lm.pivot_table(index="period", columns="lang", values="count", fill_value=0)
    _order = [c for c in ["English (45)", "French (39)", "Other"] if c in _piv.columns]
    _colors = {"English (45)": CATEGORICAL[0], "French (39)": CATEGORICAL[1], "Other": "#898781"}
    fig = go.Figure()
    for name in _order:
        fig.add_trace(go.Scatter(x=_piv.index, y=_piv[name], name=name, mode="lines",
                                 stackgroup="one", line=dict(width=0.5, color=_colors[name]),
                                 fillcolor=_colors[name],
                                 hovertemplate=f"%{{x|%Y-%m-%d}}<br>{name}: %{{y:,}}<extra></extra>"))
    fig.update_layout(title=f"Language mix ({GRANULARITY})", yaxis_title="hits", xaxis_title=None)
    render(fig)
else:
    print("language not present — skipping language mix.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Event / KPI firing timeline
# MAGIC **Form:** multi-series line (change over time). Daily count of hits **carrying** each KPI
# MAGIC event. This panel makes the **instrumentation on/off shifts** visible: `ev501–504` start
# MAGIC **2026-02-24**; `ev500` fires **only 2026-04-02 → mid-May**. Those are known tagging
# MAGIC changes, *not* anomalies — the detector must treat them as change-points.
# MAGIC
# MAGIC Event names are the Adobe platform defaults from `new_data/event.tsv` (20 = Campaign View;
# MAGIC 500–504 = clickmap events). These IDs are fully resolved; only the eVar *content* meaning
# MAGIC (what each eVar captures) still needs the eVar dictionary.

# COMMAND ----------

# Names from new_data/event.tsv (standard Adobe event lookup).
_KPI_EVENTS = {"500": "ev500 clickmappage", "20": "ev20 campaign-view",
               "501": "ev501 clickmaplink", "502": "ev502 clickmapregion",
               "503": "ev503 clickmaplinkbyregion", "504": "ev504 targetsessionid"}
_ev_col = "post_event_list" if "post_event_list" in _cols else ("event_list" if "event_list" in _cols else None)
if _ev_col:
    ids = F.array_distinct(F.transform(
        F.filter(F.transform(F.split(F.col(_ev_col), ","), lambda x: F.trim(x)), lambda x: x != ""),
        lambda x: F.split(x, "=")[0]))
    _ev = (base_df.filter(nonblank(_ev_col))
                  .select(trunc_period(local_ts()).alias("period"), F.explode(ids).alias("eid"))
                  .filter(F.col("eid").isin(list(_KPI_EVENTS.keys())))
                  .groupBy("period", "eid").count().orderBy("period").toPandas())
    share("event_timeline", {"granularity": GRANULARITY, "event_names": _KPI_EVENTS,
                             "rows": _records(_ev, date_cols=["period"])})
    fig = go.Figure()
    for i, (eid, label) in enumerate(_KPI_EVENTS.items()):
        sub = _ev[_ev["eid"] == eid]
        if len(sub):
            fig.add_trace(go.Scatter(x=sub["period"], y=sub["count"], name=label, mode="lines",
                                     line=dict(color=CATEGORICAL[i % len(CATEGORICAL)], width=2),
                                     hovertemplate=f"%{{x|%Y-%m-%d}}<br>{label}: %{{y:,}} hits<extra></extra>"))
    fig.update_layout(title=f"KPI event firing ({GRANULARITY}) — hits carrying each event",
                      yaxis_title="hits with event", xaxis_title=None)
    render(fig)
else:
    print("post_event_list not present — skipping event timeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Monthly seasonality (RRSP season)
# MAGIC **Form:** magnitude bars by month. February–March (RRSP contribution season) is the peak;
# MAGIC volume tapers into summer. A month-over-month detector must expect this and not alarm on it.

# COMMAND ----------

_mo = (base_df.groupBy(F.date_format(local_ts(), "yyyy-MM").alias("month")).count()
              .orderBy("month").toPandas())
share("monthly_volume", {"rows": _records(_mo)})
fig = go.Figure(go.Bar(x=_mo["month"], y=_mo["count"], marker=dict(color="#3987e5"),
                       text=_mo["count"].map(lambda v: f"{v:,}"), textposition="outside",
                       hovertemplate="%{x}<br>hits: %{y:,}<extra></extra>"))
fig.update_layout(title="Monthly hit volume (RRSP seasonality)", yaxis_title="hits", xaxis_title=None)
render(fig, height=380)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data manifest — integrity check for the export
# MAGIC Byte length + sha1 of every `chart:<id>` block above, so a truncated `.ipynb`
# MAGIC export can be caught offline (re-hash a block, compare against this manifest).

# COMMAND ----------

_manifest = {}
for _cid, _payload in CHART_DATA.items():
    _body = json.dumps(_payload, separators=(",", ":"), default=str)
    _manifest[_cid] = {"bytes": len(_body),
                       "sha1": hashlib.sha1(_body.encode("utf-8")).hexdigest()}
share("manifest", {"charts": _manifest, "n_charts": len(_manifest)})

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Notes.** Charts are aggregate-only (ADR-0007). Geography columns (`geo_country`,
# MAGIC `geo_region`) are populated in this source table but are **not yet in the production
# MAGIC pipeline bronze layer** — see `research/claude/12-eda-findings-analysis.md` §6 and
# MAGIC `databricks/conf/bronze_columns.py` if these need to reach the detector. Palette is the
# MAGIC CVD-validated dataviz dark set; multi-series panels carry a legend + hover + direct labels
# MAGIC so identity is never colour-alone.

# COMMAND ----------

base_df.unpersist()
