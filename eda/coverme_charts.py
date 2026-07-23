# Databricks notebook source
# MAGIC %md
# MAGIC # GMAI-Pulse — CoverMe · Interactive Charts
# MAGIC
# MAGIC A **modern, dark-theme, interactive** companion to `coverme_eda.py`. Where the EDA notebook
# MAGIC emits machine-readable JSON, this notebook renders the same observations as **Plotly** charts
# MAGIC you can hover, zoom, and filter.
# MAGIC
# MAGIC ### Scope — URL-only (single-suite)
# MAGIC This table is a single Adobe report suite (no `rsid` column), so CoverMe is scoped by URL
# MAGIC alone. Default `url_scope_mode=broad` uses the `url_scope_list` widget verbatim, seeded to the
# MAGIC three production hosts (`%coverme.com%` EN, `%pourmeproteger.com%` FR, `%insttrip.manulife.com%`
# MAGIC EN travel) that carry ~99.9% of real CoverMe traffic. `url_scope_exclude` drops UAT / AEM /
# MAGIC staging noise. URL matching uses the D4 blank-guarded coalesce with **`page_url` FIRST**
# MAGIC (0.0005% blank vs 58.9% for post_page_url — inverted vs GWAM). Same contract as the EDA notebook.
# MAGIC
# MAGIC ### What's CoverMe-specific
# MAGIC - **Language mix (~50/50 EN/FR) is a first-class panel**, split by DOMAIN (coverme.com=EN,
# MAGIC   pourmeproteger.com=FR) rather than by a numeric language code.
# MAGIC - **A quote→application funnel panel** (Quote Start → Quote Complete → Save Quote → App Start
# MAGIC   → App Confirm) — the business-flagged anomaly KPIs.
# MAGIC - **Product & sponsor mix** from eVar4 / eVar6.
# MAGIC
# MAGIC ### Privacy (ADR-0007)
# MAGIC Every panel is an **aggregate** (counts / rates by time or by a dimension). No visitor IDs,
# MAGIC IPs, cookies, or user-agents are displayed.
# MAGIC
# MAGIC ### How to run
# MAGIC Databricks → Workspace → Import → File → select this `.py`. Attach a cluster (DBR 13+; Plotly
# MAGIC ships with DBR-ML). Run the **Config** cell, adjust widgets, then Run All. Each panel also
# MAGIC prints a `===== BEGIN SHAREABLE: chart:<id> =====` block. If a panel is empty, check the
# MAGIC `scope` block first — 0 rows means the date range / geo filters / scope widgets excluded everything.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config, widgets, theme & helpers

# COMMAND ----------

from pyspark.sql import functions as F
import json, math, hashlib

# Plotly ships with Databricks Runtime for ML. On a plain runtime, uncomment:
# %pip install -q plotly
import plotly.graph_objects as go
import plotly.io as pio

# ---------------------------------------------------------------- widgets ----
dbutils.widgets.text("table_fqn", "csdo_prod_catalog.adobe_coverme_bronze.hit_data", "1. Table (catalog.schema.table)")
dbutils.widgets.dropdown("url_scope_mode", "broad", ["broad", "tight"], "2. URL scope mode (tight = coverme.com only)")
dbutils.widgets.text("url_scope_list", "%coverme.com%,%pourmeproteger.com%,%insttrip.manulife.com%",
                     "2b. URL include patterns — ADD URLS HERE (SQL LIKE, comma-sep)")
dbutils.widgets.text("url_scope_exclude",
                     "%adobeaemcloud.com%,%author-aem-prod.manulife.ca%,%uat.coverme.com%,"
                     "%uat.pourmeproteger.com%,%.uat.%,%www-aem-stage%,%localhost:5000%",
                     "2c. URL patterns to exclude (UAT/AEM/staging noise)")
dbutils.widgets.text("start_date", "2025-07-01", "3. Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2026-07-21", "4. End date (YYYY-MM-DD)")
dbutils.widgets.dropdown("geo_country", "ALL",
                         ["ALL", "can", "usa", "fra", "ind", "gbr", "phl"], "5. Country (geo_country)")
dbutils.widgets.text("geo_region", "", "6. Region(s), comma-sep e.g. on,qc (empty = all)")
dbutils.widgets.dropdown("timezone", "America/Toronto",
                         ["America/Toronto", "America/Vancouver", "America/New_York",
                          "America/Sao_Paulo", "Europe/London", "Europe/Paris",
                          "Asia/Hong_Kong", "Asia/Manila", "Asia/Kolkata",
                          "Asia/Tokyo", "Australia/Sydney"], "7. Reviewer timezone")
dbutils.widgets.dropdown("granularity", "daily", ["daily", "weekly"], "8. Time granularity")
dbutils.widgets.text("top_n", "12", "9. Top-N for dimension bars")

TABLE_FQN   = dbutils.widgets.get("table_fqn").strip()
def _csv(widget):
    return [p.strip().lower() for p in dbutils.widgets.get(widget).split(",") if p.strip()]

URL_SCOPE_MODE = dbutils.widgets.get("url_scope_mode").strip().lower()
URL_EXCLUDE    = _csv("url_scope_exclude")

# Same scope contract as the EDA notebook. The `url_scope_list` widget is AUTHORITATIVE in `broad`;
# `tight` pins coverme.com only.
URL_SCOPE_TIGHT = ["%coverme.com%"]
URL_INCLUDE = URL_SCOPE_TIGHT if URL_SCOPE_MODE == "tight" else _csv("url_scope_list")
START_DATE  = dbutils.widgets.get("start_date").strip()
END_DATE    = dbutils.widgets.get("end_date").strip()
GEO_COUNTRY = dbutils.widgets.get("geo_country").strip().lower()
GEO_REGIONS = [r.strip().lower() for r in dbutils.widgets.get("geo_region").split(",") if r.strip()]
TIMEZONE    = dbutils.widgets.get("timezone").strip()
GRANULARITY = dbutils.widgets.get("granularity").strip().lower()
TOP_N       = int(dbutils.widgets.get("top_n"))

# The quote -> application funnel (business-flagged anomaly KPIs), in LOGICAL step order.
FUNNEL_EVENTS = [("228", "Quote Start"), ("229", "Quote Complete"), ("232", "Save Quote"),
                 ("269", "App Start"), ("240", "App Confirm")]

# ---------------------------------------------------- dark theme (dataviz) ----
CATEGORICAL = ["#3987e5", "#199e70", "#c98500", "#008300",
               "#9085e9", "#e66767", "#d55181", "#d95926"]
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
    col = F.col("`" + c.replace("`", "``") + "`")
    return col.isNotNull() & (F.trim(col.cast("string")) != "")

def local_ts():
    """hit_time_gmt is epoch-seconds GMT -> convert to the reviewer's TIMEZONE. Falls back to
    date_time (already local) if hit_time_gmt is absent."""
    cols = set(base_df.columns)
    if "hit_time_gmt" in cols:
        return F.from_utc_timestamp(F.timestamp_seconds(F.col("hit_time_gmt").cast("long")), TIMEZONE)
    return F.to_timestamp(F.col("date_time"))

def trunc_period(ts_col):
    return F.date_trunc("week", ts_col) if GRANULARITY == "weekly" else F.to_date(ts_col)

# --------------------------------------------------- shareable data emit ----
CHART_DATA = {}
MAX_EMIT_STR = 2000

def _scrub(obj):
    """Formatting only (ADR-0007 §5 full-raw): round floats (4dp) and truncate absurdly long
    strings so a single value can't blow the Databricks stdout cap. No redaction."""
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub(v) for v in obj]
    if isinstance(obj, str):
        return obj if len(obj) <= MAX_EMIT_STR else obj[:MAX_EMIT_STR] + "...<trunc>"
    if isinstance(obj, float):
        return round(obj, 4) if math.isfinite(obj) else None
    return obj

def _records(pdf, date_cols=()):
    """pandas frame -> list of plain-Python dicts (native int/float, ISO date strings)."""
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

# host -> language, CoverMe splits language by DOMAIN not path
def lang_from_host(host_col):
    return (F.when(host_col.rlike(r"pourmeproteger|manuvie|assurance-manuvie"), "French")
             .when(host_col.rlike(r"coverme\.com|insttrip\.manulife\.com"), "English")
             .otherwise("Other"))

print(f"table={TABLE_FQN}  tz={TIMEZONE}  granularity={GRANULARITY}")
print(f"scope (URL-only): url_mode={URL_SCOPE_MODE}  include={URL_INCLUDE or '(off)'}  "
      f"exclude={len(URL_EXCLUDE)} patterns")
print(f"       country={GEO_COUNTRY}  regions={GEO_REGIONS or 'all'}  dates={START_DATE}..{END_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load & scope the CoverMe subset
# MAGIC Applies the URL scope (same definition as the EDA notebook — page_url-first coalesce), then
# MAGIC the widget-driven country / region / date filters. `base_df` is reused by every chart. A
# MAGIC **host breakdown** replaces GWAM's per-suite breakdown (single-suite table).

# COMMAND ----------

_raw = spark.table(TABLE_FQN)
_cols = set(_raw.columns)

def _like_any(colexpr, patterns):
    """Null-safe OR of SQL LIKE patterns; None when empty. Blank/NULL -> False, never NULL."""
    if not patterns:
        return None
    m = None
    for p in patterns:
        m = colexpr.like(p) if m is None else (m | colexpr.like(p))
    return F.coalesce(m, F.lit(False))

# D4: blank-guarded coalesce over the four URL candidates — page_url FIRST (0.0005% blank vs 58.9%
# for post_page_url). Adobe writes empty strings, not NULLs, so blanks map to NULL before coalesce.
_url_cands = ("page_url", "visit_start_page_url", "first_hit_page_url", "post_page_url")
_url_cols = [c for c in _url_cands if c in _cols]
_url_col = _url_cols[0] if _url_cols else None
_scope = None
_u = None
if _url_cols:
    _u = F.lower(F.coalesce(*[F.when(F.trim(F.col(c).cast("string")) != F.lit(""),
                                     F.trim(F.col(c).cast("string"))) for c in _url_cols], F.lit("")))
    _inc = _like_any(_u, URL_INCLUDE)
    if _inc is not None:
        _scope = _inc if _scope is None else (_scope & _inc)
    _exc = _like_any(_u, URL_EXCLUDE)
    if _exc is not None:
        _scope = ~_exc if _scope is None else (_scope & ~_exc)
base_df = _raw.filter(_scope) if _scope is not None else _raw

# date range on the reviewer-local calendar date; prefer the hit_date partition column for pruning
if "hit_date" in _cols:
    base_df = base_df.filter((F.col("hit_date") >= F.lit(START_DATE).cast("date")) &
                             (F.col("hit_date") <= F.lit(END_DATE).cast("date")))
else:
    base_df = base_df.filter((F.to_date(local_ts()) >= F.lit(START_DATE)) &
                             (F.to_date(local_ts()) <= F.lit(END_DATE)))

if GEO_COUNTRY != "all" and "geo_country" in _cols:
    base_df = base_df.filter(F.lower(F.col("geo_country")) == F.lit(GEO_COUNTRY))
if GEO_REGIONS and "geo_region" in _cols:
    base_df = base_df.filter(F.lower(F.col("geo_region")).isin(GEO_REGIONS))

base_df = base_df.cache()
_scoped_rows = base_df.count()
print(f"scoped rows in view: {_scoped_rows:,}")
if _scoped_rows == 0:
    print("!! 0 rows — widen the date range / clear the geo filters / check the scope widgets.")

# host breakdown: a single-host result means the scope silently collapsed to one brand/language.
_host = F.regexp_extract(F.regexp_replace(_u, r"^[a-z]+://", ""), r"^([^/?#]+)", 1) if _u is not None else F.lit("")
_host_breakdown = [{"host": r["host"], "rows": r["count"]}
                   for r in (base_df.select(_host.alias("host")).groupBy("host").count()
                             .orderBy(F.desc("count")).limit(15).collect())]

share("scope", {"table": TABLE_FQN, "single_suite": True,
                "url_scope_mode": URL_SCOPE_MODE, "url_include": URL_INCLUDE or None,
                "url_exclude": URL_EXCLUDE or None, "url_cols_coalesced": _url_cols or None,
                "start_date": START_DATE, "end_date": END_DATE, "geo_country": GEO_COUNTRY,
                "geo_regions": GEO_REGIONS or None, "timezone": TIMEZONE,
                "granularity": GRANULARITY, "top_n": TOP_N,
                "scoped_rows": _scoped_rows, "host_breakdown": _host_breakdown})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Traffic over time — hits, visits, visitors
# MAGIC **Form:** multi-series line (change over time). Weekly seasonality is the dominant feature.
# MAGIC Drag on the range slider to zoom; toggle a series in the legend.

# COMMAND ----------

vis_hi = "post_visid_high" if "post_visid_high" in _cols else ("visid_high" if "visid_high" in _cols else None)
vis_lo = "post_visid_low" if "post_visid_low" in _cols else ("visid_low" if "visid_low" in _cols else None)

_aggs = [F.count("*").alias("hits")]
if vis_hi and vis_lo and "visit_num" in _cols:
    _aggs.append(F.approx_count_distinct(F.concat_ws(":", vis_hi, vis_lo, "visit_num")).alias("visits"))
    _aggs.append(F.approx_count_distinct(F.concat_ws(":", vis_hi, vis_lo)).alias("visitors"))

_ts = (base_df.groupBy(trunc_period(local_ts()).alias("period")).agg(*_aggs)
              .orderBy("period").toPandas())
share("traffic_ts", {"tz": TIMEZONE, "granularity": GRANULARITY,
                     "rows": _records(_ts, date_cols=["period"])})

fig = go.Figure()
_series = [("hits", CATEGORICAL[0]), ("visits", CATEGORICAL[1]), ("visitors", CATEGORICAL[3])]
for name, color in _series:
    if name in _ts.columns:
        fig.add_trace(go.Scatter(x=_ts["period"], y=_ts[name], name=name, mode="lines",
                                 line=dict(color=color, width=2),
                                 hovertemplate=f"%{{x|%a %Y-%m-%d}}<br>{name}: %{{y:,}}<extra></extra>"))
for name, color in _series:
    if name in _ts.columns and len(_ts):
        fig.add_annotation(x=_ts["period"].iloc[-1], y=_ts[name].iloc[-1], text=f" {name}",
                           showarrow=False, xanchor="left", font=dict(color=color, size=12))
fig.update_layout(title=f"CoverMe traffic ({GRANULARITY}) — {TIMEZONE}",
                  yaxis_title="count", xaxis_title=None)
fig.update_xaxes(rangeslider=dict(visible=True), rangeslider_thickness=0.06)
render(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · When are users active? — day-of-week × hour heatmap
# MAGIC **Form:** sequential heatmap (magnitude). Computed in the **selected timezone**, so the
# MAGIC peak-hour band shifts as you change the widget.

# COMMAND ----------

_hh = (base_df.select(F.dayofweek(local_ts()).alias("dow"), F.hour(local_ts()).alias("hr"))
              .groupBy("dow", "hr").count().toPandas())
_dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_z = [[0] * 24 for _ in range(7)]
for _, r in _hh.iterrows():
    if r["hr"] is not None and r["dow"] is not None:
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
# MAGIC **Form:** sequential choropleth (magnitude by place). CoverMe is ~84% Canada, ~12% USA
# MAGIC (travel product), with a long tail. Adobe stores ISO-3 lowercase codes; we upper-case for the map.

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
                      geo=dict(bgcolor=SURFACE, lakecolor=SURFACE, showframe=False,
                               showcoastlines=False, projection_type="natural earth"))
    render(fig, height=460)
else:
    print("geo_country not present in this table build — skipping country map.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Top regions & pages
# MAGIC **Form:** horizontal magnitude bars (single hue). Canadian provinces (`geo_region`) and the
# MAGIC busiest pages (`pagename`).

# COMMAND ----------

def top_bar(col, title):
    if col not in _cols:
        print(f"{col} not present — skipping.")
        return
    pdf = (base_df.filter(nonblank(col)).groupBy(col).count()
                  .orderBy(F.desc("count")).limit(TOP_N).toPandas().iloc[::-1])
    share(f"top_{col}", {"top_n": TOP_N, "rows": _records(pdf)})
    fig = go.Figure(go.Bar(
        x=pdf["count"], y=pdf[col].astype(str), orientation="h", marker=dict(color="#3987e5"),
        text=pdf["count"].map(lambda v: f"{v:,}"), textposition="outside",
        hovertemplate="%{y}<br>hits: %{x:,}<extra></extra>"))
    fig.update_layout(title=title, xaxis_title="hits", yaxis_title=None, margin=dict(l=180))
    render(fig, height=max(320, 26 * len(pdf) + 120))

top_bar("geo_region", f"Top {TOP_N} regions (geo_region)")
top_bar("pagename", f"Top {TOP_N} pages (pagename)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Language mix over time
# MAGIC **Form:** stacked area (composition over time). CoverMe is ~**50/50 English/French**, split by
# MAGIC **domain** (coverme.com/insttrip = English, pourmeproteger.com = French). A stable mix is the
# MAGIC expected state — a sudden swing is worth investigating.

# COMMAND ----------

if _u is not None:
    _host_c = F.regexp_extract(F.regexp_replace(_u, r"^[a-z]+://", ""), r"^([^/?#]+)", 1)
    _lm = (base_df.groupBy(trunc_period(local_ts()).alias("period"),
                           lang_from_host(_host_c).alias("lang"))
                  .count().orderBy("period").toPandas())
    share("language_mix", {"granularity": GRANULARITY, "basis": "domain",
                           "rows": _records(_lm, date_cols=["period"])})
    _piv = _lm.pivot_table(index="period", columns="lang", values="count", fill_value=0)
    _order = [c for c in ["English", "French", "Other"] if c in _piv.columns]
    _colors = {"English": CATEGORICAL[0], "French": CATEGORICAL[1], "Other": "#898781"}
    fig = go.Figure()
    for name in _order:
        fig.add_trace(go.Scatter(x=_piv.index, y=_piv[name], name=name, mode="lines",
                                 stackgroup="one", line=dict(width=0.5, color=_colors[name]),
                                 fillcolor=_colors[name],
                                 hovertemplate=f"%{{x|%Y-%m-%d}}<br>{name}: %{{y:,}}<extra></extra>"))
    fig.update_layout(title=f"Language mix by domain ({GRANULARITY})", yaxis_title="hits", xaxis_title=None)
    render(fig)
else:
    print("no URL column — skipping language mix.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Quote → application funnel + KPI timeline
# MAGIC **Form:** a funnel bar (conversion) plus a multi-series line (change over time). Daily count of
# MAGIC hits **carrying** each funnel event (Quote Start → Quote Complete → Save Quote → App Start →
# MAGIC App Confirm). These are the business-flagged anomaly KPIs; a break in any step is what the
# MAGIC detector must catch. App-level steps fire at low volume — that is expected, not an anomaly.

# COMMAND ----------

_ev_col = "post_event_list" if "post_event_list" in _cols else ("event_list" if "event_list" in _cols else None)
if _ev_col:
    _funnel_ids = [eid for eid, _ in FUNNEL_EVENTS]
    _label_by_id = dict(FUNNEL_EVENTS)
    _ids = F.array_distinct(F.transform(
        F.filter(F.transform(F.split(F.col(_ev_col), ","), lambda x: F.trim(x)), lambda x: x != ""),
        lambda x: F.split(x, "=")[0]))
    _exp = (base_df.filter(nonblank(_ev_col))
                   .select(trunc_period(local_ts()).alias("period"), F.explode(_ids).alias("eid"))
                   .filter(F.col("eid").isin(_funnel_ids)))
    _ev = _exp.groupBy("period", "eid").count().orderBy("period").toPandas()
    _tot = {r["eid"]: r["n"] for r in _exp.groupBy("eid").agg(F.count("*").alias("n")).collect()}
    _funnel_rows = [{"step": name, "event_id": eid, "hits": int(_tot.get(eid, 0))}
                    for eid, name in FUNNEL_EVENTS]
    share("funnel", {"steps": _funnel_rows,
                     "timeline": _records(_ev, date_cols=["period"]), "event_names": _label_by_id})

    # funnel bar (conversion shape)
    figf = go.Figure(go.Funnel(
        y=[name for _, name in FUNNEL_EVENTS], x=[r["hits"] for r in _funnel_rows],
        marker=dict(color=CATEGORICAL[:len(FUNNEL_EVENTS)]),
        textinfo="value+percent initial",
        hovertemplate="%{y}<br>hits: %{x:,}<extra></extra>"))
    figf.update_layout(title="Quote → application funnel (hits carrying each event)")
    render(figf, height=380)

    # KPI firing timeline
    figt = go.Figure()
    for i, (eid, name) in enumerate(FUNNEL_EVENTS):
        sub = _ev[_ev["eid"] == eid]
        if len(sub):
            figt.add_trace(go.Scatter(x=sub["period"], y=sub["count"], name=name, mode="lines",
                                      line=dict(color=CATEGORICAL[i % len(CATEGORICAL)], width=2),
                                      hovertemplate=f"%{{x|%Y-%m-%d}}<br>{name}: %{{y:,}} hits<extra></extra>"))
    figt.update_layout(title=f"Funnel event firing ({GRANULARITY}) — hits carrying each event",
                       yaxis_title="hits with event", xaxis_title=None)
    render(figt)
else:
    print("post_event_list not present — skipping funnel.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Product & sponsor mix
# MAGIC **Form:** horizontal magnitude bars. Top product categories (eVar4) and sponsors /
# MAGIC distributors / associations (eVar6) — the business dimensions the funnel is sliced by.

# COMMAND ----------

def top_evar(evar_n, title):
    col = next((c for c in (f"post_evar{evar_n}", f"evar{evar_n}") if c in _cols), None)
    if not col:
        print(f"eVar{evar_n} not present — skipping.")
        return
    pdf = (base_df.filter(nonblank(col)).groupBy(col).count()
                  .orderBy(F.desc("count")).limit(TOP_N).toPandas().iloc[::-1])
    share(f"top_evar{evar_n}", {"col": col, "top_n": TOP_N, "rows": _records(pdf)})
    fig = go.Figure(go.Bar(
        x=pdf["count"], y=pdf[col].astype(str), orientation="h", marker=dict(color="#199e70"),
        text=pdf["count"].map(lambda v: f"{v:,}"), textposition="outside",
        hovertemplate="%{y}<br>hits: %{x:,}<extra></extra>"))
    fig.update_layout(title=title, xaxis_title="hits", yaxis_title=None, margin=dict(l=200))
    render(fig, height=max(320, 26 * len(pdf) + 120))

top_evar(4, f"Top {TOP_N} product categories (eVar4)")
top_evar(6, f"Top {TOP_N} sponsors / distributors (eVar6)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Monthly seasonality
# MAGIC **Form:** magnitude bars by month. Reveals the CoverMe volume shape over the year; a
# MAGIC month-over-month detector must expect the recurring pattern and not alarm on it.

# COMMAND ----------

_mo = (base_df.groupBy(F.date_format(local_ts(), "yyyy-MM").alias("month")).count()
              .orderBy("month").toPandas())
share("monthly_volume", {"rows": _records(_mo)})
fig = go.Figure(go.Bar(x=_mo["month"], y=_mo["count"], marker=dict(color="#3987e5"),
                       text=_mo["count"].map(lambda v: f"{v:,}"), textposition="outside",
                       hovertemplate="%{x}<br>hits: %{y:,}<extra></extra>"))
fig.update_layout(title="Monthly hit volume", yaxis_title="hits", xaxis_title=None)
render(fig, height=380)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data manifest — integrity check for the export
# MAGIC Byte length + sha1 of every `chart:<id>` block above, so a truncated `.ipynb` export can be
# MAGIC caught offline (re-hash a block, compare against this manifest).

# COMMAND ----------

_manifest = {}
for _cid, _payload in CHART_DATA.items():
    _body = json.dumps(_payload, separators=(",", ":"), default=str)
    _manifest[_cid] = {"bytes": len(_body), "sha1": hashlib.sha1(_body.encode("utf-8")).hexdigest()}
share("manifest", {"charts": _manifest, "n_charts": len(_manifest)})

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Notes.** Charts are aggregate-only (ADR-0007). Scope is URL-only (single-suite, no rsid),
# MAGIC with the D4 blank-guarded `page_url`-first coalesce. Language is split by domain
# MAGIC (coverme.com=EN, pourmeproteger.com=FR). Palette is the CVD-validated dataviz dark set;
# MAGIC multi-series panels carry a legend + hover + direct labels so identity is never colour-alone.

# COMMAND ----------

base_df.unpersist()
