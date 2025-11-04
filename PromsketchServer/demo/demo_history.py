# --- imports & config ---
import time
import requests
import pandas as pd
import streamlit as st
from collections import deque
from math import isfinite
import urllib.parse
import math
import sqlite3  # NEW: SQLite

PROMETHEUS_QUERY_URL = "http://localhost:9090/api/v1/query"
PROMSKETCH_QUERY_URL = "http://localhost:7000/parse?q="

REFRESH_SEC = 2
HISTORY_LEN = 120  # keep 120 points (sliding window)

# --- list of expressions being compared (use the same PromQL on both sides) ---
QUERY_EXPRS = {
    "0.5-Quantile": 'quantile_over_time(0.5, fake_machine_metric{machineid="machine_0"}[10000s])',
    "0.9-Quantile": 'quantile_over_time(0.9, fake_machine_metric{machineid="machine_0"}[10000s])',
    "Avg":          'avg_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "Count":        'count_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "Sum":          'sum_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "Min":          'min_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "Max":          'max_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "Entropy":      'entropy_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "L1 Norm":      'l1_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "L2 Norm":      'l2_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "Distinct":     'distinct_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "StdDev":       'stddev_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
    "Variance":     'stdvar_over_time(fake_machine_metric{machineid="machine_0"}[10000s])',
}

# =========================
# Persistence (SQLite)
# =========================
DB_PATH = "telemetry.sqlite3"

@st.cache_resource
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS values_hist (
        ts TEXT NOT NULL,
        metric TEXT NOT NULL,
        prom REAL,
        sketch REAL
    );
    CREATE INDEX IF NOT EXISTS idx_values_hist_ts ON values_hist(ts);
    CREATE INDEX IF NOT EXISTS idx_values_hist_metric ON values_hist(metric);

    CREATE TABLE IF NOT EXISTS latency_hist (
        ts TEXT NOT NULL,
        prom_local_ms REAL,
        sketch_local_ms REAL,
        sketch_server_ms REAL
    );
    CREATE INDEX IF NOT EXISTS idx_latency_hist_ts ON latency_hist(ts);
    """)
    return conn

def append_value_row(conn, ts_iso: str, metric: str, prom_v: float, sketch_v: float):
    conn.execute(
        "INSERT INTO values_hist(ts, metric, prom, sketch) VALUES (?,?,?,?)",
        (ts_iso, metric, float(prom_v), float(sketch_v)),
    )

def append_latency_row(conn, ts_iso: str,
                       prom_local_ms: float, sketch_local_ms: float, sketch_server_ms: float | None):
    conn.execute(
        "INSERT INTO latency_hist(ts, prom_local_ms, sketch_local_ms, sketch_server_ms) VALUES (?,?,?,?)",
        (
            ts_iso,
            float(prom_local_ms) if math.isfinite(prom_local_ms) else None,
            float(sketch_local_ms) if math.isfinite(sketch_local_ms) else None,
            float(sketch_server_ms) if (sketch_server_ms is not None and math.isfinite(sketch_server_ms)) else None,
        ),
    )

def prune_history(conn, keep_hours=48):
    cutoff = (pd.Timestamp.utcnow() - pd.Timedelta(hours=keep_hours)).isoformat()
    conn.execute("DELETE FROM values_hist  WHERE ts < ?", (cutoff,))
    conn.execute("DELETE FROM latency_hist WHERE ts < ?", (cutoff,))
    conn.commit()

# NEW: preload history from SQLite into the deques so the charts are populated on startup
def preload_history(conn):
    try:
        # per metric
        for name in QUERY_EXPRS.keys():
            rows = pd.read_sql_query(
                "SELECT ts, prom, sketch FROM values_hist WHERE metric=? ORDER BY ts DESC LIMIT ?",
                conn, params=(name, HISTORY_LEN), parse_dates=["ts"]
            )
            if not rows.empty:
                rows = rows.iloc[::-1]  # ascending by time
                for _, r in rows.iterrows():
                    append_point(
                        name,
                        pd.Timestamp(r["ts"]),
                        float(r["prom"]) if pd.notna(r["prom"]) else float("nan"),
                        float(r["sketch"]) if pd.notna(r["sketch"]) else float("nan"),
                    )
        # latency
        Lrows = pd.read_sql_query(
            "SELECT ts, prom_local_ms, sketch_local_ms, sketch_server_ms "
            "FROM latency_hist ORDER BY ts DESC LIMIT ?",
            conn, params=(HISTORY_LEN,), parse_dates=["ts"]
        )
        if not Lrows.empty:
            Lrows = Lrows.iloc[::-1]
            for _, r in Lrows.iterrows():
                append_latency_point(
                    pd.Timestamp(r["ts"]),
                    float(r["prom_local_ms"])  if pd.notna(r["prom_local_ms"])  else float("nan"),
                    float(r["sketch_local_ms"]) if pd.notna(r["sketch_local_ms"]) else float("nan"),
                    float(r["sketch_server_ms"]) if pd.notna(r["sketch_server_ms"]) else None,
                )
    except Exception as e:
        st.warning(f"Failed to preload history: {e}")

# --- helpers ---
def query_prometheus(expr: str):
    """Return (value, local_latency_ms, None)"""
    try:
        start = time.perf_counter()
        r = requests.get(PROMETHEUS_QUERY_URL, params={"query": expr}, timeout=10)
        local_latency_ms = (time.perf_counter() - start) * 1000.0
        j = r.json()
        res = j.get("data", {}).get("result", [])
        if not res:
            return float("nan"), local_latency_ms, None
        v = float(res[0]["value"][1])
        return v, local_latency_ms, None
    except Exception:
        return float("nan"), float("nan"), None

def query_promsketch(expr: str):
    """Return (value, local_latency_ms, server_latency_ms)"""
    try:
        encoded = urllib.parse.quote(expr)
        url = PROMSKETCH_QUERY_URL + encoded
        start = time.perf_counter()
        r = requests.get(url, timeout=10)
        local_latency_ms = (time.perf_counter() - start) * 1000.0
        if r.status_code == 200:
            j = r.json()
            results = j.get("data", [])
            server_latency_ms = j.get("query_latency_ms", None)
            if results:
                first = results[0]
                val = first.get("value")
                ts = first.get("timestamp")
                # optional: display a brief summary for the most recent query
                st.info(
                    f"PromSketch value: {val} @ {ts} | [LOCAL] {local_latency_ms:.2f} ms "
                    f"[SERVER] {server_latency_ms if server_latency_ms is not None else '-'} ms"
                )
                try:
                    return float(val), local_latency_ms, float(server_latency_ms) if server_latency_ms is not None else None
                except Exception:
                    return float("nan"), local_latency_ms, float(server_latency_ms) if server_latency_ms is not None else None
            else:
                st.warning(f"PromSketch: result kosong untuk query: {expr}")
                return float("nan"), local_latency_ms, server_latency_ms if server_latency_ms is not None else None
        elif r.status_code == 202:
            st.warning(f"PromSketch: Sketch not ready yet. {r.json().get('message')}")
            return float("nan"), local_latency_ms, None
        else:
            st.error(f"PromSketch error: {r.text}")
            return float("nan"), local_latency_ms, None
    except Exception as e:
        st.error(f"Gagal query PromSketch: {e}")
        return float("nan"), float("nan"), None

def init_state():
    if "hist" not in st.session_state:
        st.session_state.hist = {}
        for name in QUERY_EXPRS:
            st.session_state.hist[name] = {
                "t": deque(maxlen=HISTORY_LEN),
                "prom": deque(maxlen=HISTORY_LEN),
                "sketch": deque(maxlen=HISTORY_LEN),
            }
    if "latency" not in st.session_state:
        st.session_state.latency = {
            "t": deque(maxlen=HISTORY_LEN),
            "prom_local": deque(maxlen=HISTORY_LEN),
            "sketch_local": deque(maxlen=HISTORY_LEN),
            "sketch_server": deque(maxlen=HISTORY_LEN),
        }

def append_point(name: str, t: pd.Timestamp, prom_v: float, sketch_v: float):
    buf = st.session_state.hist[name]
    buf["t"].append(t)
    buf["prom"].append(prom_v)
    buf["sketch"].append(sketch_v)

def make_dataframe(name: str) -> pd.DataFrame:
    buf = st.session_state.hist[name]
    if not buf["t"]:
        return pd.DataFrame(columns=["Prometheus", "Sketches"])
    df = pd.DataFrame({
        "time": list(buf["t"]),
        "Prometheus": list(buf["prom"]),
        "Sketches": list(buf["sketch"]),
    }).set_index("time")
    return df

def append_latency_point(t: pd.Timestamp, prom_local_ms: float, sketch_local_ms: float, sketch_server_ms: float | None):
    L = st.session_state.latency
    L["t"].append(t)
    L["prom_local"].append(prom_local_ms if isfinite(prom_local_ms) else math.nan)
    L["sketch_local"].append(sketch_local_ms if isfinite(sketch_local_ms) else math.nan)
    # allow None -> NaN so the chart still renders
    L["sketch_server"].append(sketch_server_ms if (sketch_server_ms is not None and isfinite(sketch_server_ms)) else math.nan)

def make_latency_df() -> pd.DataFrame:
    L = st.session_state.latency
    if not L["t"]:
        return pd.DataFrame(columns=["Prometheus local (ms)", "PromSketch local (ms)", "PromSketch server (ms)"])
    df = pd.DataFrame({
        "time": list(L["t"]),
        "Prometheus local (ms)": list(L["prom_local"]),
        "PromSketch local (ms)": list(L["sketch_local"]),
        "PromSketch server (ms)": list(L["sketch_server"]),
    }).set_index("time")
    return df

# --- UI ---
st.set_page_config(layout="wide")
st.title("PromSketch vs. Prometheus")
st.subheader("Live Aggregation Query Results")
st.caption("Both sources run the same PromQL expressions. Lines update on each refresh.")

init_state()

# --- open DB & pruning timer ---
conn = get_db()
if "last_prune_ts" not in st.session_state:
    st.session_state["last_prune_ts"] = pd.Timestamp.utcnow()

# Section: Latency charts
st.markdown("### Query Latency (ms)")
latency_placeholder = st.empty()
st.markdown("---")

# per-metric chart placeholders
cols_per_row = 2
names = list(QUERY_EXPRS.keys())
placeholders = {}

for row_start in range(0, len(names), cols_per_row):
    row = st.columns(cols_per_row)
    for i, name in enumerate(names[row_start:row_start+cols_per_row]):
        with row[i]:
            st.markdown(f"#### {name}")
            placeholders[name] = st.empty()  # populated with line_chart instances

# NEW: preload history & perform an initial render before entering the loop
preload_history(conn)

# initial draw for each metric when history is available
for name in names:
    df_metric_init = make_dataframe(name)
    if not df_metric_init.empty:
        placeholders[name].line_chart(df_metric_init, use_container_width=True)

# initial draw for latency when history is available
df_latency_init = make_latency_df()
if not df_latency_init.empty:
    latency_placeholder.line_chart(df_latency_init, use_container_width=True)

# live update loop
while True:
    now = pd.Timestamp.utcnow()
    # accumulate latency values for this refresh cycle
    prom_local_sum = 0.0
    sketch_local_sum = 0.0
    sketch_server_sum = 0.0
    n_prom = 0
    n_sketch_local = 0
    n_sketch_server = 0

    for name, expr in QUERY_EXPRS.items():
        prom_v, prom_local_ms, _ = query_prometheus(expr)
        sketch_v, sketch_local_ms, sketch_server_ms = query_promsketch(expr)

        # accumulate latency contributions
        if isfinite(prom_local_ms):
            prom_local_sum += prom_local_ms
            n_prom += 1
        if isfinite(sketch_local_ms):
            sketch_local_sum += sketch_local_ms
            n_sketch_local += 1
        if (sketch_server_ms is not None) and isfinite(sketch_server_ms):
            sketch_server_sum += sketch_server_ms
            n_sketch_server += 1

        # store metric history values (for on-page charting)
        append_point(name, now, prom_v, sketch_v)

        # render per-metric chart
        df_metric = make_dataframe(name)
        placeholders[name].line_chart(df_metric, use_container_width=True)

        # persist values to SQLite
        append_value_row(conn, now.isoformat(), name, prom_v, sketch_v)

    # compute average latency for this refresh
    prom_local_avg = (prom_local_sum / n_prom) if n_prom > 0 else float("nan")
    sketch_local_avg = (sketch_local_sum / n_sketch_local) if n_sketch_local > 0 else float("nan")
    sketch_server_avg = (sketch_server_sum / n_sketch_server) if n_sketch_server > 0 else float("nan")

    # persist and render latency chart
    append_latency_point(now, prom_local_avg, sketch_local_avg, sketch_server_avg)
    df_latency = make_latency_df()
    latency_placeholder.line_chart(df_latency, use_container_width=True)

    # persist latency averages + commit + perform periodic pruning
    append_latency_row(conn, now.isoformat(), prom_local_avg, sketch_local_avg, sketch_server_avg)
    conn.commit()

    if (now - st.session_state["last_prune_ts"]).total_seconds() > 300:  # roughly every 5 minutes
        prune_history(conn, keep_hours=48)
        st.session_state["last_prune_ts"] = now

    # refresh interval
    time.sleep(REFRESH_SEC)
