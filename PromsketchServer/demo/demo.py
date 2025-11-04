# --- imports & config ---
import time
import requests
import pandas as pd
import streamlit as st
from collections import deque
from math import isfinite
import urllib.parse
import math
import re  # <— ADD

PROMETHEUS_QUERY_URL = "http://localhost:9090/api/v1/query"
PROMSKETCH_QUERY_URL = "http://localhost:7000/parse?q="
PROMSKETCH_METRICS_URL = "http://localhost:7000/metrics"  # <— ADD: for totalIngested

REFRESH_SEC = 2
HISTORY_LEN = 120  # 120 (sliding window)


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

# === Testing-friendly COST MODEL ===
INSERT_COST_PER_MILLION = 0.002    # $ per 1M inserts  (~$2 per 1B)
QUERY_COST_PER_MILLION  = 0.001    # $ per 1M scanned  (~$1 per 1B)
STORAGE_COST_PER_GB_HOUR= 0.0005   # $ per GB-hour (~$0.012 per GB-day)
ASSUMED_BYTES_PER_SAMPLE= 8        # 8 bytes/sample

# State for cost tracking
if "cost" not in st.session_state:
    st.session_state.cost = {
        "start_time": time.time(),
        # Prometheus
        "prom_last_total": 0.0,
        "prom_cum": 0.0,
        # PromSketch
        "sketch_last_total": 0.0,
        "sketch_cum": 0.0,
        # accumulator for PromSketch query samples
        "sketch_query_samples_total": 0.0,
    }

def get_insert_cost_testing(samples_insert_total: float) -> float:
    return (samples_insert_total / 1e6) * INSERT_COST_PER_MILLION

def get_query_cost_testing(samples_query_total: float) -> float:
    return (samples_query_total / 1e6) * QUERY_COST_PER_MILLION

def get_storage_cost_testing(samples_insert_total: float, elapsed_hours: float) -> float:
    gb = (samples_insert_total * ASSUMED_BYTES_PER_SAMPLE) / 1e9
    return gb * STORAGE_COST_PER_GB_HOUR * max(0.0, elapsed_hours)

def get_total_cost_from_counters(insert_total: float, query_total: float, elapsed_hours: float) -> float:
    return (
        get_insert_cost_testing(insert_total)
        + get_query_cost_testing(query_total)
        + get_storage_cost_testing(insert_total, elapsed_hours)
    )

def _get_prom_counter(metric: str) -> float:
    """Ambil counter Prometheus dari /api/v1/query."""
    try:
        r = requests.get(PROMETHEUS_QUERY_URL, params={"query": metric}, timeout=10)
        r.raise_for_status()
        j = r.json()
        return float(j["data"]["result"][0]["value"][1])
    except Exception:
        return 0.0

def get_prom_cost_testing():
    """(total, delta) biaya Prometheus dari counter bawaan."""
    elapsed_hours = (time.time() - st.session_state.cost["start_time"]) / 3600.0
    inserts = _get_prom_counter("prometheus_tsdb_head_samples_appended_total")
    scans   = _get_prom_counter("prometheus_engine_query_samples_total")
    total   = get_total_cost_from_counters(inserts, scans, elapsed_hours)
    delta   = max(0.0, total - st.session_state.cost["prom_last_total"])
    st.session_state.cost["prom_last_total"] = total
    st.session_state.cost["prom_cum"] = total
    return total, delta

def get_promsketch_total_ingested() -> float:
    """
    Baca totalIngested dari /metrics PromSketch.
    Coba beberapa nama umum: promsketch_total_ingested, promsketch_samples_ingested_total, totalIngested.
    """
    try:
        resp = requests.get(PROMSKETCH_METRICS_URL, timeout=5)
        text = resp.text
        for name in ("promsketch_total_ingested", "promsketch_samples_ingested_total", "totalIngested"):
            m = re.search(rf"^{name}\s+(\d+(?:\.\d+)?)$", text, flags=re.MULTILINE)
            if m:
                return float(m.group(1))
    except Exception:
        pass
    return 0.0

def get_promsketch_cost_testing():
    """(total, delta) biaya PromSketch: insert dari totalIngested, query dari akumulator exec samples."""
    elapsed_hours = (time.time() - st.session_state.cost["start_time"]) / 3600.0
    inserts = get_promsketch_total_ingested()
    scans   = st.session_state.cost["sketch_query_samples_total"]
    total   = get_total_cost_from_counters(inserts, scans, elapsed_hours)
    delta   = max(0.0, total - st.session_state.cost["sketch_last_total"])
    st.session_state.cost["sketch_last_total"] = total
    st.session_state.cost["sketch_cum"] = total
    return total, delta


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
    """Return (value, local_latency_ms, server_latency_ms, sketch_exec_samples)"""
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
            annotations = j.get("annotations", {}) if isinstance(j, dict) else {}
            sketch_exec_samples = annotations.get("sketch_exec_sample_count", annotations.get("promsketch_sample_count"))

            if results:
                first = results[0]
                val = first.get("value")
                ts = first.get("timestamp")
                # optionally display a short summary for the last query
                st.info(
                    f"PromSketch value: {val} @ {ts} | [LOCAL] {local_latency_ms:.2f} ms "
                    f"[SERVER] {server_latency_ms if server_latency_ms is not None else '-'} ms | "
                    f"samples={sketch_exec_samples if sketch_exec_samples is not None else '-'}"
                )
                try:
                    return float(val), local_latency_ms, (float(server_latency_ms) if server_latency_ms is not None else None), (float(sketch_exec_samples) if sketch_exec_samples is not None else None)
                except Exception:
                    return float("nan"), local_latency_ms, (float(server_latency_ms) if server_latency_ms is not None else None), (float(sketch_exec_samples) if sketch_exec_samples is not None else None)
            else:
                st.warning(f"PromSketch: result kosong untuk query: {expr}")
                return float("nan"), local_latency_ms, (server_latency_ms if server_latency_ms is not None else None), (float(sketch_exec_samples) if sketch_exec_samples is not None else None)
        elif r.status_code == 202:
            st.warning(f"PromSketch: Sketch not ready yet. {r.json().get('message')}")
            return float("nan"), local_latency_ms, None, None
        else:
            st.error(f"PromSketch error: {r.text}")
            return float("nan"), local_latency_ms, None, None
    except Exception as e:
        st.error(f"Gagal query PromSketch: {e}")
        return float("nan"), float("nan"), None, None

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
    if "per_metric_latency" not in st.session_state:
        st.session_state.per_metric_latency = {}
        for name in QUERY_EXPRS:
            st.session_state.per_metric_latency[name] = {
                "prom": deque(maxlen=HISTORY_LEN),
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
st.caption("Using fake_machine_metric from Prometheus' built-in tsdb demo data")

init_state()

# Section: Latency charts
st.markdown("### Query Latency (ms)")
latency_placeholder = st.empty()
latency_summary_placeholder = st.empty()
st.markdown("---")

# Section: Cost (testing model)
st.markdown("### Cost (testing model)")
c1, c2 = st.columns(2)
sketch_cost_placeholder = c1.empty()
prom_cost_placeholder   = c2.empty()
st.markdown("---")

# placeholders for per-metric charts
cols_per_row = 2
names = list(QUERY_EXPRS.keys())
placeholders = {}

for row_start in range(0, len(names), cols_per_row):
    row = st.columns(cols_per_row)
    for i, name in enumerate(names[row_start:row_start+cols_per_row]):
        with row[i]:
            st.markdown(f"#### {name}")
            placeholders[name] = {
                "chart": st.empty(),
                "latency": st.empty(),
            }

def _format_latency(value: float) -> str:
    if value is None:
        return "-"
    try:
        fv = float(value)
    except Exception:
        return "-"
    if math.isnan(fv):
        return "-"
    return f"{fv:.2f}"


def _finite_mean(values: deque[float]) -> float:
    finite_values = []
    for v in values:
        try:
            fv = float(v)
        except Exception:
            continue
        if not math.isnan(fv):
            finite_values.append(fv)
    if not finite_values:
        return float("nan")
    return sum(finite_values) / len(finite_values)

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
        sketch_v, sketch_local_ms, sketch_server_ms, sketch_samples = query_promsketch(expr)

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

        # accumulate PromSketch query samples for cost estimation
        try:
            if sketch_samples is not None and math.isfinite(sketch_samples):
                st.session_state.cost["sketch_query_samples_total"] += float(sketch_samples)
        except Exception:
            pass

        # store metric history samples
        append_point(name, now, prom_v, sketch_v)

        latency_hist = st.session_state.per_metric_latency[name]
        latency_hist["prom"].append(prom_local_ms if isfinite(prom_local_ms) else math.nan)
        latency_hist["sketch_local"].append(sketch_local_ms if isfinite(sketch_local_ms) else math.nan)
        latency_hist["sketch_server"].append(
            sketch_server_ms if (sketch_server_ms is not None and isfinite(sketch_server_ms)) else math.nan
        )

        # render per-metric chart
        df_metric = make_dataframe(name)
        placeholders[name]["chart"].line_chart(df_metric, use_container_width=True)

        prom_avg = _finite_mean(latency_hist["prom"])
        sketch_local_avg = _finite_mean(latency_hist["sketch_local"])
        sketch_server_avg = _finite_mean(latency_hist["sketch_server"])
        placeholders[name]["latency"].markdown(
            f"*Latency avg* — Prometheus: **{_format_latency(prom_avg)} ms**, "
            f"PromSketch local: **{_format_latency(sketch_local_avg)} ms**, "
            f"PromSketch server: **{_format_latency(sketch_server_avg)} ms**"
        )

    # compute average latency for this refresh
    prom_local_avg = (prom_local_sum / n_prom) if n_prom > 0 else float("nan")
    sketch_local_avg = (sketch_local_sum / n_sketch_local) if n_sketch_local > 0 else float("nan")
    sketch_server_avg = (sketch_server_sum / n_sketch_server) if n_sketch_server > 0 else float("nan")

    # persist and render latency chart
    append_latency_point(now, prom_local_avg, sketch_local_avg, sketch_server_avg)
    df_latency = make_latency_df()
    latency_placeholder.line_chart(df_latency, use_container_width=True)
    overall_prom = _finite_mean(st.session_state.latency["prom_local"])
    overall_sketch_local = _finite_mean(st.session_state.latency["sketch_local"])
    overall_sketch_server = _finite_mean(st.session_state.latency["sketch_server"])
    latency_summary_placeholder.markdown(
        f"*Overall latency avg* — Prometheus local: **{_format_latency(overall_prom)} ms**, "
        f"PromSketch local: **{_format_latency(overall_sketch_local)} ms**, "
        f"PromSketch server: **{_format_latency(overall_sketch_server)} ms**"
    )

    # === COST: compute and render ===
    prom_total_cost, prom_delta = get_prom_cost_testing()
    sketch_total_cost, sketch_delta = get_promsketch_cost_testing()

    sketch_cost_placeholder.metric(
        label="PromSketch Cumulative Cost",
        value=f"${sketch_total_cost:,.4f}",
        delta=f"${sketch_delta:.6f} per refresh",
    )
    prom_cost_placeholder.metric(
        label="Prometheus Cumulative Cost",
        value=f"${prom_total_cost:,.4f}",
        delta=f"${prom_delta:.6f} per refresh",
    )

    # interval refresh
    time.sleep(REFRESH_SEC)
