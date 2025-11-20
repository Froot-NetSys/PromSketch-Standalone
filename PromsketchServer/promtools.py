import math
import os
import signal
import sys
import time
import urllib.parse
from collections import defaultdict
import re

import requests
import yaml

RULES_FILE = os.environ.get(
    "PROMSKETCH_RULES_FILE",
    "./promsketch-rules.yml",
)
PROMSKETCH_QUERY_URL = os.environ.get("PROMSKETCH_QUERY_URL", "http://localhost:7000/parse?q=")
PROMETHEUS_QUERY_URL = os.environ.get("PROMETHEUS_QUERY_URL", "http://localhost:9090/api/v1/query")
RESULT_PUSH_URL = os.environ.get("PROMSKETCH_RESULT_ENDPOINT", "http://localhost:7000/ingest-query-result")

PROMETHEUS_SAMPLE_ACCUM = defaultdict(float)
PROMSKETCH_SAMPLE_ACCUM = defaultdict(float)


def _format_sample_value(value):
    if isinstance(value, float):
        if not math.isfinite(value):
            return "nan"
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}"
    return str(value)


def log_sample_load():
    """Print aggregated query sample counts for both backends."""
    prom_total = sum(PROMETHEUS_SAMPLE_ACCUM.values())
    sketch_total = sum(PROMSKETCH_SAMPLE_ACCUM.values())
    print(f"[LOAD] Prometheus raw samples processed total: {_format_sample_value(prom_total)}")
    print(f"[LOAD] PromSketch sketch samples processed total: {_format_sample_value(sketch_total)}")


def _compute_ms_per_thousand(latency_ms, samples):
    if not (latency_ms is not None and math.isfinite(latency_ms)):
        return float("nan")
    if not (samples is not None and math.isfinite(samples)):
        return float("nan")
    base = max(1.0, samples)
    return (latency_ms / base) * 1000.0


# Load YAML rule definitions containing named PromQL queries.
def load_rules(path):
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data.get("rules", [])
    
# Query Prometheus directly and return value plus measured client- and server-side latency.
def query_prometheus(query_str):
    """Return tuple (value, client_latency_ms, internal_latency_ms, samples_processed, timestamp_str, series_count) with NaNs on failure."""
    try:
        start_time = time.perf_counter()
        response = requests.get(
            PROMETHEUS_QUERY_URL,
            params={"query": query_str, "stats": "all"},
            timeout=10,
        )
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        if response.status_code != 200:
            print(f"[PROMETHEUS] HTTP {response.status_code}: {response.text}")
            return float("nan"), float("nan"), float("nan"), float("nan"), None, 0

        payload = response.json()
        result = payload.get("data", {}).get("result", [])
        series_count = len(result)
        if not result:
            print("[PROMETHEUS] Empty result set.")
            return float("nan"), latency_ms, float("nan"), float("nan"), None, series_count

        value = float(result[0]["value"][1])
        timestamp = result[0]["value"][0]
        stats = payload.get("data", {}).get("stats", {})
        internal_latency_ms = float("nan")
        samples_processed = float("nan")
        if stats:
            timings = stats.get("timings", {})
            # evalTotalTime reports server-side evaluation duration in seconds.
            eval_total_time = timings.get("evalTotalTime")
            if eval_total_time is not None:
                internal_latency_ms = float(eval_total_time) * 1000.0
            samples_info = stats.get("samples", {})
            total_samples = samples_info.get("totalQueryableSamples")
            if total_samples is not None:
                try:
                    samples_processed = float(total_samples)
                except (TypeError, ValueError):
                    samples_processed = float("nan")
        return value, latency_ms, internal_latency_ms, samples_processed, timestamp, series_count
    except Exception as exc:
        print(f"[PROMETHEUS] Failed to query: {exc}")
        return float("nan"), float("nan"), float("nan"), float("nan"), None, 0


# Query PromSketch and capture both client-side and server-reported latency.
def query_promsketch(query_str):
    """Return tuple (value, local_latency_ms, server_latency_ms, samples_processed, timestamp_str, series_count)."""
    encoded = urllib.parse.quote(query_str)
    url = PROMSKETCH_QUERY_URL + encoded
    try:
        start_time = time.perf_counter()
        response = requests.get(url, timeout=10)
        local_latency_ms = (time.perf_counter() - start_time) * 1000.0

        if response.status_code == 200:
            payload = response.json()
            server_latency_ms = payload.get("query_latency_ms", None)
            results = payload.get("data", [])
            series_count = len(results)
            # annotations may include sample counts from both Prometheus and PromSketch
            annotations = payload.get("annotations", {}) or {}
            sketch_samples = annotations.get("promsketch_sample_count")
            if sketch_samples is None:
                sketch_samples = annotations.get("sketch_exec_sample_count")
            if not results:
                print("[PROMSKETCH] Result kosong.")
                return (
                    float("nan"),
                    local_latency_ms,
                    float(server_latency_ms) if server_latency_ms is not None else None,
                    float(sketch_samples) if sketch_samples is not None else float("nan"),
                    None,
                    series_count,
                )

            first = results[0]
            value = first.get("value")
            timestamp = first.get("timestamp")
            return (
                float(value) if value is not None else float("nan"),
                local_latency_ms,
                float(server_latency_ms) if server_latency_ms is not None else None,
                float(sketch_samples) if sketch_samples is not None else float("nan"),
                timestamp,
                series_count,
            )

        if response.status_code == 202:
            message = response.json().get("message")
            print(f"[PROMSKETCH] Sketch belum siap: {message}")
            return float("nan"), local_latency_ms, None, float("nan"), None, 0

        print(f"[PROMSKETCH] HTTP {response.status_code}: {response.text}")
        return float("nan"), local_latency_ms, None, float("nan"), None, 0
    except Exception as exc:
        print(f"[PROMSKETCH] Failed to query: {exc}")
        return float("nan"), float("nan"), None, float("nan"), None, 0


# Compare a single query across Prometheus and PromSketch, printing latency/value details.
def run_query(query_str):
    print(f"\n=== Query: {query_str} ===")

    normalized_query = " ".join(query_str.split())
    prom_value, prom_latency_ms, prom_internal_ms, prom_samples, prom_ts, prom_series_count = query_prometheus(query_str)
    if math.isfinite(prom_samples):
        PROMETHEUS_SAMPLE_ACCUM[normalized_query] += prom_samples
    sketch_value, sketch_local_ms, sketch_server_ms, sketch_samples, sketch_ts, sketch_series_count = query_promsketch(query_str)
    if math.isfinite(sketch_samples):
        PROMSKETCH_SAMPLE_ACCUM[normalized_query] += sketch_samples

    if math.isfinite(prom_latency_ms):
        print(f"[PROMETHEUS] Local latency : {prom_latency_ms:.2f} ms")
    if math.isfinite(prom_internal_ms):
        print(f"[PROMETHEUS] Internal latency : {prom_internal_ms:.2f} ms")
    if math.isfinite(prom_samples):
        print(f"[PROMETHEUS] Raw samples processed (stats.totalSamples) : {_format_sample_value(prom_samples)}")
    print(f"[PROMETHEUS] Timeseries matched : {prom_series_count}")

    if math.isfinite(sketch_local_ms):
        print(f"[PROMSKETCH] Local latency : {sketch_local_ms:.2f} ms")
    sketch_backend_ms = float(sketch_server_ms) if (sketch_server_ms is not None and math.isfinite(sketch_server_ms)) else float("nan")
    if math.isfinite(sketch_backend_ms):
        print(f"[PROMSKETCH] Server latency : {sketch_backend_ms:.2f} ms")
    if math.isfinite(sketch_samples):
        print(f"[PROMSKETCH] Sketch samples processed : {_format_sample_value(sketch_samples)}")
    print(f"[PROMSKETCH] Timeseries returned : {sketch_series_count}")
    if math.isfinite(prom_internal_ms) and math.isfinite(sketch_backend_ms):
        backend_delta = sketch_backend_ms - prom_internal_ms
        print(f"[BACKEND DELTA] PromSketch - Prometheus backend latency = {backend_delta:+.2f} ms")

    if math.isfinite(prom_value):
        print(f"[PROMETHEUS] Value = {prom_value} @ {prom_ts}")
    if math.isfinite(sketch_value):
        print(f"[PROMSKETCH] Value = {sketch_value} @ {sketch_ts}")

    func = query_str.split("(")[0]
    metric = query_str.split("(")[1].split("{")[0] if "(" in query_str and "{" in query_str else "unknown_metric"
    machineid = "machine_0"
    quantile = "0.00"

    push_result_to_server(
        func=func,
        metric=metric,
        machineid=machineid,
        quantile=quantile,
        value=sketch_value,
        timestamp=sketch_ts,
        sketch_client_latency_ms=sketch_local_ms,
        sketch_server_latency_ms=sketch_server_ms,
        prometheus_latency_ms=prom_latency_ms,
        prometheus_internal_latency_ms=prom_internal_ms,
        prometheus_samples=prom_samples,
        promsketch_samples=sketch_samples,
        prometheus_series_count=prom_series_count,
        promsketch_series_count=sketch_series_count,
    )

    log_sample_load()


# Forward the result payload back to the PromSketch ingestion endpoint (optional telemetry).
def push_result_to_server(
    func,
    metric,
    machineid,
    quantile,
    value,
    timestamp,
    sketch_client_latency_ms=None,
    sketch_server_latency_ms=None,
    prometheus_latency_ms=None,
    prometheus_internal_latency_ms=None,
    prometheus_samples=None,
    promsketch_samples=None,
    prometheus_series_count=None,
    promsketch_series_count=None,
):
    body = {
        "function": func,
        "original_metric": metric,
        "machineid": machineid,
        "quantile": quantile,
        "value": value,
        "timestamp": timestamp,
    }
    if sketch_client_latency_ms is not None and math.isfinite(sketch_client_latency_ms):
        body["client_latency_ms"] = sketch_client_latency_ms
    if sketch_server_latency_ms is not None and math.isfinite(sketch_server_latency_ms):
        body["server_latency_ms"] = sketch_server_latency_ms
    if prometheus_latency_ms is not None and math.isfinite(prometheus_latency_ms):
        body["prometheus_latency_ms"] = prometheus_latency_ms
    if (
        prometheus_internal_latency_ms is not None
        and math.isfinite(prometheus_internal_latency_ms)
    ):
        body["prometheus_internal_latency_ms"] = prometheus_internal_latency_ms
    if prometheus_samples is not None and math.isfinite(prometheus_samples):
        body["prometheus_samples"] = prometheus_samples
    if promsketch_samples is not None and math.isfinite(promsketch_samples):
        body["promsketch_samples"] = promsketch_samples
    if prometheus_series_count is not None:
        body["prometheus_series_count"] = prometheus_series_count
    if promsketch_series_count is not None:
        body["promsketch_series_count"] = promsketch_series_count
    try:
        requests.post(RESULT_PUSH_URL, json=body, timeout=5)
    except Exception as exc:
        print(f"[WARN] Gagal push hasil ke server: {exc}")

# Handle Ctrl+C so the loop exits cleanly.
def signal_handler(sig, frame):
    print("\n[INFO] Program dihentikan oleh user.")
    sys.exit(0)

# Initialize, iterate through rules, and continuously run comparisons.
def main():
    signal.signal(signal.SIGINT, signal_handler)

    rules = load_rules(RULES_FILE)
    if not rules:
        print("No rules found in rules.yml")
        return

    while True:
        for rule in rules:
            name = rule.get("name", "Unnamed")
            query = rule.get("query")
            if not query:
                print(f"Skipping rule '{name}': No query specified")
                continue
            print(f"\n=== Running Rule: {name} ===")
            run_query(query)
        time.sleep(30)  # update every 60 seconds

if __name__ == "__main__":
    main()
