import math
import os
import signal
import sys
import time
import urllib.parse

import requests
import yaml

RULES_FILE = os.environ.get(
    "PROMSKETCH_RULES_FILE",
    "/mnt/78D8516BD8512920/GARUDA_ACE/FROOT-LAB/promsketch-standalone/PromsketchServer/promsketch-rules.yml",
)
PROMSKETCH_QUERY_URL = os.environ.get("PROMSKETCH_QUERY_URL", "http://localhost:7000/parse?q=")
PROMETHEUS_QUERY_URL = os.environ.get("PROMETHEUS_QUERY_URL", "http://localhost:9090/api/v1/query")
RESULT_PUSH_URL = os.environ.get("PROMSKETCH_RESULT_ENDPOINT", "http://localhost:7000/ingest-query-result")

# Load YAML rule definitions containing named PromQL queries.
def load_rules(path):
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data.get("rules", [])
    
# Query Prometheus directly and return value plus measured client latency.
def query_prometheus(query_str):
    """Return tuple (value, latency_ms, timestamp_str) with NaNs on failure."""
    try:
        start_time = time.perf_counter()
        response = requests.get(
            PROMETHEUS_QUERY_URL,
            params={"query": query_str},
            timeout=10,
        )
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        if response.status_code != 200:
            print(f"[PROMETHEUS] HTTP {response.status_code}: {response.text}")
            return float("nan"), float("nan"), None

        payload = response.json()
        result = payload.get("data", {}).get("result", [])
        if not result:
            print("[PROMETHEUS] Empty result set.")
            return float("nan"), latency_ms, None

        value = float(result[0]["value"][1])
        timestamp = result[0]["value"][0]
        return value, latency_ms, timestamp
    except Exception as exc:
        print(f"[PROMETHEUS] Failed to query: {exc}")
        return float("nan"), float("nan"), None


# Query PromSketch and capture both client-side and server-reported latency.
def query_promsketch(query_str):
    """Return tuple (value, local_latency_ms, server_latency_ms, timestamp_str)."""
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
            if not results:
                print("[PROMSKETCH] Result kosong.")
                return float("nan"), local_latency_ms, server_latency_ms, None

            first = results[0]
            value = first.get("value")
            timestamp = first.get("timestamp")
            return (
                float(value) if value is not None else float("nan"),
                local_latency_ms,
                float(server_latency_ms) if server_latency_ms is not None else None,
                timestamp,
            )

        if response.status_code == 202:
            message = response.json().get("message")
            print(f"[PROMSKETCH] Sketch belum siap: {message}")
            return float("nan"), local_latency_ms, None, None

        print(f"[PROMSKETCH] HTTP {response.status_code}: {response.text}")
        return float("nan"), local_latency_ms, None, None
    except Exception as exc:
        print(f"[PROMSKETCH] Failed to query: {exc}")
        return float("nan"), float("nan"), None, None


# Compare a single query across Prometheus and PromSketch, printing latency/value details.
def run_query(query_str):
    print(f"\n=== Query: {query_str} ===")

    prom_value, prom_latency_ms, prom_ts = query_prometheus(query_str)
    sketch_value, sketch_local_ms, sketch_server_ms, sketch_ts = query_promsketch(query_str)

    if math.isfinite(prom_latency_ms):
        print(f"[PROMETHEUS] Local latency : {prom_latency_ms:.2f} ms")
    if math.isfinite(sketch_local_ms):
        server_part = f"{sketch_server_ms:.2f} ms" if (sketch_server_ms is not None and math.isfinite(sketch_server_ms)) else "-"
        print(f"[PROMSKETCH] Local latency : {sketch_local_ms:.2f} ms | Server latency: {server_part}")

    if math.isfinite(prom_latency_ms) and math.isfinite(sketch_local_ms):
        delta = sketch_local_ms - prom_latency_ms
        print(f"[COMPARE] PromSketch - Prometheus local latency = {delta:+.2f} ms")

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
    )


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
        time.sleep(60)  # update every 5 seconds

if __name__ == "__main__":
    main()
