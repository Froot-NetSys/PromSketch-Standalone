# PromSketch Demo & Server — Usage & Configuration Guide

This document explains how to run the **PromSketch server (Go)**, the **ingester (Python)**, and the **demo (Streamlit)**, lists the **query expressions** used by the demo, and summarizes the **configurable parameters** you can tweak.

---

## 1) Component Overview

* **Main server (Go, port 7000)**: control & query endpoints (`/parse`, `/health`, `/debug-state`, `/register_config`, `/metrics`).
* **Per‑partition servers (ports 71xx)**: spawned automatically after registration; expose `/ingest` (JSON ingest) & `/metrics` (per‑port metrics + RAW exposure).
* **Ingester (Python)**: reads Prometheus‑style `num_samples_config.yml` targets, scrapes each target’s `/metrics`, maps *machineid → port*, then batches POST to `/ingest` on the corresponding 71xx ports.
* **Demo (Streamlit)**: plots PromSketch vs Prometheus results (time‑series charts per expression), latency, and a simple cost model.

High‑level flow: **exporters → ingester → ports 71xx (/ingest) → sketches in server → /parse → demo**.

---

## 2) Prerequisites

* Go ≥ 1.25 (to run the server).
* Python ≥ 3.10 with: `aiohttp`, `pyyaml`, `prometheus_client`, `streamlit`, `pandas`, `requests`.
* (Optional) Prometheus running at `http://localhost:9090` for side‑by‑side comparisons.

---

## 3) Quick Start

### A) Run the Go server (port 7000)

```bash
cd PromsketchServer
# optional: adjust ingest concurrency
export MAX_INGEST_GOROUTINES=1024
# run the server
go run .
```

The server opens control endpoints on **:7000** and pprof on **localhost:6060**. Partition ports **71xx** are created automatically after a `POST /register_config` arrives from the ingester.

### B) Prepare the scrape config (minimal example)

Create `num_samples_config.yml`:

```yaml
scrape_configs:
  - job_name: fake-exporter
    scrape_interval: 1s
    static_configs:
      - targets: ["localhost:8000", "localhost:8001"]
```

> Make sure each target exposes Prometheus metrics and attaches a `machineid` label to samples you intend to forward.

### C) Run the ingester (Python)

```bash
python custom_ingester_noDB_test3_dynamic.py --config num_samples_config.yml
```

Workflow:

1. Read `targets` from YAML.
2. `POST /register_config` to **:7000** with capacity hints (e.g., `estimated_timeseries`) to determine how many **71xx** ports to spawn.
3. Scrape each target’s `/metrics`, parse into samples `(name, labels, value)`.
4. Map **machineid → 71xx port** (ranges are based on `MACHINES_PER_PORT`).
5. Batch POST to `http://localhost:71xx/ingest` at a fixed interval.

### D) (Optional) Run Prometheus for comparison

Run Prometheus on `http://localhost:9090`. You can also let Prometheus scrape the per‑port RAW endpoints `:71xx/metrics` via a `promsketch_raw_groups` job if you want to monitor partition‑level ingest.

### E) Run the demo (Streamlit)

```bash
streamlit run demo.py
```

In the UI you’ll see:

* **Latency** charts (local Prometheus, local PromSketch, server PromSketch).
* **Metric value** charts for each expression.
* **Cost panel** estimated insert/query/storage costs driven by Prometheus & PromSketch counters.

---

## 4) Endpoints & Payloads

### Main server endpoints (:7000)

* `GET /health` → server status.
* `GET /metrics` → server‑level metrics (e.g., total ingested).
* `GET /parse?q=<expr>` → execute a time‑window aggregation on sketches.
* `POST /register_config` (JSON) → create/extend 71xx partition servers.
* `GET /debug-state` → sketch coverage (per machine) & internal state checks.
* `POST /ingest-query-result` → (optional) ingest query results.

### Per‑partition endpoints (ports 71xx)

* `GET /metrics` → per‑port metrics like `promsketch_port_ingested_metrics_total{metric_name, machineid}` plus RAW exposure.
* `POST /ingest` (JSON) → payload:

```json
{
  "Timestamp": 1757000814123,
  "Metrics": [
    {"Name": "fake_machine_metric", "Labels": {"machineid": "machine_0"}, "Value": 123.0},
    {"Name": "fake_machine_metric", "Labels": {"machineid": "machine_1"}, "Value": 456.0}
  ]
}
```

Success response: `{ "status": "success", "ingested_metrics_count": N }`.

### Example manual query call

```bash
# example with a 300s window
echo "$(curl -s 'http://localhost:7000/parse?q=sum_over_time(fake_machine_metric{machineid="machine_0"}[300s])')"
```

> Note: the server may return **202 Accepted** when coverage is not ready yet (the demo handles this). Retry after a short while once ingest is flowing.

---

## 5) Built‑in Query Expressions

General pattern: `func(metric{label="..."}[window])`.

Used by the **demo** (examples show `machineid="machine_0"`, `window=10000s`):

* `quantile_over_time(0.5, ...)`, `quantile_over_time(0.9, ...)`
* `avg_over_time(...)`, `count_over_time(...)`, `sum_over_time(...)`
* `min_over_time(...)`, `max_over_time(...)`
* `entropy_over_time(...)`, `l1_over_time(...)`, `l2_over_time(...)`, `distinct_over_time(...)`
* `stddev_over_time(...)`, `stdvar_over_time(...)`

Example **rules** file for batch queries (optional): `promsketch-rules.yml` also includes `sum2_over_time(...)`.

> **Numeric argument**: for `quantile_over_time(q, ...)`, `q ∈ [0, 1]`.

---

## 6) What You Can Configure

### a) Demo (Streamlit) — `demo.py`

* **Endpoints**:

  * `PROMETHEUS_QUERY_URL` (default `http://localhost:9090/api/v1/query`)
  * `PROMSKETCH_QUERY_URL` (default `http://localhost:7000/parse?q=`)
  * `PROMSKETCH_METRICS_URL` (default `http://localhost:7000/metrics`)
* **UI & buffers**: `REFRESH_SEC`, `HISTORY_LEN`.
* **Query list**: `QUERY_EXPRS` (extend/modify as needed).
* **Cost model**: `INSERT_COST_PER_MILLION`, `QUERY_COST_PER_MILLION`, `STORAGE_COST_PER_GB_HOUR`, `ASSUMED_BYTES_PER_SAMPLE`.
* **Counters feeding the cost panel**: from Prometheus (e.g., `prometheus_tsdb_head_samples_appended_total`, `prometheus_engine_query_samples_total`) and from PromSketch (e.g., total ingested & `sketch_query_samples_total`).

### b) Ingester — `custom_ingester_noDB_test3_dynamic.py`

* **Routing**: `PROMSKETCH_BASE_PORT` (default 7100), `MACHINES_PER_PORT` (default 200), `PORT_BLOCKLIST={7000}` (control port must never receive ingest).
* **Targets & intervals**: defined in `num_samples_config.yml` → `targets` and `scrape_interval` (`ms/s/m/h`).
* **Capacity hinting**: `metrics_per_target` (estimated time‑series per target) influences `estimated_timeseries` during registration (number of 71xx ports).
* **Batching**: `BATCH_SEND_INTERVAL_SECONDS`, `POST_TIMEOUT_SECONDS`, `REGISTER_SLEEP_SECONDS`.

### c) Server (Go) — `main.go`

* **Concurrency**: `MAX_INGEST_GOROUTINES` (env var), default 1024.
* **Port partitioning**: defaults `startPort=7100`, `machinesPerPort=200`; can be overridden via `POST /register_config`.
* **Prometheus auto‑update (optional)**: `UpdatePrometheusYML(path)` can inject a `promsketch_raw_groups` job with active 71xx ports.
* **Logging & CSV**:

  * *Throughput*: `throughput_log.csv` (samples/sec, total samples).
  * *Aggregation debug*: `debug_agg_log.csv`.
  * *Execution & window debug*: `debug_exec_sketches.csv`, `debug_window_counts.csv`.
* **Window alignment**: the server aligns `mint/maxt` with sketch coverage and can compare `count_over_time` (Prometheus vs PromSketch) for debugging.

---

## 7) Tips & Troubleshooting

* Ensure the `machineid` label exists on samples you want to partition; otherwise everything may fall back to a default mapping.
* Never send ingest to **7000** (control); use the spawned **71xx** ports.
* If `GET /parse` frequently returns 202 (coverage not ready), let ingest run a bit longer until the time window is filled.
* Check `:7000/metrics` and each `:71xx/metrics` to verify totals and RAW exposure.

---

## 8) Appendix

### Example `promsketch-rules.yml`

```yaml
rules:
  - name: "Average over time"
    query: avg_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Count over time"
    query: count_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Entropy over time"
    query: entropy_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Max over time"
    query: max_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Min over time"
    query: min_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Standard Deviation over time"
    query: stddev_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Variance over time"
    query: stdvar_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Sum over time"
    query: sum_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Sum squared over time"
    query: sum2_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Distinct over time"
    query: distinct_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "L1 norm over time"
    query: l1_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "L2 norm over time"
    query: l2_over_time(fake_machine_metric{machineid="machine_0"}[10000s])
  - name: "Quantile 0.9 over time"
    query: quantile_over_time(0.9, fake_machine_metric{machineid="machine_0"}[10000s])
```

---

**Done.** Adjust the *Configuration* section to match your experiment, then run the corresponding components.
