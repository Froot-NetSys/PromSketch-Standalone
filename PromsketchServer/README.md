# PromSketch Server — Configuration & Reference

This document focuses on the server-side configuration, especially where to place the YAML configs and how they connect to `prometheus-config`. Run/workflow guidance and UI walkthroughs now live in the **root README**.

---

## 1) Endpoints & Payloads

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

## 2) Built‑in Query Expressions

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

## 3) What You Can Configure

### a) Demo (Streamlit) — `demo.py`

* **Endpoints**:

  * `PROMETHEUS_QUERY_URL` (default `http://localhost:9090/api/v1/query`)
  * `PROMSKETCH_QUERY_URL` (default `http://localhost:7000/parse?q=`)
  * `PROMSKETCH_METRICS_URL` (default `http://localhost:7000/metrics`)
* **UI & buffers**: `REFRESH_SEC`, `HISTORY_LEN`.
* **Query list**: `QUERY_EXPRS` (extend/modify as needed).
* **Cost model**: `INSERT_COST_PER_MILLION`, `QUERY_COST_PER_MILLION`, `STORAGE_COST_PER_GB_HOUR`, `ASSUMED_BYTES_PER_SAMPLE`.
* **Counters feeding the cost panel**: from Prometheus (e.g., `prometheus_tsdb_head_samples_appended_total`, `prometheus_engine_query_samples_total`) and from PromSketch (e.g., total ingested & `sketch_query_samples_total`).

### b) Ingester — `custom_ingester.py`

* **Routing**: `PROMSKETCH_BASE_PORT` (default 7100), `MACHINES_PER_PORT` (default 200), `PORT_BLOCKLIST={7000}` (control port must never receive ingest).
* **Targets & intervals**: defined in `num_samples_config.yml` → `targets` and `scrape_interval` (`ms/s/m/h`).
* **Capacity hinting**: `metrics_per_target` (estimated time‑series per target) influences `estimated_timeseries` during registration (number of 71xx ports).
* **Batching**: `BATCH_SEND_INTERVAL_SECONDS`, `POST_TIMEOUT_SECONDS`, `REGISTER_SLEEP_SECONDS`.

### c) Server (Go) — `main.go`

* **Concurrency**: `MAX_INGEST_GOROUTINES` (env var), default 1024.
* **Port partitioning**: defaults `startPort=7100`, `machinesPerPort=200`; can be overridden via `POST /register_config`.
* **Prometheus auto‑update (optional)**: `UpdatePrometheusYML(path)` can inject a `promsketch_raw_groups` job with active 71xx ports.
* **Remote write forwarding**: `PROMSKETCH_REMOTE_WRITE_ENDPOINT` (default `http://localhost:9090/api/v1/write`) controls the target; set it to an empty string to skip forwarding. `PROMSKETCH_REMOTE_WRITE_TIMEOUT` accepts Go durations (e.g., `5s`, `1m`) for delivery timeouts. The server deduplicates payloads, enforces monotonic timestamps per series, and pushes samples asynchronously so ingest latency stays unaffected.
* **Logging & CSV**:

  * *Throughput*: `throughput_log.csv` (samples/sec, total samples).
  * *Aggregation debug*: `debug_agg_log.csv`.
  * *Execution & window debug*: `debug_exec_sketches.csv`, `debug_window_counts.csv`.
* **Window alignment**: the server aligns `mint/maxt` with sketch coverage and can compare `count_over_time` (Prometheus vs PromSketch) for debugging.

---

## 4) Tips & Troubleshooting

* Ensure the `machineid` label exists on samples you want to partition; otherwise everything may fall back to a default mapping.
* Never send ingest to **7000** (control); use the spawned **71xx** ports.
* If `GET /parse` frequently returns 202 (coverage not ready), let ingest run a bit longer until the time window is filled.
* Check `:7000/metrics` and each `:71xx/metrics` to verify totals and RAW exposure.

---

## 5) Prometheus configs & rules

* `prometheus-config/prometheus.yml`: baseline Prometheus scrape config used alongside PromSketch. It includes jobs for the main server (`:7000`) and partition RAW endpoints (`promsketch_raw_groups`), plus references to the rule files below. The Go server can rewrite this file via `UpdatePrometheusYML` to inject active 71xx ports.
* `prometheus-config/promsketch-latency.yml`: rule group that records quantiles (p50/p90/p99) over `promsketch_engine_query_duration_seconds` buckets, useful for query latency dashboards.
* `prometheus-config/prometheus-rules.yml`: general Prometheus alert/rule set shipped with this repo; keep it alongside `prometheus.yml` when starting Prometheus.
* `promsketch-rules.yml` (root of `PromsketchServer/`): example batch queries targeting PromSketch-specific functions; you can load or adapt it for your tests.

---

## 6) Appendix

### Example `promsketch-rules.yml` (PromsketchServer/promsketch-rules.yml)

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
