# Standalone PromSketch

This repository provides a standalone PromSketch version, which scrapes samples from Prometheus exporters, caches rule queries as intermediate sketches in [PromSketch](https://github.com/Froot-NetSys/promsketch), and forwards all raw samples to Prometheus for backup. 

## Initial Environment Setup

1. Create and activate a Python virtual environment, then install the required dependencies.
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate

   pip install prometheus-client
   pip install numpy
   pip install pyyaml
   pip install requests
   pip install aiohttp
   pip install pyshark
   ```
2. Download the CAIDA dataset and use `ExporterStarter/datasets/pcap_process.py` to convert it into `.txt` format.
   ```bash
   sudo apt update
   sudo apt install tshark # For converting CAIDA pcap traces
   ```

## PromSketch Standalone Server – Setup and Testing Guide

This repository contains the implementation of standalone **PromSketch**, a sketch-based time series processing server, along with supporting tools for ingestion, query testing, visualization, and performance benchmarking.

---

### Components

* **Main PromSketch Server** (`PromsketchServer/main.go:64`)
  Hosts the control API, sketch storage, and query execution. The server reads env flags during `init` to wire features like ingestion concurrency and remote write forwarding (`PromsketchServer/main.go:153-209`). HTTP handlers for `/ingest` and dynamic port partitions live in the same file (`PromsketchServer/main.go:645-689`).

* **Remote write forwarder** (`PromsketchServer/remote_write.go:25-90`)
  Background worker that converts each ingest payload into Prometheus remote write samples and ships them over HTTP with bounded timeouts and monotonic timestamps. It is wired into the ingest path at `PromsketchServer/main.go:688-689`.

* **Custom Ingester** (`ExporterStarter/custom_ingester.py`)
  Forwards raw data into the PromSketch server through dynamically created multiport ingestion endpoints.

* **Export Manager** (`ExporterStarter/ExportManager.py`)
  Generates synthetic time series data for ingestion tests.

* **PromTools** (`PromsketchServer/promtools.py`)
  Issues PromQL queries to the PromSketch server at fixed intervals (default: every 5 seconds).

### Remote write: enable/disable and where to configure

* Default state: remote write is **enabled** and targets `http://localhost:9090/api/v1/write` (`PromsketchServer/main.go:193-208`). The writer is only created when the endpoint string is non-empty.
* Disable options:
  - Env-only toggle: export `PROMSKETCH_REMOTE_WRITE_ENDPOINT=""` before starting the server to skip forwarding while keeping local sketch ingestion intact.
  - Default-off build: set `remoteWriteEndpoint: ""` in `defaults` inside `PromsketchServer/main.go` (around lines `80-85`), rebuild, and the writer will remain disabled unless you pass a non-empty env var.
* Point to another TSDB/gateway: set `PROMSKETCH_REMOTE_WRITE_ENDPOINT="http://your-host:port/api/v1/write"` and optionally tune `PROMSKETCH_REMOTE_WRITE_TIMEOUT` (Go duration, e.g., `5s`, `1m`) to bound delivery latency (`PromsketchServer/main.go:198-207`).
* Delivery path: payloads accepted by `/ingest` are forwarded asynchronously so ingest latency is unaffected (`PromsketchServer/main.go:645-690`). The background worker that serializes and posts the remote write request lives in `PromsketchServer/remote_write.go:25-91`.
* Safety notes: timestamps are made monotonic per series and backpressure is applied with a bounded queue; check the log prefix `[REMOTE WRITE]` to confirm deliveries (`PromsketchServer/remote_write.go:39-91`).

### Remote write vs scraping ports 71xx

* **Remote write fan-out** pushes every ingested sample to a single remote-write endpoint for long-term/backup TSDBs. It runs asynchronously (no extra ingest latency) and requires the receiver feature on the target Prometheus/TSDB. Best when you need full-fidelity copies of ingest.
* **Scraping ports 71xx** has Prometheus pull `:71xx/metrics` via the `promsketch_raw_groups` job (see `PromsketchServer/prometheus-config/prometheus.yml` and the auto-generated section from `UpdatePrometheusYML` in `PromsketchServer/main.go:214-274`). Those endpoints expose per-partition gauges plus RAW replicas of the latest ingested values (`PromsketchServer/main.go:720-763`), which is ideal for monitoring partition health and quick dashboards.
* **When to choose**: use remote write for durable, sample-for-sample replication; use 71xx scraping to observe partition throughput/last values or when you cannot enable remote write on the TSDB. You can also run both: remote write for storage plus 71xx scraping for ops visibility.

### Configuration files (ingester & Prometheus)

* **Ingestion/scrape config** (`config.yaml` or `num_samples_config.yml`)
  - Location: place alongside the ingester (commonly `ExporterStarter/`) and pass via `--config`.
  - Structure: Prometheus-style `scrape_configs` with `targets`, `scrape_interval`, and labels; ensure every target emits a `machineid` label for sharding.
  - Example:
    ```yaml
    scrape_configs:
      - job_name: fake-exporter
        scrape_interval: 1s
        static_configs:
          - targets: ["localhost:8000", "localhost:8001"]
    ```
  - Purpose: drives `POST /register_config` capacity hints and how many 71xx ingest ports the server spawns.

* **Prometheus config** (`PromsketchServer/prometheus-config/prometheus.yml`)
  - Use this when running Prometheus alongside PromSketch—either to scrape partition RAW endpoints (`promsketch_raw_groups`) or to accept remote write.
  - The server’s `UpdatePrometheusYML` helper rewrites this file with active 71xx ports; keep Prometheus pointed to this path.
  - Rule files in the same folder: `prometheus-rules.yml`, `promsketch-latency.yml`.

* **Prep tips**
  - Ensure `machineid` exists on exporter samples for correct partition routing.
  - Tune `scrape_interval` to workload rate and size `targets` appropriately.

---

### Step-by-Step Running Instructions

#### 1. Launch the Main PromSketch Server

From `PromsketchServer/`, run:

```bash
cd PromsketchServer/

MAX_INGEST_GOROUTINES=n go run .
```

* `MAX_INGEST_GOROUTINES` controls concurrency for ingestion.
* On startup, the server **automatically rewrites `prometheus.yml`** based on the number of multiport ingestion endpoints.
* Control endpoints on **:7000**, pprof on **localhost:6060**, and ingestion partitions on **71xx** appear after the ingester registers. If remote write is on (default `http://localhost:9090/api/v1/write`), each ingest payload is also serialized and posted asynchronously; set `PROMSKETCH_REMOTE_WRITE_ENDPOINT=""` to skip or point at another TSDB.

---

#### 2. Start the Export Manager and Custom Ingester

Inside `ExporterStarter/`, run the following to generate and ingest synthetic data:

In one terminal:
```bash
cd ExporterStarter/

# Start Export Manager
python3 ExportManager.py \
  --config=num_samples_config.yml \
  --targets=8 \
  --timeseries=10000 \
  --max_windowsize=100000 \
  --querytype=entropy \
  --waiteval=60
```

In another terminal: 
```bash
cd ExporterStarter/

# Start Custom Ingester
python3 custom_ingester.py --config=num_samples_config.yml
```

Ingester workflow (see `PromsketchServer/main.go` and `PromsketchServer/README.md` for details):

1. Read `targets` from YAML (`num_samples_config.yml`).
2. `POST /register_config` to **:7000** with capacity hints (e.g., `estimated_timeseries`) to decide how many **71xx** ports to spawn.
3. Scrape each target’s `/metrics`, parse samples `(name, labels, value)`.
4. Map **machineid → 71xx port** using `MACHINES_PER_PORT`.
5. Batch `POST http://localhost:71xx/ingest` on a fixed interval.

---

#### 3. Start Prometheus

From the Prometheus build directory:

```bash
cd PromsketchServer/prometheus/ # download prometheus and compile here

./prometheus --config.file=../prometheus-config/prometheus.yml   --enable-feature=remote-write-receiver --web.enable-lifecycle
```

Ensure that the `prometheus.yml` path points to the file rewritten by the server.
If you enable remote write on PromSketch, keep `--enable-feature=remote-write-receiver` and set `PROMSKETCH_REMOTE_WRITE_ENDPOINT` (for example `http://localhost:9090/api/v1/write`) before starting the Go server.
Prometheus can also scrape partition RAW endpoints `:71xx/metrics` via the `promsketch_raw_groups` job in `prometheus-config/prometheus.yml` to observe per-partition ingest.

To use extended prometheus supporting `l2_over_time`, `entropy_over_time`, and `distinct_over_time`, use:

```bash
cd PromSketch-Standalone/
git submodule update --init --recursive

cd PromsketchServer/external/prometheus-sketch-VLDB/prometheus-extended/prometheus
make build
./prometheus --config.file=../../../../prometheus-config/prometheus.yml   --enable-feature=remote-write-receiver --web.enable-lifecycle
```

---

#### 4. Run PromTools for Query Testing

Run PromTools from `PromsketchServer/` to continuously send PromQL queries:

```bash
cd PromsketchServer/

python3 promtools.py
```

Queries such as `avg_over_time`, `entropy_over_time`, and `quantile_over_time` will be executed every 5 seconds.

---

#### 5. Run the Streamlit demo (optional UI)

From `PromsketchServer/demo/`, start the live dashboard:

```bash
cd PromsketchServer/demo
streamlit run demo.py
```

You’ll see live latency charts (Prometheus vs PromSketch), per-expression metric values, and a cost panel fed by Prometheus & PromSketch counters.

---

#### 5. Visualize with Grafana

* Connect Grafana to your Prometheus instance.
* Create dashboards and panels to display ingested metrics and query outputs.
* Enable **auto-refresh** for live visualization.

---

### Performance Testing

You can benchmark ingestion and query execution as follows:

1. **Ingestion Throughput Test**
   Increase `--targets`, `--timeseries`, or `numClients` in `ExportManager.py` and `custom_ingester.py` to simulate high ingestion rates.

2. **Query Latency Test**
   Use `promtools.py` to measure query response times while ingestion load is active.

3. **System Profiling**
   Enable Go’s built-in `pprof` for CPU and memory profiling:

   ```bash
   go tool pprof http://localhost:7000/debug/pprof/profile?seconds=30
   ```

---

### Notes

* **Multiport ingestion endpoints** handle raw data ingestion and forward metrics directly to Prometheus.
* **Main server (7000)** is responsible for sketch aggregation and query execution. It must be active for queries to run.

---















