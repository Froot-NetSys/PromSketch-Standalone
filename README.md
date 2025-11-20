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
   ```
2. Download the CAIDA dataset and use `ExporterStarter/datasets/pcap_process.py` to convert it into `.txt` format.

## PromSketch Standalone Server – Setup and Testing Guide

This repository contains the implementation of **PromSketch**, a sketch-based time series processing server, along with supporting tools for ingestion, query testing, visualization, and performance benchmarking.

---

### Components

* **Main Server**
  Path: `ProsmketchServer/main.go`
  Handles ingestion, sketch storage, and PromQL query execution. Runs on **localhost:7000** by default.

* **Custom Ingester**
  Path: `ExporterStarter/custom_ingester.py`
  Forwards raw data into the PromSketch server through dynamically created multiport ingestion endpoints.

* **Export Manager**
  Path: `ExporterStarter/ExportManager.py`
  Generates synthetic time series data for ingestion tests.

* **PromTools**
  Path: `promtools.py`
  Issues PromQL queries to the PromSketch server at fixed intervals (default: every 5 seconds).

---

### Step-by-Step Running Instructions

#### 1. Start the Export Manager and Custom Ingester

Inside `ExporterStarter/`, run the following to generate and ingest synthetic data:

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

# Start Custom Ingester
python3 custom_ingester.py --config=num_samples_config.yml
```

---

#### 2. Launch the Main PromSketch Server

From `ProsmketchServer/`, run:

```bash
cd ProsmketchServer/

MAX_INGEST_GOROUTINES=n go run .
```

* `MAX_INGEST_GOROUTINES` controls concurrency for ingestion.
* On startup, the server **automatically rewrites `prometheus.yml`** based on the number of multiport ingestion endpoints.

---

#### 3. Start Prometheus

From the Prometheus build directory:

```bash
cd ProsmketchServer/prometheus/

./prometheus --config.file=documentation/examples/prometheus.yml   --enable-feature=remote-write-receiver --web.enable-lifecycle
```

Ensure that the `prometheus.yml` path points to the file rewritten by the server.

---

#### 4. Run PromTools for Query Testing

Run PromTools from `ProsmketchServer/` to continuously send PromQL queries:

```bash
cd ProsmketchServer/

python3 promtools.py
```

Queries such as `avg_over_time`, `entropy_over_time`, and `quantile_over_time` will be executed every 5 seconds.

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



