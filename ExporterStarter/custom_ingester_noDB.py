import os
import sys
import time
from collections import defaultdict
from io import StringIO
from urllib.parse import urlparse

import requests
import yaml
from prometheus_client.parser import text_fd_to_metric_families

# Configuration for PromSketch
PROMSKETCH_CONTROL_URL = os.environ.get("PROMSKETCH_CONTROL_URL", "http://localhost:7000")
PROMSKETCH_BASE_PORT = int(os.environ.get("PROMSKETCH_BASE_PORT", "7100"))
MACHINES_PER_PORT = int(os.environ.get("PROMSKETCH_MACHINES_PER_PORT", "200"))
METRICS_PER_TARGET_HINT = int(os.environ.get("PROMSKETCH_METRICS_PER_TARGET", "1250"))
PORT_BLOCKLIST = {7000}
SCRAPE_TIMEOUT = int(os.environ.get("PROMSKETCH_SCRAPE_TIMEOUT_SECONDS", "5"))

_CONTROL_HOST = urlparse(PROMSKETCH_CONTROL_URL).hostname or "localhost"

def parse_duration(duration_str):
    if duration_str.endswith('ms'):
        return int(duration_str[:-2]) / 1000  # convert to seconds
    elif duration_str.endswith('s'):
        return int(duration_str[:-1])
    elif duration_str.endswith('m'):
        return int(duration_str[:-1]) * 60
    elif duration_str.endswith('h'):
        return int(duration_str[:-1]) * 3600
    else:
        raise ValueError(f"Unsupported duration format: {duration_str}")

def register_capacity(num_targets: int):
    payload = {
        "num_targets": num_targets,
        "estimated_timeseries": num_targets * METRICS_PER_TARGET_HINT,
        "machines_per_port": MACHINES_PER_PORT,
        "start_port": PROMSKETCH_BASE_PORT,
    }
    try:
        resp = requests.post(f"{PROMSKETCH_CONTROL_URL}/register_config", json=payload, timeout=5)
        resp.raise_for_status()
        print(f"[REGISTER_CONFIG] success status={resp.status_code}")
    except Exception as exc:
        print(f"[REGISTER_CONFIG] warning: {exc}")


def machine_to_port(machineid: str) -> int:
    try:
        idx = int(str(machineid).split("_")[1])
    except Exception:
        idx = 0
    port_index = idx // MACHINES_PER_PORT
    return PROMSKETCH_BASE_PORT + port_index


def ingest_data(config_file):
    try:
        with open(config_file, "r") as f:
            config_data = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Config file '{config_file}' not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML config file '{config_file}': {e}")
        sys.exit(1)

    targets = config_data["scrape_configs"][0]["static_configs"][0]["targets"]
    
    # Removed InfluxDB client initialization
    # client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    # write_api = client.write_api(write_options=SYNCHRONOUS)
    num_targets = len(targets)
    register_capacity(num_targets)
    print(f"Starting data ingestion for targets: {targets}")
    
    # Get scrape_interval from the config file
    scrape_interval_str = config_data["scrape_configs"][0].get("scrape_interval", "10s")
    try:
        interval_seconds = parse_duration(scrape_interval_str)
        if interval_seconds <= 0:
            print(f"Warning: scrape_interval '{scrape_interval_str}' is zero or negative. Setting to 1 second.")
            interval_seconds = 1
    except ValueError as e:
        print(f"Configuration Error: {e}. Defaulting scrape_interval to 10 seconds.")
        interval_seconds = 10

    # Buffer to collect metrics before sending in a batch
    metrics_buffer = []
    last_send_time = time.time()
    # You can adjust the batch interval and size here
    BATCH_SEND_INTERVAL_SECONDS = 0.5  # Send every 0.5 seconds
    MAX_BATCH_SIZE = 500  # Send if buffer reaches 500 metrics

    while True:  # Loop to continuously scrape data
        current_scrape_time = int(time.time() * 1000)  # Timestamp in milliseconds

        for target in targets:
            try:
                # Send HTTP GET request to the exporter
                response = requests.get(f"http://{target}/metrics", timeout=SCRAPE_TIMEOUT)
                response.raise_for_status()  # Handle HTTP errors
                
                # Parse Prometheus metric response
                for family in text_fd_to_metric_families(StringIO(response.text)):
                    for sample in family.samples:
                        # Prepare metric data as expected by the Go server
                        # Go server expects map[string]string for Labels
                        labels_dict = {k: v for k, v in sample.labels.items()}
                        
                        metrics_buffer.append({
                            "Name": sample.name,
                            "Labels": labels_dict,
                            "Value": float(sample.value),
                        })

            except requests.exceptions.ConnectionError as e:
                print(f"Error connecting to exporter {target}: {e}. Make sure fake exporters are running.")
            except Exception as e:
                print(f"An unexpected error occurred for target {target}: {e}")
        
        # Send buffer if it's time or if batch size is reached
        if (time.time() - last_send_time >= BATCH_SEND_INTERVAL_SECONDS) or (len(metrics_buffer) >= MAX_BATCH_SIZE):
            if metrics_buffer:
                buckets = defaultdict(list)
                for metric in metrics_buffer:
                    machineid = metric["Labels"].get("machineid", "machine_0")
                    port = machine_to_port(machineid)
                    if port in PORT_BLOCKLIST:
                        print(f"[SKIP] Blocking ingest for machineid={machineid} resolved to blocked port {port}")
                        continue
                    buckets[port].append(metric)

                for port, items in sorted(buckets.items()):
                    endpoint = f"http://{_CONTROL_HOST}:{port}/ingest"
                    payload = {
                        "Timestamp": current_scrape_time,
                        "Metrics": items,
                    }
                    try:
                        resp = requests.post(endpoint, json=payload, timeout=5)
                        resp.raise_for_status()
                        print(f"Sent {len(items)} metrics to {endpoint}. Response: {resp.status_code}")
                    except requests.exceptions.RequestException as exc:
                        print(f"Error sending data to {endpoint}: {exc}")

                metrics_buffer = []
                last_send_time = time.time()
        
        # Sleep according to scrape_interval
        time.sleep(interval_seconds)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Custom Data Ingester for PromSketch")
    parser.add_argument("--config", type=str, required=True, help="Path to Prometheus config file (e.g., num_samples_config.yml)")
    args = parser.parse_args()

    ingest_data(args.config)
