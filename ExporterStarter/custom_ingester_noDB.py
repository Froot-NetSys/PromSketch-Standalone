import yaml
import requests
import time
import sys
from prometheus_client.parser import text_fd_to_metric_families
from io import StringIO
import json  # Added json import

# Configuration for your PromSketch Go Server
PROMSKETCH_GO_URL = "http://localhost:7000"
PROMSKETCH_INGEST_ENDPOINT = f"{PROMSKETCH_GO_URL}/ingest"

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
                response = requests.get(f"http://{target}/metrics")
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
            if len(metrics_buffer) > 0:
                payload = {
                    # Use the timestamp when metrics were scraped
                    "Timestamp": current_scrape_time, 
                    "Metrics": metrics_buffer,
                }
                try:
                    # Send POST request to the Go server
                    resp = requests.post(PROMSKETCH_INGEST_ENDPOINT, json=payload, timeout=5)  # Add timeout
                    resp.raise_for_status()  # Handle HTTP errors from Go server
                    print(f"Sent {len(metrics_buffer)} metrics to PromSketch Go server. Response: {resp.status_code}")
                except requests.exceptions.RequestException as e:
                    print(f"Error sending data to PromSketch Go server: {e}")
                
                metrics_buffer = []  # Clear buffer after sending
                last_send_time = time.time()  # Reset last send time
        
        # Sleep according to scrape_interval
        time.sleep(interval_seconds)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Custom Data Ingester for PromSketch")
    parser.add_argument("--config", type=str, required=True, help="Path to Prometheus config file (e.g., num_samples_config.yml)")
    args = parser.parse_args()

    ingest_data(args.config)
