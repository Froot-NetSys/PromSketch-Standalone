import yaml
import requests
import time
from prometheus_client.parser import text_fd_to_metric_families
from io import StringIO

# Asumsi Anda menggunakan InfluxDB, instal: pip install influxdb-client
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

# Konfigurasi InfluxDB (ganti dengan kredensial Anda)
INFLUXDB_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUXDB_TOKEN = "plXzdAdmgRhsDAuS8tQksaOvh8ETfqgFLNjr0tuMRIh_p-VVn4uyOhdwE_T0jXHR6jyxUB68Gldj3ZUY0jP84g=="
INFLUXDB_ORG = "research"
INFLUXDB_BUCKET = "promsketch_data"

def parse_duration(duration_str):
    if duration_str.endswith('ms'):
        return int(duration_str[:-2]) / 1000  # konversi ke detik
    elif duration_str.endswith('s'):
        return int(duration_str[:-1])
    elif duration_str.endswith('m'):
        return int(duration_str[:-1]) * 60
    elif duration_str.endswith('h'):
        return int(duration_str[:-1]) * 3600
    else:
        raise ValueError(f"Unsupported duration format: {duration_str}")

def ingest_data(config_file):
    with open(config_file, "r") as f:
        config_data = yaml.safe_load(f)

    targets = config_data["scrape_configs"][0]["static_configs"][0]["targets"]
    
    # Inisialisasi InfluxDB Client
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    print(f"Starting data ingestion for targets: {targets}")
    
    while True: # Loop untuk terus mengikis data
        for target in targets:
            try:
                # Lakukan permintaan HTTP GET ke exporter
                response = requests.get(f"http://{target}/metrics")
                response.raise_for_status() # Tangani error HTTP
                
                # Parsing respons metrik Prometheus
                for family in text_fd_to_metric_families(StringIO(response.text)):
                    for sample in family.samples:
                        metric_name = sample.name
                        labels = sample.labels
                        value = sample.value
                        
                        # Buat Point untuk InfluxDB
                        point = Point(metric_name)
                        for label_key, label_value in labels.items():
                            point.tag(label_key, label_value)
                        point.field("value", float(value))
                        point.time(time.time_ns()) # Gunakan nanoseconds untuk presisi tinggi

                        # Tulis Point ke InfluxDB
                        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
                        # print(f"Ingested: {metric_name} {labels} {value} at {point.time}")

            except requests.exceptions.ConnectionError as e:
                print(f"Error connecting to exporter {target}: {e}")
            except Exception as e:
                print(f"An unexpected error occurred for target {target}: {e}")
        
        # Jeda sesuai scrape_interval (diambil dari config file atau diatur secara default)
        scrape_interval_str = config_data["scrape_configs"][0].get("scrape_interval", "10s")
        interval_seconds = parse_duration(scrape_interval_str)
        time.sleep(interval_seconds)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Custom Data Ingester for PromSketch")
    parser.add_argument("--config", type=str, required=True, help="Path to Prometheus config file (e.g., num_samples_config.yml)")
    args = parser.parse_args()

    ingest_data(args.config)