import asyncio
import os
import sys
import time
from collections import defaultdict
from io import StringIO
from urllib.parse import urlparse

import aiohttp
import yaml
from prometheus_client.parser import text_fd_to_metric_families

def _parse_port_blocklist(raw: str) -> set[int]:
    ports: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            ports.add(int(token))
        except ValueError:
            print(f"[WARN] Ignoring invalid port in PROMSKETCH_PORT_BLOCKLIST: {token}")
    return ports


PROMSKETCH_CONTROL_URL = os.environ.get("PROMSKETCH_CONTROL_URL", "http://localhost:7000")
PROMSKETCH_BASE_PORT = int(os.environ.get("PROMSKETCH_BASE_PORT", "7100"))
MACHINES_PER_PORT = int(os.environ.get("PROMSKETCH_MACHINES_PER_PORT", "200"))
METRICS_PER_TARGET_HINT = int(os.environ.get("PROMSKETCH_METRICS_PER_TARGET", "1250"))
PORT_BLOCKLIST = _parse_port_blocklist(os.environ.get("PROMSKETCH_PORT_BLOCKLIST", "7000"))
BATCH_SEND_INTERVAL_SECONDS = float(os.environ.get("PROMSKETCH_BATCH_INTERVAL_SECONDS", "0.5"))
POST_TIMEOUT_SECONDS = float(os.environ.get("PROMSKETCH_POST_TIMEOUT_SECONDS", "8"))
REGISTER_SLEEP_SECONDS = float(os.environ.get("PROMSKETCH_REGISTER_SLEEP_SECONDS", "0.5"))
SCRAPE_TIMEOUT_SECONDS = float(os.environ.get("PROMSKETCH_SCRAPE_TIMEOUT_SECONDS", "5"))

_CONTROL_HOST = urlparse(PROMSKETCH_CONTROL_URL).hostname or "localhost"

total_sent = 0
start_time = time.time()

# Map machineid → port using MACHINES_PER_PORT ranges
def machine_to_port(machineid: str) -> int:
    # machineid format: "machine_0", "machine_1", ..., "machine_199"
    try:
        idx = int(str(machineid).split("_")[1])
    except Exception:
        idx = 0
    port_index = idx // MACHINES_PER_PORT
    return PROMSKETCH_BASE_PORT + port_index

# Read num_samples_config.yml, estimate total time series, then register with the main server at :7000/register_config
async def register_capacity(config_data):
    targets = config_data["scrape_configs"][0]["static_configs"][0]["targets"]
    num_targets = len(targets)
    total_ts_est = num_targets * METRICS_PER_TARGET_HINT
    payload = {
        "num_targets": num_targets,
        "estimated_timeseries": total_ts_est,
        "machines_per_port": MACHINES_PER_PORT,
        "start_port": PROMSKETCH_BASE_PORT,
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{PROMSKETCH_CONTROL_URL}/register_config",
                                    json=payload, timeout=5) as resp:
                print("[REGISTER_CONFIG]", resp.status)
        except Exception as e:
            print("[REGISTER_CONFIG ERROR]", e)

# Fetch metrics from targets listed in num_samples_config.yml via HTTP and parse Prometheus text format into sample lists
async def fetch_metrics(session, target):
    metrics = []
    try:
        async with session.get(f"http://{target}/metrics", timeout=SCRAPE_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                print(f"[ERROR] Failed to scrape {target}: {response.status}")
                return metrics
            text = await response.text()
            for family in text_fd_to_metric_families(StringIO(text)):
                for sample in family.samples:
                    labels_dict = dict(sample.labels)
                    metrics.append({
                        "Name": sample.name,
                        "Labels": labels_dict,
                        "Value": float(sample.value),
                    })
    except Exception as e:
        print(f"[ERROR] Scraping {target} failed: {e}")
    return metrics

async def log_speed():
    global total_sent
    while True:
        elapsed = time.time() - start_time
        if elapsed > 0:
            print(f"[INGEST SPEED] Sent {total_sent} samples in {elapsed:.2f}s "
                  f"= {total_sent/elapsed:.2f} samples/sec")
        await asyncio.sleep(5)

def parse_duration(duration_str):
    if duration_str.endswith('ms'):
        return int(duration_str[:-2]) / 1000
    if duration_str.endswith('s'):
        return int(duration_str[:-1])
    if duration_str.endswith('m'):
        return int(duration_str[:-1]) * 60
    if duration_str.endswith('h'):
        return int(duration_str[:-1]) * 3600
    raise ValueError(f"Unsupported duration format: {duration_str}")

async def post_with_retry(session, url, payload, retries=2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            async with session.post(url, json=payload, timeout=POST_TIMEOUT_SECONDS) as resp:
                text = await resp.text()
                return resp.status, text
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.2 * (attempt + 1))
    raise last_err

# ... remainder of the earlier code stays the same ...

async def ingest_loop(config_file):
    global total_sent

    try:
        with open(config_file, "r") as f:
            config_data = yaml.safe_load(f)
        await register_capacity(config_data)
        await asyncio.sleep(REGISTER_SLEEP_SECONDS)  # <— NEW: give 71xx ports time to start
    except Exception as e:
        print(f"Error loading config file: {e}")
        sys.exit(1)

    targets = config_data["scrape_configs"][0]["static_configs"][0]["targets"]
    num_targets = len(targets)
    # MAX_BATCH_SIZE = num_targets * METRICS_PER_TARGET_HINT

    print(f"[CONFIG] metrics_per_target_hint = {METRICS_PER_TARGET_HINT}")
    print(f"[ROUTING] BASE_PORT={PROMSKETCH_BASE_PORT} MACHINES_PER_PORT={MACHINES_PER_PORT}")

    scrape_interval_str = config_data["scrape_configs"][0].get("scrape_interval", "10s")
    try:
        interval_seconds = parse_duration(scrape_interval_str)
        if interval_seconds <= 0:
            interval_seconds = 1
    except Exception as e:
        print(f"Invalid scrape_interval: {e}")
        interval_seconds = 1

    asyncio.create_task(log_speed())

    async with aiohttp.ClientSession() as session:
        while True:
            current_scrape_time = int(time.time() * 1000)
            tasks = [fetch_metrics(session, target) for target in targets]
            results = await asyncio.gather(*tasks)

            metrics_buffer = []
            for metric_list in results:
                metrics_buffer.extend(metric_list)

            # Immediately send every metric fetched during this interval
            if metrics_buffer:
                buckets = defaultdict(list)
                for m in metrics_buffer:
                    mid = m["Labels"].get("machineid", "machine_0")
                    port = machine_to_port(mid)
                    if port in PORT_BLOCKLIST:
                        print(f"[SKIP] machineid={mid} resolved to blocked port {port}")
                        continue
                    buckets[port].append(m)

                for port, items in sorted(buckets.items()):
                    url = f"http://{_CONTROL_HOST}:{port}/ingest"
                    payload = {"Timestamp": current_scrape_time, "Metrics": items}
                    try:
                        status, body = await post_with_retry(session, url, payload, retries=2)
                        if status == 200:
                            total_sent += len(items)
                            print(f"[SEND OK] {len(items)} → {url}")
                        else:
                            print(f"[SEND ERR {status}] {url} → {body[:200]}")
                    except Exception as e:
                        print(f"[SEND EXC] {url}: {e}")

            await asyncio.sleep(interval_seconds)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=True, help="Path to Prometheus config file")
    args = parser.parse_args()
    asyncio.run(ingest_loop(args.config))
