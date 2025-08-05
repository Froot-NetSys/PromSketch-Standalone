import yaml
import requests
import urllib.parse
import time

RULES_FILE = "/mnt/78D8516BD8512920/GARUDA_ACE/FROOT-LAB/promsketch-standalone/PromsketchServer/prometheus/documentation/examples/prometheus-rules.yml"
SERVER_URL = "http://localhost:7000/parse?q="

def load_rules(path):
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data.get("rules", [])


def run_query(query_str):
    encoded = urllib.parse.quote(query_str)
    url = SERVER_URL + encoded
    print(f"\nSending query: {query_str}")
    try:
        start_time = time.time()
        response = requests.get(url)
        latency = time.time() - start_time

        print(f"Query latency: {latency:.3f} seconds")

        if response.status_code == 200:
            json_data = response.json()
            print("Result:", json_data)

            results = json_data.get("data", [])
            for entry in results:
                value = entry.get("value")
                timestamp = entry.get("timestamp")

                # Extract metadata from query string
                func = query_str.split("(")[0]
                metric = query_str.split("(")[1].split("{")[0]
                machineid = query_str.split('machineid="')[1].split('"')[0]
                quantile = "0.00"  # default
                if "_" in func:
                    parts = func.split("_")
                    if parts[0].replace(".", "", 1).isdigit():
                        quantile = parts[0]
                
                push_result_to_server(func, metric, machineid, quantile, value, timestamp)

        elif response.status_code == 202:
            print("[PENDING] Sketch not ready yet. Message:", response.json().get("message"))
        else:
            print("Error:", response.text)
    except Exception as e:
        print(f"Failed to send query: {e}")

def push_result_to_server(func, metric, machineid, quantile, value, timestamp):
    body = {
        "function": func,
        "original_metric": metric,
        "machineid": machineid,
        "quantile": quantile,
        "value": value,
        "timestamp": timestamp,
    }
    try:
        res = requests.post("http://localhost:7000/ingest-query-result", json=body)
        if res.status_code == 200:
            print("[SUCCESS] Pushed result to server")
        else:
            print("[FAIL] Failed to push result:", res.text)
    except Exception as e:
        print(f"[FAIL] Exception while pushing result: {e}")

def main():
    rules = load_rules(RULES_FILE)
    if not rules:
        print("No rules found in rules.yml")
        return

    for rule in rules:
        name = rule.get("name", "Unnamed")
        query = rule.get("query")
        if not query:
            print(f"Skipping rule '{name}': No query specified")
            continue
        print(f"\n=== Running Rule: {name} ===")
        run_query(query)
        time.sleep(1)  # optional delay

if __name__ == "__main__":
    main()
