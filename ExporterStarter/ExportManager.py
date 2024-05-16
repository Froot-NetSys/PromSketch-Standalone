# Example usage: python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=1

import yaml
import argparse
import sys, os
import subprocess

ports = []
processes = []
num_targets = 3
ts_batch_size = 1
START_PORT = 8000


def define_targets(file):
    with open(file, "r") as f:
        config_data = yaml.safe_load(f)
    prefix = "localhost:"
    res = []
    for port in ports:
        res.append(prefix + str(port))
    config_data["scrape_configs"][0]["static_configs"][0]["targets"] = res

    with open(file, "w") as f:
        yaml.dump(config_data, f, default_flow_style=True)


def create_ports(targets):
    for i in range(targets):
        port = START_PORT + i
        ports.append(port)


def start_prometheus(config):
    process = subprocess.Popen(
        [
            "/users/zz_y/prometheus/prometheus",
            f"--config.file={config}",
            "--storage.local.path=/mydata",
        ]
    )
    processes.append(process)


def start_fake_exporters():
    for port in ports:
        starting_val = (port - START_PORT) * ts_batch_size
        process = subprocess.Popen(
            [
                sys.executable,
                "fake_norm_exporter.py",
                f"--port={str(port)}",
                f"--valuescale=10000",
                f"--instancestart={str(starting_val)}",
                f"--batchsize={str(ts_batch_size)}",
            ]
        )
        processes.append(process)

if __name__ == "__main__":

    os.system("pkill -9 prometheus")
    os.system("kill $(ps aux | grep '[p]ython fake_norm_exporter.py' | awk '{print $2}')")

    parser = argparse.ArgumentParser(description="process Prometheus config file")
    parser.add_argument("--config", type=str, help="config")
    parser.add_argument("--timeseries", type=int, help="number of timeseries to generate")
    parser.add_argument("--targets", type=int, help="number of fake exporter targets")
    args = parser.parse_args()
    
    if args.config is None:
        print("Missing Prometheus configuration file, --config=str ")
        sys.exit(0)

    config_file = args.config    
    num_targets = args.targets
    ts_batch_size = int(args.timeseries / num_targets)
    
    create_ports(num_targets)
    define_targets(config_file)
    
    start_prometheus(config_file)
    start_fake_exporters()
