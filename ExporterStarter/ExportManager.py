# Example usage:
"""
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=1 --windowsize=10 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=10 --windowsize=10 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=100 --windowsize=10 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=2 --timeseries=1000 --windowsize=10 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=20 --timeseries=10000 --windowsize=10 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=200 --timeseries=100000 --windowsize=10 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=2000 --timeseries=1000000 --windowsize=10 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=1 --windowsize=100 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=1 --windowsize=1000 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=1 --windowsize=10000 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=1 --windowsize=100000 --querytype=avg
python ExportManager.py --config=num_samples_config.yml --targets=1 --timeseries=1 --windowsize=1000000 --querytype=avg
"""

import yaml
import argparse
import sys, os, time
import subprocess

ports = []
processes = []
START_PORT = 10000


def define_targets(file, query_type):
    with open(file, "r") as f:
        config_data = yaml.safe_load(f)
    prefix = "localhost:"
    res = []
    for port in ports:
        res.append(prefix + str(port))
    config_data["scrape_configs"][0]["static_configs"][0]["targets"] = res
    config_data["rule_files"] = [f"samples_{query_type}_concurrent.yml"]
    with open(file, "w") as f:
        yaml.dump(config_data, f, default_flow_style=True)


def create_ports(num_targets):
    for i in range(num_targets):
        port = START_PORT + i
        ports.append(port)


def start_prometheus(config, query_type, num_ts):
    f = open(
        f"prometheus_latency_profile_{str(num_ts)}_ts_{query_type}_samples.txt",
        "w",
    )
    process = subprocess.Popen(
        [
            "./prometheus",
            f"--config.file={config}",
            '--rules.max-concurrent-evals=20',
            '--query.max-samples=1000000000',
            '--query.timeout=60m',
        ],
        stdout=f,
        stderr=f,
    )
    processes.append(process)


def start_fake_exporters(ts_batch_size):
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


def start_evaluation_tool(
    num_targets, num_timeseries, waiteval
):
    process = subprocess.call(
        [
            sys.executable,
            "../EvaluationTools/EvalData.py",
            f"--targets={str(num_targets)}",
            f"--timeseries={str(num_timeseries)}",
            f"--waiteval={str(waiteval)}",
        ]
    )
    print("started evaluation tool!")
    processes.append(process)


if __name__ == "__main__":

    os.system("./kill.sh")

    parser = argparse.ArgumentParser(description="process Prometheus config file")
    parser.add_argument("--config", type=str, help="config")
    parser.add_argument(
        "--timeseries", type=int, help="number of timeseries to generate"
    )
    parser.add_argument("--targets", type=int, help="number of fake exporter targets")
    parser.add_argument(
        "--windowsize", type=int, help="number of samples in the query window"
    )
    parser.add_argument("--querytype", type=str, help="query type [avg, sum, quantile]")
    parser.add_argument(
        "--waiteval", type=int, help="seconds to wait between each evaluation"
    )
    parser.add_argument("--max_windowsize", type=int, help="the max window size")
    args = parser.parse_args()

    if args.config is None:
        print("Missing Prometheus configuration file, --config=str ")
        sys.exit(0)

    config_file = args.config
    num_targets = args.targets
    ts_batch_size = int(args.timeseries / num_targets)  # assume it's divisible
    query_type = args.querytype
    window_size = args.max_windowsize

    create_ports(num_targets)
    define_targets(config_file, query_type)

    start_prometheus(config_file, query_type, args.timeseries)
    print("before start fake exporter")
    start_fake_exporters(ts_batch_size)
    
    time.sleep(max(window_size * 0.1 * 1.01, 1800))  # at least sleep 30 min
    start_evaluation_tool(
        num_targets, args.timeseries, args.waiteval
    )
