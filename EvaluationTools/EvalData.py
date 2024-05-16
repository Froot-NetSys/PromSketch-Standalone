import re
import requests
import argparse
import pandas as pd
import sys
import time

df_ts = pd.read_csv("timeseries.csv")
stats_df = pd.DataFrame(
    {
        "Sample_Size": [],
        "Quantile": [],
        "Sum": [],
        "Average": [],
    }
)

pattern = r"(\d+)"
pattern2 = r"(fake_metric_avg|fake_metric_sum|fake_metric_quantile)"
mapping = {
    "fake_metric_avg": "Average",
    "fake_metric_sum": "Sum",
    "fake_metric_quantile": "Quantile",
}


def make_requests(wait_eval):
    res = []
    for i in range(10):
        response = requests.get("http://localhost:9090/api/v1/rules")
        res_json = response.json()
        res_json = res_json["data"]["groups"]
        for group in res_json:
            match = re.search(pattern, group["file"])
            samples = int(match[0])
            row = {"Sample_Size": samples}
            for rule in group["rules"]:
                name = mapping[re.search(pattern2, rule["name"])[0]]
                eval_time = rule["evaluationTime"]
                row[name] = eval_time
            res.append(row)
            print(res)
        time.sleep(wait_eval)

    return res


if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument(
        "--waiteval", type=int, help="time to wait before next eval in seconds"
    )
    parse.add_argument("--targets", type=int, help="number of targets")
    args = parse.parse_args()
    if args.waiteval is None or args.targets is None:
        print("Missing argument --waiteval, --targets")
        sys.exit(0)

    wait_time = args.waiteval
    targets = args.targets
    res = make_requests(wait_time)
    stats_df = pd.concat([stats_df, pd.DataFrame(res)], ignore_index=True)
    agg_table = stats_df.groupby("Sample_Size").agg(['mean', 'std'])

    # file name: <number_of_samples>_<query_type>_<number_of_timeseries>.csv
    agg_table.to_csv(f"{10}_samples_avg_10_ts.csv", index = False)

'''
    avg_row = {"Monitoring_Targets": targets}
    for col in mapping.values():
        avg_row[col] = avgs[col]
    print(avg_row)
    df_ts = pd.concat([df_ts, pd.DataFrame([avg_row])], ignore_index=True)
    stats_df.to_csv(f"targets_{targets}_data.csv", index=False)
    df_ts.to_csv("timeseries.csv", index=False)
'''