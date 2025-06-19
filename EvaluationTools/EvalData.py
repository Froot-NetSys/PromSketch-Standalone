import re
import requests
import argparse
import pandas as pd
import sys
import time

# Base URL for your PromSketch Go server
PROMSKETCH_GO_URL = "http://localhost:7000"  # Make sure this matches your Go server port

# The make_requests function will be fundamentally modified
def make_requests(targets, num_timeseries, window_size, query_type, wait_eval):
    all_results_data = []  # List to store all results from different queries
    
    # Assumption: timeseries are evenly distributed across targets
    # You need to define the list of timeseries you want to query.
    # Based on fake_norm_exporter.py and ExportManager.py, timeseries are named machine_0, machine_1, etc.
    
    # Create a list of machine IDs to query
    machine_ids_to_query = [f"machine_{i}" for i in range(num_timeseries)]

    print(f"Starting evaluation for {num_timeseries} timeseries across {targets} targets with window size {window_size} and query type {query_type}")

    for _ in range(10):  # Repeat the evaluation 10 times
        current_eval_round_start_time = time.time()
        
        for machine_id in machine_ids_to_query:
            query_metric_name = "fake_machine_metric"  # Metric name from fake_norm_exporter.py
            
            # Query time: assume querying the latest window
            # Prometheus timestamp in milliseconds
            query_end_time_ms = int(time.time() * 1000)
            query_start_time_ms = query_end_time_ms - (window_size * 1000)  # window_size is in seconds, convert to ms

            # Prepare query parameters for the Go server
            params = {
                "func": query_type + "_over_time",  # Example: "avg_over_time"
                "metric": query_metric_name,
                "mint": query_start_time_ms,
                "maxt": query_end_time_ms,
                f"label_machineid": machine_id  # Send machine label
            }
            
            # If the query_type is quantile, add `args` argument
            if query_type == "quantile":
                params["args"] = "0.5"  # Example: 0.5 quantile (median)

            query_url = f"{PROMSKETCH_GO_URL}/query"
            
            # --- DEBUGGING ---
            print(f"DEBUG: Sending query to {query_url} with params: {params}")
            # --- END DEBUGGING ---

            # Record time before the request
            request_start_time = time.perf_counter()
            try:
                response = requests.get(query_url, params=params, timeout=60)  # 60-second timeout
                response.raise_for_status()  # Handle HTTP errors
                
                # Record time after the request
                request_end_time = time.perf_counter()
                evaluation_time = request_end_time - request_start_time  # Query execution time on the PromSketch Go side
                
                res_json = response.json()
                
                if res_json.get("status") == "success":
                    # Query data successful
                    # We no longer parse 'rules' like Prometheus
                    # We only record evaluation time for each query
                    
                    # The number of samples evaluated per query is usually window_size * number_of_timeseries_requested
                    # But in PromSketch, this is an estimate of the samples covered by the sketch.
                    # For performance metrics, we can use window_size as a proxy for Sample_Size
                    
                    row = {
                        "Evaluation_Time": evaluation_time,
                        "Sample_Size": window_size,  # Or adjust to actual sample metrics if available
                        "Timeseries_ID": machine_id,
                        "Query_Type": query_type,
                        "Query_Start_Time_ms": query_start_time_ms,
                        "Query_End_Time_ms": query_end_time_ms,
                        "Num_Results": len(res_json.get("data", [])),
                        "Round_Timestamp": current_eval_round_start_time  # Start timestamp for this evaluation round
                    }
                    all_results_data.append(row)
                else:
                    print(f"Query for {machine_id} failed: {res_json.get('error', 'Unknown error')}")
                    # Still record evaluation time for failed queries as well, optionally with error status
                    row = {
                        "Evaluation_Time": evaluation_time,
                        "Sample_Size": window_size,
                        "Timeseries_ID": machine_id,
                        "Query_Type": query_type,
                        "Query_Start_Time_ms": query_start_time_ms,
                        "Query_End_Time_ms": query_end_time_ms,
                        "Num_Results": 0,
                        "Status": "Failed",
                        "Error_Message": res_json.get('error', 'Unknown error'),
                        "Round_Timestamp": current_eval_round_start_time
                    }
                    all_results_data.append(row)

            except requests.exceptions.RequestException as e:
                # Record evaluation time if the request fails completely (e.g., timeout)
                request_end_time = time.perf_counter()
                evaluation_time = request_end_time - request_start_time
                print(f"Error querying {machine_id}: {e}")
                row = {
                    "Evaluation_Time": evaluation_time,
                    "Sample_Size": window_size,
                    "Timeseries_ID": machine_id,
                    "Query_Type": query_type,
                    "Query_Start_Time_ms": query_start_time_ms,
                    "Query_End_Time_ms": query_end_time_ms,
                    "Num_Results": 0,
                    "Status": "Request Failed",
                    "Error_Message": str(e),
                    "Round_Timestamp": current_eval_round_start_time
                }
                all_results_data.append(row)
            
        time.sleep(wait_eval)  # Delay between evaluation rounds

    return all_results_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", type=int, help="number of targets")
    parser.add_argument("--timeseries", type=int, help="total number of timeseries to query")
    parser.add_argument("--waiteval", type=int, help="time to wait between each evaluation round in seconds")
    parser.add_argument("--windowsize", type=int, help="query window size in seconds (e.g., 10 for 10 seconds)")
    parser.add_argument("--querytype", type=str, help="query type [avg, sum, quantile, entropy]")
    args = parser.parse_args()

    if (args.waiteval is None or 
        args.targets is None or 
        args.timeseries is None or 
        args.windowsize is None or 
        args.querytype is None):
        print("Missing required arguments: --waiteval, --targets, --timeseries, --windowsize, --querytype")
        sys.exit(1)

    # Ensure the querytype is valid
    valid_query_types = ["avg", "sum", "quantile", "entropy"]
    if args.querytype not in valid_query_types:
        print(f"Invalid --querytype. Choose from: {', '.join(valid_query_types)}")
        sys.exit(1)

    wait_time = args.waiteval
    targets = args.targets
    num_timeseries = args.timeseries
    window_size = args.windowsize
    query_type = args.querytype

    print("Starting EvalData.py...")
    print(f"Configuration: Targets={targets}, Timeseries={num_timeseries}, WaitEval={wait_time}s, WindowSize={window_size}s, QueryType={query_type}")

    # Call the make_requests function with provided arguments
    all_raw_data = make_requests(targets, num_timeseries, window_size, query_type, wait_time)
    
    if not all_raw_data:
        print("No data collected from queries. Exiting.")
        sys.exit(0)

    # Create a DataFrame from all collected data
    stats_df = pd.DataFrame(all_raw_data)
    
    # Save raw data
    raw_filename = f"raw_eval_data_ts_{num_timeseries}_ws_{window_size}_qt_{query_type}.csv"
    stats_df.to_csv(raw_filename, index=False)
    print(f"Raw data saved to {raw_filename}")

    # Aggregate data (e.g., average evaluation time per sample size / query type)
    # You can do further aggregation here if needed
    # For example, average Evaluation_Time per Sample_Size and Query_Type
    agg_table = stats_df.groupby(["Sample_Size", "Query_Type"]).agg(
        Evaluation_Time_mean=('Evaluation_Time', 'mean'),
        Evaluation_Time_std=('Evaluation_Time', 'std'),
        Num_Queries=('Evaluation_Time', 'count')  # Count number of queries
    )
    agg_filename = f"aggregated_eval_data_ts_{num_timeseries}_ws_{window_size}_qt_{query_type}.csv"
    agg_table.to_csv(agg_filename)
    print(f"Aggregated data saved to {agg_filename}")

    print("EvalData.py finished.")
