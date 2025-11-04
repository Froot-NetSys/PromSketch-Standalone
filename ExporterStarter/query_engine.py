# query_engine.py
from flask import Flask, request, jsonify
from influxdb_client import InfluxDBClient
import time

# InfluxDB client already initialized
INFLUXDB_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUXDB_TOKEN = "plXzdAdmgRhsDAuS8tQksaOvh8ETfqgFLNjr0tuMRIh_p-VVn4uyOhdwE_T0jXHR6jyxUB68Gldj3ZUY0jP84g=="
INFLUXDB_ORG = "research"
INFLUXDB_BUCKET = "promsketch_data"

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

app = Flask(__name__)

# Dummy implementation for illustration, replace with actual sketch logic
def calculate_avg_from_db(metric_name, labels, start_time, end_time):
    # Example InfluxQL or Flux query
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
    |> range(start: {start_time}, stop: {end_time})
    |> filter(fn: (r) => r._measurement == "{metric_name}")
    '''
    for label_key, label_value in labels.items():
        query += f'|> filter(fn: (r) => r.{label_key} == "{label_value}")'
    query += '|> keep(columns: ["_value"])'
    
    tables = query_api.query(query, org=INFLUXDB_ORG)
    values = []
    for table in tables:
        for record in table.records:
            values.append(record.get_value())
    
    if values:
        return sum(values) / len(values)
    return None

@app.route('/query', methods=['GET'])
def handle_query():
    metric_name = request.args.get('metric')
    query_type = request.args.get('querytype') # avg, sum, quantile
    start_time_ms = int(request.args.get('start')) # Timestamp in milliseconds
    end_time_ms = int(request.args.get('end'))   # Timestamp in milliseconds
    
    # Convert ms to seconds or nanoseconds if the DB requires it
    start_time_ns = start_time_ms * 1_000_000
    end_time_ns = end_time_ms * 1_000_000

    # Parse labels from the query string (e.g., label_machineid=machine_0)
    labels = {k.replace('label_', ''): v for k, v in request.args.items() if k.startswith('label_')}

    result = None
    if query_type == 'avg':
        result = calculate_avg_from_db(metric_name, labels, start_time_ns, end_time_ns)
        # TODO: Replace with sketch-backed average computation
    elif query_type == 'sum':
        # TODO: Implement sum using sketches
        pass
    elif query_type == 'quantile':
        # TODO: Implement quantile using sketches
        pass
    
    if result is not None:
        return jsonify({"status": "success", "data": result})
    else:
        return jsonify({"status": "error", "message": "No data or unsupported query type"}), 400

if __name__ == '__main__':
    app.run(port=9090) # Port for the PromSketch query API
