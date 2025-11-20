# Promsketch Prometheus Config

- `promsketch-latency.yml` configures `promsketch_latency` to calculate the median query latency produced by promsketch-standalone.
- To compare that latency to Prometheus Sketches, use the built-in metric `prometheus_engine_query_duration_seconds{quantile="0.5"}`.
`prometheus_engine_query_duration_seconds{quantile="0.9"}`.
`prometheus_engine_query_duration_seconds{quantile="0.99"}`.


