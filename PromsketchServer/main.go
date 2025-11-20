package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	_ "net/http/pprof"

	"github.com/Froot-NetSys/Promsketch-Standalone/promsketch"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/model/labels"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/promql/parser"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/util/annotations"
)

type IngestPayload struct {
	Timestamp int64           `json:"timestamp"`
	Metrics   []MetricPayload `json:"metrics"`
}

type MetricPayload struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

type queryResultPayload struct {
	Rule                      string   `json:"rule"`
	RuleFile                  string   `json:"rule_file"`
	Function                  string   `json:"function"`
	OriginalMetric            string   `json:"original_metric"`
	MachineID                 string   `json:"machineid"`
	Quantile                  string   `json:"quantile"`
	Value                     float64  `json:"value"`
	Timestamp                 int64    `json:"timestamp"`
	SketchClientLatencyMS     *float64 `json:"client_latency_ms"`
	SketchServerLatencyMS     *float64 `json:"server_latency_ms"`
	PrometheusLatencyMS       *float64 `json:"prometheus_latency_ms"`
	PrometheusInternalLatency *float64 `json:"prometheus_internal_latency_ms"`
}

const (
	machineIDLabel = "machineid"
)

type serverConfig struct {
	disableInsert bool
	disableQuery  bool
	remoteWriter  *remoteWriter
}

type serverDefaults struct {
	disableInsert       bool
	disableQuery        bool
	remoteWriteEndpoint string
	remoteWriteTimeout  time.Duration
}

var ps *promsketch.PromSketches
var cloudEndpoint = os.Getenv("FORWARD_ENDPOINT")
var cfg serverConfig
var defaults = serverDefaults{
	disableInsert:       false,
	disableQuery:        false,
	remoteWriteEndpoint: "http://localhost:9090/api/v1/write",
	remoteWriteTimeout:  5 * time.Second,
}

type ingestionSnapshot struct {
	Rate        float64   `json:"rate_per_sec"`
	AvgRate     float64   `json:"avg_rate_per_sec"`
	Total       int64     `json:"total_ingested"`
	Samples     int64     `json:"samples_in_interval"`
	IntervalSec float64   `json:"interval_seconds"`
	Timestamp   time.Time `json:"timestamp"`
}

var (
	ingestedMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "promsketch_ingested_metrics_total",
			Help: "Total number of ingested metrics",
		},
		[]string{"metric", "machineid"},
	)

	queryResults = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "promsketch_query_result",
			Help: "Result of PromSketch queries",
		},
		[]string{"function", "original_metric", "machineid", "quantile"},
	)

	ruleLatencyGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "promsketch_rule_latency_ms",
			Help: "Latest observed query latency (ms) per rule/backend as reported by promtools",
		},
		[]string{"backend", "rule", "rule_file", "function", "original_metric", "machineid"},
	)
)
var (
	queryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "promsketch_query_duration_seconds",
			Help:    "Total PromSketch query handler latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"func", "aggregate", "status"},
	)

	queryEvalDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "promsketch_eval_duration_seconds",
			Help:    "Time spent inside ps.Eval/EvalAggregate in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"func", "aggregate", "status"},
	)

	queryStageDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "promsketch_engine_query_duration_seconds",
			Help:    "Breakdown of PromSketch query latency by stage, like Prometheus slices.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"slice", "func", "aggregate", "status"},
	)
)

var maxIngestGoroutines int
var lastIngestionStats atomic.Value

func init() {
	ps = promsketch.NewPromSketches()
	log.Println("PromSketches instance initialized")
	val := os.Getenv("MAX_INGEST_GOROUTINES")
	if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
		maxIngestGoroutines = parsed
	} else {
		maxIngestGoroutines = 1024 // default fallback
	}

	prometheus.MustRegister(ingestedMetrics)
	prometheus.MustRegister(queryResults)
	prometheus.MustRegister(ruleLatencyGauge)
	lastIngestionStats.Store(ingestionSnapshot{})

	cfg.disableInsert = defaults.disableInsert
	envDisableInsert, hasEnvDisableInsert := readBoolEnv("PROMSKETCH_DISABLE_INSERT")
	if hasEnvDisableInsert {
		cfg.disableInsert = envDisableInsert
	}
	if cfg.disableInsert {
		source := "defaults"
		if hasEnvDisableInsert {
			source = "PROMSKETCH_DISABLE_INSERT"
		}
		log.Printf("[CONFIG] Sketch insertion disabled (%s)", source)
	}

	cfg.disableQuery = defaults.disableQuery
	envDisableQuery, hasEnvDisableQuery := readBoolEnv("PROMSKETCH_DISABLE_QUERY")
	if hasEnvDisableQuery {
		cfg.disableQuery = envDisableQuery
	}
	if cfg.disableQuery {
		source := "defaults"
		if hasEnvDisableQuery {
			source = "PROMSKETCH_DISABLE_QUERY"
		}
		log.Printf("[CONFIG] Query endpoint disabled (%s)", source)
	}
	endpoint := strings.TrimSpace(defaults.remoteWriteEndpoint)
	if envEndpoint := strings.TrimSpace(os.Getenv("PROMSKETCH_REMOTE_WRITE_ENDPOINT")); envEndpoint != "" {
		endpoint = envEndpoint
	}
	if endpoint != "" {
		timeout := defaults.remoteWriteTimeout
		if raw := strings.TrimSpace(os.Getenv("PROMSKETCH_REMOTE_WRITE_TIMEOUT")); raw != "" {
			if parsed, err := time.ParseDuration(raw); err == nil {
				timeout = parsed
			} else {
				log.Printf("[CONFIG] Failed to parse PROMSKETCH_REMOTE_WRITE_TIMEOUT (%s): %v. Using default %s", raw, err, timeout)
			}
		}
		cfg.remoteWriter = newRemoteWriter(endpoint, timeout)
		log.Printf("[REMOTE WRITE] Enabled forwarding to %s (timeout=%s)", endpoint, timeout)
	}
}

const (
	defaultStartPort         = 7100
	defaultMachinesPerPort   = 200
	prometheusConfigPath     = "./prometheus/documentation/examples/prometheus.yml"
	promConfigUpdateInterval = 10 * time.Second
)

var (
	portServers          []*PortServer
	portMutex            sync.Mutex
	startPort            = defaultStartPort
	machinesPerPort      = defaultMachinesPerPort
	throughputAvgWindow  = readIntEnv("PROMSKETCH_THROUGHPUT_AVG_WINDOW", 5)
	lastPromConfigUpdate time.Time
)

type PortServer struct {
	Port     int
	Registry *prometheus.Registry
	Metrics  *prometheus.GaugeVec
	Raw      map[string]*prometheus.GaugeVec // raw per-port metrics exposed for scraping
	RawMux   sync.Mutex
}

func NewPortServer(port int) *PortServer {
	reg := prometheus.NewRegistry()
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "promsketch_port_ingested_metrics_total",
			Help: "Total ingested metrics per-port (partition).",
		},
		[]string{"metric_name", "machineid"},
	)
	reg.MustRegister(gauge)

	return &PortServer{
		Port:     port,
		Registry: reg,
		Metrics:  gauge,
		Raw:      make(map[string]*prometheus.GaugeVec),
	}
}

// func (ps *PortServer) Start() {
// 	mux := http.NewServeMux()
// 	mux.Handle("/metrics", promhttp.HandlerFor(ps.Registry, promhttp.HandlerOpts{}))
// 	addr := fmt.Sprintf(":%d", ps.Port)
// 	go func() {
// 		log.Printf("[PORT SERVER] Serving /metrics on port %d", ps.Port)
// 		log.Fatal(http.ListenAndServe(addr, mux))
// 	}()
// }

type RegisterPayload struct {
	NumTargets          int `json:"num_targets"`
	EstimatedTimeseries int `json:"estimated_timeseries"`
	MachinesPerPort     int `json:"machines_per_port"`
	StartPort           int `json:"start_port"`
}

// handleRegisterConfig (/register_config) provisions per-port ingest servers.
func handleRegisterConfig(c *gin.Context) {
	var rp RegisterPayload
	if err := c.ShouldBindJSON(&rp); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad payload"})
		return
	}
	if rp.MachinesPerPort > 0 {
		machinesPerPort = rp.MachinesPerPort
	}
	if rp.StartPort > 0 {
		startPort = rp.StartPort
	}

	needPorts := 1
	if rp.EstimatedTimeseries > 0 && machinesPerPort > 0 {
		needPorts = (rp.EstimatedTimeseries + machinesPerPort - 1) / machinesPerPort
	}

	portMutex.Lock()
	for len(portServers) < needPorts {
		p := startPort + len(portServers)
		srv := NewPortServer(p)
		srv.Start() // serves /ingest + /metrics on the port
		portServers = append(portServers, srv)
		log.Printf("[PORT SERVER] Initialized /metrics and /ingest on port %d", p)
	}
	ports := len(portServers)
	portMutex.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"status":            "ok",
		"ports":             ports,
		"machines_per_port": machinesPerPort,
		"start_port":        startPort,
	})
}
func (psv *PortServer) Start() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(psv.Registry, promhttp.HandlerOpts{}))

	// Register the per-port /ingest handler that forwards payloads locally.
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload IngestPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		log.Printf("[DEBUG] /ingest from %s on port=%d metrics=%d",
			r.RemoteAddr, psv.Port, len(payload.Metrics))
		if n, err := processIngest(payload); err != nil {
			http.Error(w, "ingest failed", http.StatusInternalServerError)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, fmt.Sprintf(`{"status":"success","ingested_metrics_count":%d}`, n))
		}
	})

	addr := fmt.Sprintf(":%d", psv.Port)
	go func() {
		log.Printf("[PORT SERVER] Serving /metrics and /ingest on port %d", psv.Port)
		log.Fatal(http.ListenAndServe(addr, mux))
	}()
}

func getOrCreatePortIndex(machineID string) int {
	numStr := strings.TrimPrefix(machineID, "machine_")
	idx, err := strconv.Atoi(numStr)
	if err != nil {
		log.Printf("[PORT ERROR] Failed to parse machine index from: %s", machineID)
		return 0 // fallback
	}

	portIndex := idx / machinesPerPort

	// Ensure portServers[portIndex] is available
	portMutex.Lock()
	defer portMutex.Unlock()

	for len(portServers) <= portIndex {
		port := startPort + len(portServers)
		server := NewPortServer(port)
		server.Start()
		portServers = append(portServers, server)
		log.Printf("[PORT SERVER] Initialized /metrics and /ingest on port %d", port)
	}

	return portIndex
}

// getOrCreateRaw ensures RAW metrics are registered in the port registry
func (psv *PortServer) getOrCreateRaw(name string, labelKeys []string) *prometheus.GaugeVec {
	psv.RawMux.Lock()
	defer psv.RawMux.Unlock()

	if g, ok := psv.Raw[name]; ok {
		return g
	}
	g := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name, // e.g. "fake_machine_metric" from exporter
			Help: fmt.Sprintf("Raw metric %s ingested on partition port %d", name, psv.Port),
		},
		labelKeys,
	)
	psv.Registry.MustRegister(g)
	psv.Raw[name] = g
	return g
}

// ============================
// Generate prometheus config
// ============================
const scrapeSectionTemplate = `  - job_name: "promsketch_raw_groups"
	static_configs:
	  - targets:
{{- range .Ports }}
		  - "localhost:{{ . }}"
{{- end }}
`

type PromConfigData struct {
	Ports []int
}

func UpdatePrometheusYML(path string) error {
	var ports []int
	for _, ps := range portServers {
		ports = append(ports, ps.Port)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	lines := strings.Split(string(data), "\n")
	start, end := -1, -1
	inScrapeConfigs := false

	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "scrape_configs:") {
			inScrapeConfigs = true
		}
		if inScrapeConfigs && strings.Contains(line, "job_name: \"promsketch_raw_groups\"") {
			start = i
		}
		if start != -1 && i > start && strings.HasPrefix(line, "  - job_name:") {
			end = i
			break
		}
	}
	if start != -1 && end == -1 {
		end = len(lines)
	}

	var buf bytes.Buffer
	tmpl := template.Must(template.New("scrape").Parse(scrapeSectionTemplate))
	err = tmpl.Execute(&buf, PromConfigData{Ports: ports})
	if err != nil {
		return err
	}

	scrapeSection := strings.Split(buf.String(), "\n")

	var newLines []string
	if start != -1 {
		// Update existing scrape_configs section
		newLines = append(lines[:start], scrapeSection...)
		newLines = append(newLines, lines[end:]...)
	} else {
		// Add to the end of scrape_configs
		inserted := false
		for i, line := range lines {
			newLines = append(newLines, line)
			if strings.HasPrefix(strings.TrimSpace(line), "scrape_configs:") && !inserted {
				inserted = true
				for j := i + 1; j < len(lines); j++ {
					if strings.HasPrefix(lines[j], "  - job_name:") || strings.TrimSpace(lines[j]) == "" {
						continue
					}
					newLines = append(newLines, scrapeSection...)
					break
				}
			}
		}
		if !inserted {
			newLines = append(newLines, scrapeSection...)
		}
	}

	return os.WriteFile(path, []byte(strings.Join(newLines, "\n")), 0644)
}

func readBoolEnv(key string) (bool, bool) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return false, false
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return false, false
	}
	return parsed, true
}

func readIntEnv(key string, def int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	parsed, err := strconv.Atoi(val)
	if err != nil || parsed <= 0 {
		return def
	}
	return parsed
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	go logIngestionRate()

	router := gin.Default()
	router.POST("/ingest", handleIngest)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	// router.GET("/throughput_test", runStressThroughputTest)
	router.GET("/parse", handleParse)
	router.POST("/ingest-query-result", handleQueryResultIngest)
	router.GET("/debug-state", handleDebugState)
	router.POST("/register_config", handleRegisterConfig)
	router.GET("/ingest_stats", handleIngestStats)

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP", "message": "PromSketch Go server is running."})
	})

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
		// http.Handle("/metrics", promhttp.Handler())
		// log.Fatal(http.ListenAndServe(":7000", nil))
	}()

	log.Printf("PromSketch Go server listening on :7000")
	if err := router.Run(":7000"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}

	ymlPath := "./prometheus/documentation/examples/prometheus.yml"
	if err := UpdatePrometheusYML(ymlPath); err != nil {
		log.Printf("Failed to update Prometheus config: %v", err)
	} else {
		log.Printf("Updated Prometheus config at %s", ymlPath)
	}
}

func handleDebugState(c *gin.Context) {
	log.Println("[DEBUG] Starting state check...")
	foundCount := 0
	detail := []string{}

	for i := 0; i < 10000; i++ {
		machineID := fmt.Sprintf("machine_%d", i)
		lsetBuilder := labels.NewBuilder(labels.Labels{})
		lsetBuilder.Set("machineid", machineID)
		// Adjust the metric name and function below to inspect a different sketch.
		lsetBuilder.Set(labels.MetricName, "fake_machine_metric")
		lset := lsetBuilder.Labels()

		minTime, maxTime := ps.PrintCoverage(lset, "avg_over_time")

		if minTime != -1 {
			foundCount++
			log.Printf("[DEBUG] Data found for %s | Coverage: %d -> %d", machineID, minTime, maxTime)
			detail = append(detail, fmt.Sprintf("machine_%d: %d → %d", i, minTime, maxTime))
		}
	}

	if foundCount == 0 {
		log.Println("[DEBUG] State check finished. NO active sketches found.")
		c.JSON(http.StatusOK, gin.H{
			"status":  "state check finished",
			"message": "No active sketches found.",
		})
	} else {
		log.Printf("[DEBUG] State check finished. Found %d active sketches.", foundCount)
		c.JSON(http.StatusOK, gin.H{
			"status":          "state check finished",
			"found_sketches":  foundCount,
			"sketch_coverage": detail,
		})
	}
}

func debugAggregationResult(fname string, lset map[string]string, mint, maxt int64, result float64, count float64) {
	// log.Printf("[DEBUG-AGG] function=%s labels=%v mint=%d maxt=%d result=%f count=%f", fname, lset, mint, maxt, result, count)
	// Also write to CSV (optional)
	f, err := os.OpenFile("debug_agg_log.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		w := csv.NewWriter(f)
		defer func() { w.Flush(); f.Close() }()
		_ = w.Write([]string{
			time.Now().Format(time.RFC3339),
			fname,
			fmt.Sprintf("%v", lset),
			fmt.Sprintf("%d", mint),
			fmt.Sprintf("%d", maxt),
			fmt.Sprintf("%f", result),
			fmt.Sprintf("%f", count),
		})
	}
}
func debugExecSamples(fname string, lset labels.Labels, mint, maxt, curTime int64, sampleCount float64) {
	log.Printf("[DEBUG-EXEC] function=%s metric=%s machineid=%s mint=%d maxt=%d cur=%d sample_count=%.0f",
		fname, lset.Get("__name__"), lset.Get("machineid"), mint, maxt, curTime, sampleCount)

	f, err := os.OpenFile("debug_exec_sketches.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		w := csv.NewWriter(f)
		_ = w.Write([]string{
			time.Now().Format(time.RFC3339),
			fname,
			lset.Get("__name__"),
			lset.Get("machineid"),
			fmt.Sprintf("%d", mint),
			fmt.Sprintf("%d", maxt),
			fmt.Sprintf("%d", curTime),
			fmt.Sprintf("%.0f", sampleCount),
		})
		w.Flush()
		_ = f.Close()
	}
}
func debugWindowCounts(
	fname string,
	lset labels.Labels,
	promMint, promMaxt, pskMint, pskMaxt int64,
	promCount, pskCount float64,
) {
	log.Printf("[DEBUG COUNT] function=%s metric=%s machineid=%s | PROMETHEUS mint=%d maxt=%d count=%.0f | PROMSKETCH mint=%d maxt=%d count=%.0f",
		fname, lset.Get("__name__"), lset.Get("machineid"),
		promMint, promMaxt, promCount, pskMint, pskMaxt, pskCount,
	)

	f, err := os.OpenFile("debug_window_counts.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		// Write the header if the file is still empty
		if info, _ := f.Stat(); info.Size() == 0 {
			_, _ = f.WriteString("timestamp,function,metric,machineid,prom_mint,prom_maxt,prom_count,psk_mint,psk_maxt,psk_count\n")
		}
		w := csv.NewWriter(f)
		_ = w.Write([]string{
			time.Now().Format(time.RFC3339),
			fname,
			lset.Get("__name__"),
			lset.Get("machineid"),
			fmt.Sprintf("%d", promMint),
			fmt.Sprintf("%d", promMaxt),
			fmt.Sprintf("%.0f", promCount),
			fmt.Sprintf("%d", pskMint),
			fmt.Sprintf("%d", pskMaxt),
			fmt.Sprintf("%.0f", pskCount),
		})
		w.Flush()
		_ = f.Close()
	}
}

var totalIngested int64

// handleIngest accepts JSON payloads from exporters and triggers local/remote ingestion.
func handleIngest(c *gin.Context) {
	var payload IngestPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid JSON payload: %v", err.Error())})
		return
	}

	log.Printf("[DEBUG] ingest request: %d metrics", len(payload.Metrics))
	for _, m := range payload.Metrics {
		log.Printf("[DEBUG] metric=%s labels=%v value=%v", m.Name, m.Labels, m.Value)
	}

	_, _ = processIngest(payload)
	// Automatically refresh prometheus.yml if enough time has passed
	now := time.Now()
	if now.Sub(lastPromConfigUpdate) > promConfigUpdateInterval {
		if err := UpdatePrometheusYML(prometheusConfigPath); err != nil {
			log.Printf("Failed to update Prometheus config: %v", err)
		} else {
			log.Printf("Updated Prometheus config at %s", prometheusConfigPath)
			lastPromConfigUpdate = now
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": len(payload.Metrics)})
}

// processIngest fans out ingestion work while respecting max goroutine limits.
func processIngest(payload IngestPayload) (int, error) {
	log.Printf("[DEBUG] processIngest: %d metrics in this request", len(payload.Metrics))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxIngestGoroutines)

	for _, metric := range payload.Metrics {
		wg.Add(1)
		sem <- struct{}{}
		go func(metric MetricPayload) {
			defer wg.Done()
			defer func() { <-sem }()
			ingestMetricSample(metric, payload.Timestamp)
		}(metric)
	}
	wg.Wait()
	if cfg.remoteWriter != nil {
		cfg.remoteWriter.Forward(payload)
	}
	return len(payload.Metrics), nil
}

// ingestMetricSample inserts a single metric into sketches and records telemetry.
func ingestMetricSample(metric MetricPayload, timestamp int64) {
	if !cfg.disableInsert {
		if err := ps.SketchInsert(buildLabelSet(metric), timestamp, metric.Value); err != nil {
			return
		}
	}
	recordIngestionTelemetry(metric)
}

// buildLabelSet converts metric labels into a Prometheus labels.Labels set.
func buildLabelSet(metric MetricPayload) labels.Labels {
	builder := labels.NewBuilder(labels.Labels{})
	for key, val := range metric.Labels {
		builder.Set(key, val)
	}
	builder.Set(labels.MetricName, metric.Name)
	return builder.Labels()
}

// recordIngestionTelemetry updates global counters and per-port gauges.
func recordIngestionTelemetry(metric MetricPayload) {
	newTotal := atomic.AddInt64(&totalIngested, 1)

	// DEBUG: this will fire for EVERY ingested sample
	log.Printf("[DEBUG] sample ingested metric=%s machineid=%s total=%d value=%v",
		metric.Name,
		metric.Labels[machineIDLabel],
		newTotal,
		metric.Value,
	)

	machineID := metric.Labels["machineid"]
	ingestedMetrics.WithLabelValues(metric.Name, machineID).Inc()
	updatePerPortMetrics(metric, machineID)
}

// updatePerPortMetrics surfaces the latest value for a machine on its assigned ingest port.
func updatePerPortMetrics(metric MetricPayload, machineID string) {
	if machineID == "" {
		return
	}
	portIndex := getOrCreatePortIndex(machineID)
	if portIndex >= len(portServers) {
		return
	}

	portServers[portIndex].Metrics.WithLabelValues(metric.Name, machineID).Set(metric.Value)
	labelKeys, labelVals := sortedLabelPairs(metric.Labels)

	rawGauge := portServers[portIndex].getOrCreateRaw(metric.Name, labelKeys)
	rawGauge.WithLabelValues(labelVals...).Set(metric.Value)
}

// sortedLabelPairs returns deterministic key/value slices for use with GaugeVec.
func sortedLabelPairs(labelMap map[string]string) ([]string, []string) {
	keys := make([]string, 0, len(labelMap))
	for key := range labelMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	values := make([]string, len(keys))
	for i, key := range keys {
		values[i] = labelMap[key]
	}
	return keys, values
}

// handleQueryResultIngest receives query results forwarded by promtools.py.
func handleQueryResultIngest(c *gin.Context) {
	var result queryResultPayload
	if err := c.ShouldBindJSON(&result); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	queryResults.WithLabelValues(result.Function, result.OriginalMetric, result.MachineID, result.Quantile).Set(result.Value)

	recordRuleLatencyMetric("promsketch_client", result.SketchClientLatencyMS, &result)
	recordRuleLatencyMetric("promsketch_server", result.SketchServerLatencyMS, &result)
	recordRuleLatencyMetric("prometheus_client", result.PrometheusLatencyMS, &result)
	recordRuleLatencyMetric("prometheus_internal", result.PrometheusInternalLatency, &result)

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func recordRuleLatencyMetric(backend string, value *float64, payload *queryResultPayload) {
	if value == nil {
		return
	}

	val := *value
	if math.IsNaN(val) {
		return
	}

	ruleName := strings.TrimSpace(payload.Rule)
	if ruleName == "" {
		ruleName = fmt.Sprintf("%s:%s", payload.Function, payload.OriginalMetric)
	}

	ruleFile := strings.TrimSpace(payload.RuleFile)
	if ruleFile == "" {
		ruleFile = "unknown"
	}

	ruleLatencyGauge.WithLabelValues(
		backend,
		ruleName,
		ruleFile,
		payload.Function,
		payload.OriginalMetric,
		payload.MachineID,
	).Set(val)
}

func logIngestionRate() {
	var lastTotal int64 = 0
	lastTime := time.Now()
	if throughputAvgWindow < 1 {
		throughputAvgWindow = 1
	}
	rateWindow := make([]float64, 0, throughputAvgWindow)

	// Prepare the CSV log file for throughput metrics.
	file, err := os.OpenFile("throughput_log.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open throughput log file: %v", err)
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	if fileInfo.Size() == 0 {
		file.WriteString("timestamp,samples_per_sec,total_samples\n")
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		current := atomic.LoadInt64(&totalIngested)
		now := time.Now()
		elapsed := now.Sub(lastTime).Seconds()
		if elapsed <= 0 {
			continue
		}
		samples := current - lastTotal
		rate := float64(samples) / elapsed
		rateWindow = append(rateWindow, rate)
		if len(rateWindow) > throughputAvgWindow {
			rateWindow = rateWindow[1:]
		}
		var avgRate float64
		for _, r := range rateWindow {
			avgRate += r
		}
		if len(rateWindow) > 0 {
			avgRate /= float64(len(rateWindow))
		}
		timestamp := now.Format(time.RFC3339)

		log.Printf(
			"[INGEST STATS] interval=%.2fs samples=%d inst_rate=%.2f/sec avg_rate=%.2f/sec total=%d",
			elapsed, samples, rate, avgRate, current,
		)
		lastIngestionStats.Store(ingestionSnapshot{
			Rate:        rate,
			AvgRate:     avgRate,
			Total:       current,
			Samples:     samples,
			IntervalSec: elapsed,
			Timestamp:   now,
		})

		// Save to CSV file
		entry := fmt.Sprintf("%s,%.2f,%d\n", timestamp, rate, current)
		if _, err := file.WriteString(entry); err != nil {
			log.Printf("[CSV LOG ERROR] %v", err)
		}

		lastTotal = current
		lastTime = now
	}
}

// handleIngestStats exposes rolling ingest metrics for dashboards or CLI tools.
func handleIngestStats(c *gin.Context) {
	raw := lastIngestionStats.Load()
	stats, _ := raw.(ingestionSnapshot)
	total := atomic.LoadInt64(&totalIngested)
	stats.Total = total
	c.JSON(http.StatusOK, gin.H{
		"total_ingested":      stats.Total,
		"rate_per_sec":        stats.Rate,
		"avg_rate_per_sec":    stats.AvgRate,
		"samples_in_interval": stats.Samples,
		"interval_seconds":    stats.IntervalSec,
		"timestamp_ms":        stats.Timestamp.UnixMilli(),
		"timestamp_rfc3339":   stats.Timestamp.Format(time.RFC3339),
	})
}

// handleParse validates PromQL-style range queries and executes them via sketches.
func handleParse(c *gin.Context) {
	if cfg.disableQuery {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "query endpoint disabled"})
		return
	}

	// === GLOBAL QUERY LATENCY (end-to-end, seperti sebelumnya) ===
	start := time.Now()
	queryFuncLabel := "unknown"
	aggLabel := "unknown"
	statusLabel := "error" // assume error until success

	defer func() {
		dur := time.Since(start).Seconds()
		queryDuration.WithLabelValues(queryFuncLabel, aggLabel, statusLabel).Observe(dur)
	}()

	query := c.Query("q")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing query parameter 'q'"})
		return
	}
	log.Printf("[QUERY ENGINE] Received query: %s", query)

	expr, err := parser.ParseExpr(query)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Parse error: %v", err)})
		return
	}

	call, ok := expr.(*parser.Call)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Query must be a function call (e.g., avg_over_time(...))"})
		return
	}

	funcName := call.Func.Name
	queryFuncLabel = funcName
	otherArgs := 0.0

	matrixSelectorArg := call.Args[len(call.Args)-1]
	rangeArg, ok := matrixSelectorArg.(*parser.MatrixSelector)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "The last argument must be a range vector (e.g., metric[60000ms])"})
		return
	}

	if funcName == "quantile_over_time" {
		if len(call.Args) < 2 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "quantile_over_time requires two arguments: a number and a range vector"})
			return
		}
		firstArg, ok := call.Args[0].(*parser.NumberLiteral)
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "First argument of quantile_over_time must be a number"})
			return
		}
		if firstArg.Val < 0 || firstArg.Val > 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Quantile must be between 0 and 1"})
			return
		}
		otherArgs = firstArg.Val
		log.Printf("[QUERY ENGINE] quantile_over_time arg: quantile=%.4f", otherArgs)
	} else if len(call.Args) > 1 {
		firstArg, ok := call.Args[0].(*parser.NumberLiteral)
		if ok {
			otherArgs = firstArg.Val
		}
	}

	vs, ok := rangeArg.VectorSelector.(*parser.VectorSelector)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse vector selector inside the range vector"})
		return
	}
	metricName := vs.Name
	labelMap := make(map[string]string)
	for _, matcher := range vs.LabelMatchers {
		if matcher.Type == labels.MatchEqual {
			labelMap[matcher.Name] = matcher.Value
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Only '=' label matchers are supported"})
			return
		}
	}

	machineID, hasMachineID := labelMap[machineIDLabel]
	aggregateAllMachines := !hasMachineID || machineID == ""
	if aggregateAllMachines {
		aggLabel = "aggregate"
	} else {
		aggLabel = "single"
	}

	lsetBuilder := labels.NewBuilder(labels.Labels{})
	for k, v := range labelMap {
		lsetBuilder.Set(k, v)
	}
	lsetBuilder.Set(labels.MetricName, metricName)
	lset := lsetBuilder.Labels()

	log.Printf("[QUERY ENGINE] Parsed components: func=%s, lset=%v, otherArgs=%.2f", funcName, lset, otherArgs)

	// === STAGE 1: PREPARE TIME (parse + coverage + window alignment + sample count) ===
	prepareStart := time.Now()

	// Calculate the requested Prometheus window: [now-range, now].
	requestedPromRange := rangeArg.Range
	maxt := time.Now().UnixMilli()
	mint := maxt - requestedPromRange.Milliseconds()

	var sketchMin, sketchMax int64
	if aggregateAllMachines {
		sketchMin, sketchMax = ps.PrintAggregateCoverage(lset, funcName, machineIDLabel)
	} else {
		sketchMin, sketchMax = ps.PrintCoverage(lset, funcName)
	}
	log.Printf("[SKETCH COVERAGE] Sketch coverage: min=%d max=%d | requested mint=%d maxt=%d", sketchMin, sketchMax, mint, maxt)

	// If there's no data in sketch cache, provision it and ask client to retry.
	if sketchMin == -1 || sketchMax == -1 {
		if aggregateAllMachines {
			log.Printf("[QUERY ENGINE] No aggregate coverage yet for %v. Ensuring per-machine sketches.", lset)
			ps.EnsureAggregateSketches(lset, funcName, requestedPromRange.Milliseconds(), 100000, 10000.0, machineIDLabel)
		} else {
			log.Printf("[QUERY ENGINE] No sketch coverage available yet for %v. Creating instance.", lset)
			ps.NewSketchCacheInstance(lset, funcName, requestedPromRange.Milliseconds(), 100000, 10000.0)
		}

		c.JSON(http.StatusAccepted, gin.H{
			"status":  "pending",
			"message": "Sketch data is being prepared. Please try again in a few moments.",
		})
		return
	}

	// Store the Prometheus window before evaluating the sketch
	promMaxt := maxt
	promMint := promMaxt - requestedPromRange.Milliseconds()

	// Align sketch evaluation range with the Prometheus request.
	mint = promMint
	maxt = promMaxt
	log.Printf("[DEBUG WINDOW] Prometheus requested window: mint=%d maxt=%d (duration=%d ms)", promMint, promMaxt, requestedPromRange.Milliseconds())
	log.Printf("[DEBUG WINDOW] PromSketch final window:     mint=%d maxt=%d (duration=%d ms)", mint, maxt, maxt-mint)

	// Track sample counts for both the Prometheus and PromSketch windows.
	var promCount float64 = math.NaN()
	if aggregateAllMachines {
		if cntVec, _ := ps.EvalAggregate("count_over_time", lset, 0.0, promMint, promMaxt, promMaxt, machineIDLabel); len(cntVec) > 0 && !math.IsNaN(cntVec[0].F) {
			promCount = cntVec[0].F
		}
	} else if cntVec, _ := ps.Eval("count_over_time", lset, 0.0, promMint, promMaxt, promMaxt); len(cntVec) > 0 && !math.IsNaN(cntVec[0].F) {
		promCount = cntVec[0].F
	}

	var pskCount float64 = math.NaN()
	if aggregateAllMachines {
		if cntVec, _ := ps.EvalAggregate("count_over_time", lset, 0.0, mint, maxt, maxt, machineIDLabel); len(cntVec) > 0 && !math.IsNaN(cntVec[0].F) {
			pskCount = cntVec[0].F
		}
	} else if cntVec, _ := ps.Eval("count_over_time", lset, 0.0, mint, maxt, maxt); len(cntVec) > 0 && !math.IsNaN(cntVec[0].F) {
		pskCount = cntVec[0].F
	}

	// Log to console and CSV for later inspection.
	debugWindowCounts(funcName, lset, promMint, promMaxt, mint, maxt, promCount, pskCount)

	// Sanity-check the adjusted window boundaries.
	if maxt <= mint {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "failed",
			"message": "Query time range is outside of sketch data coverage.",
		})
		return
	}

	// END STAGE 1: record prepare_time
	prepareDur := time.Since(prepareStart).Seconds()
	queryStageDuration.WithLabelValues("prepare_time", queryFuncLabel, aggLabel, "success").Observe(prepareDur)

	curTime := maxt
	log.Printf("[QUERY ENGINE] Final adjusted range: mint=%d maxt=%d", mint, maxt)

	// === STAGE 2: INNER EVAL (ps.Eval / ps.EvalAggregate) ===
	startEval := time.Now()
	var vector promsketch.Vector
	var annotations annotations.Annotations
	if aggregateAllMachines {
		vector, annotations = ps.EvalAggregate(funcName, lset, otherArgs, mint, maxt, curTime, machineIDLabel)
	} else {
		vector, annotations = ps.Eval(funcName, lset, otherArgs, mint, maxt, curTime)
	}

	evalLatency := time.Since(startEval)

	// Eval-only latency metric (histogram khusus eval)
	queryEvalDuration.WithLabelValues(queryFuncLabel, aggLabel, "success").Observe(evalLatency.Seconds())
	// Breakdown ala Prometheus slice="inner_eval"
	queryStageDuration.WithLabelValues("inner_eval", queryFuncLabel, aggLabel, "success").Observe(evalLatency.Seconds())

	log.Printf("[QUERY ENGINE] ps.Eval latency: %s (%.2f ms)", evalLatency, float64(evalLatency.Microseconds())/1000.0)

	// === STAGE 3: RESULT BUILDING / SORT / ENCODE ===
	resultStart := time.Now()

	results := []map[string]interface{}{}
	resultMachineLabel := machineID
	if aggregateAllMachines {
		resultMachineLabel = "global"
	}
	for _, sample := range vector {
		if !math.IsNaN(sample.F) {
			// Fix the timestamp if it was never set (zero)
			timestamp := sample.T
			if timestamp == 0 {
				timestamp = curTime
			}

			results = append(results, map[string]interface{}{
				"timestamp": timestamp,
				"value":     sample.F,
			})

			queryResults.WithLabelValues(
				funcName,
				lset.Get("__name__"),
				resultMachineLabel,
				fmt.Sprintf("%.2f", otherArgs),
			).Set(sample.F)
		}
	}

	needsExecCount := funcName == "l1_over_time" || funcName == "l2_over_time" ||
		funcName == "distinct_over_time" || funcName == "entropy_over_time"

	var execCount float64 = math.NaN()
	if needsExecCount {
		// Run a second pass: count_over_time(...) on the final PromSketch window
		if aggregateAllMachines {
			if cntVec, _ := ps.EvalAggregate("count_over_time", lset, 0.0, mint, maxt, curTime, machineIDLabel); len(cntVec) > 0 && !math.IsNaN(cntVec[0].F) {
				execCount = cntVec[0].F
			}
		} else if cntVec, _ := ps.Eval("count_over_time", lset, 0.0, mint, maxt, curTime); len(cntVec) > 0 && !math.IsNaN(cntVec[0].F) {
			execCount = cntVec[0].F
		}
		// Persist execution sample stats for debugging.
		debugExecSamples(funcName, lset, mint, maxt, curTime, execCount)
	}

	log.Printf("[QUERY ENGINE] Evaluation successful. Returning %d data points.", len(results))
	statusLabel = "success"

	response := gin.H{
		"status":           "success",
		"data":             results,
		"query_latency_ms": float64(evalLatency.Microseconds()) / 1000.0,
	}

	// Attach annotations so clients can read window and sample-count metadata.
	respAnn := gin.H{}
	for k, v := range annotations {
		respAnn[k] = v
	}
	respAnn["exec_window"] = gin.H{"mint": mint, "maxt": maxt, "cur": curTime}
	if needsExecCount && !math.IsNaN(execCount) {
		respAnn["sketch_exec_sample_count"] = execCount
	}
	if len(respAnn) > 0 {
		response["annotations"] = respAnn
	}
	respAnn["prometheus_request_window"] = gin.H{"mint": promMint, "maxt": promMaxt}
	respAnn["prometheus_sample_count"] = promCount
	respAnn["promsketch_sample_count"] = pskCount

	// Kirim response ke client
	c.JSON(http.StatusOK, response)

	// END STAGE 3: record result_sort / encode time
	resultDur := time.Since(resultStart).Seconds()
	queryStageDuration.WithLabelValues("result_sort", queryFuncLabel, aggLabel, statusLabel).Observe(resultDur)
}
