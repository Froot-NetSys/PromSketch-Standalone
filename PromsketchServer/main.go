package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	_ "net/http/pprof"

	"github.com/SieDeta/promsketch_std/promsketch"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zzylol/prometheus-sketches/model/labels"
	"github.com/zzylol/prometheus-sketches/promql/parser"
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

var ps *promsketch.PromSketches
var cloudEndpoint = os.Getenv("FORWARD_ENDPOINT")

var (
	ingestedMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
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
)

var maxIngestGoroutines int

func init() {
	ps = promsketch.NewPromSketches()
	log.Println("PromSketches instance initialized")

	val := os.Getenv("MAX_INGEST_GOROUTINES")
	if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
		maxIngestGoroutines = parsed
	} else {
		maxIngestGoroutines = 3 // default fallback
	}

	prometheus.MustRegister(ingestedMetrics)
	prometheus.MustRegister(queryResults)
}

var (
	machineToPort   sync.Map // maps machineID (string) to port index (int)
	portServers     []*PortServer
	portMutex       sync.Mutex
	startPort       = 7100
	machinesPerPort = 200
)

type PortServer struct {
	Port     int
	Registry *prometheus.Registry
	Metrics  *prometheus.GaugeVec
}

func NewPortServer(port int) *PortServer {
	reg := prometheus.NewRegistry()
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "promsketch_port_ingested_metrics_total",
			Help: "Total number of ingested metrics (partitioned)",
		},
		[]string{"metric", "machineid"},
	)
	reg.MustRegister(gauge)

	return &PortServer{
		Port:     port,
		Registry: reg,
		Metrics:  gauge,
	}
}

func (ps *PortServer) Start() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(ps.Registry, promhttp.HandlerOpts{}))

	addr := fmt.Sprintf(":%d", ps.Port)
	go func() {
		log.Printf("[PORT SERVER] Serving /metrics on port %d", ps.Port)
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

	// Pastikan portServers[portIndex] tersedia
	portMutex.Lock()
	defer portMutex.Unlock()

	for len(portServers) <= portIndex {
		port := startPort + len(portServers)
		server := NewPortServer(port)
		server.Start()
		portServers = append(portServers, server)
		log.Printf("[PORT SERVER] Initialized /metrics on port %d", port)
	}

	return portIndex
}

// ============================
// Fungsi untuk generate prometheus config
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
	for i, line := range lines {
		if strings.Contains(line, "job_name: \"promsketch_raw_groups\"") {
			start = i
		}
		if start != -1 && strings.HasPrefix(line, "  - job_name:") && i > start {
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

	var newLines []string
	if start != -1 {
		newLines = append(lines[:start], strings.Split(buf.String(), "\n")...)
		newLines = append(newLines, lines[end:]...)
	} else {
		newLines = append(lines, "")
		newLines = append(newLines, strings.Split(buf.String(), "\n")...)
	}

	return os.WriteFile(path, []byte(strings.Join(newLines, "\n")), 0644)
}

func main() {
	go logIngestionRate()

	router := gin.Default()
	router.POST("/ingest", handleIngest)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	// router.GET("/throughput_test", runStressThroughputTest)
	router.GET("/parse", handleParse)
	router.GET("/debug-state", handleDebugState)

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP", "message": "PromSketch Go server is running."})
	})

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":7000", nil))
	}()

	log.Printf("PromSketch Go server listening on :7000")
	if err := router.Run(":7000"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}

	ymlPath := "./prometheus/documentation/examples/prometheus.yml"
	if err := UpdatePrometheusYML(ymlPath); err != nil {
		log.Printf("❌ Failed to update Prometheus config: %v", err)
	} else {
		log.Printf("✅ Updated Prometheus config at %s", ymlPath)
	}
}

// handleDebugState akan memeriksa 10000 time series pertama
// dan mencetak yang mana saja yang sudah memiliki data sketch.
func handleDebugState(c *gin.Context) {
	log.Println("[DEBUG] Starting state check...")
	foundCount := 0
	detail := []string{}

	// Kita periksa dari machine_0 hingga machine_9999
	for i := 0; i < 10000; i++ {
		machineID := fmt.Sprintf("machine_%d", i)
		lsetBuilder := labels.NewBuilder(labels.Labels{})
		lsetBuilder.Set("machineid", machineID)
		lsetBuilder.Set(labels.MetricName, "fake_machine_metric") // Ganti sesuai metric kamu
		lset := lsetBuilder.Labels()

		minTime, maxTime := ps.PrintCoverage(lset, "avg_over_time") // Ganti ke fungsi yang kamu uji

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

var totalIngested int64

// func handleIngest(c *gin.Context) {
// 	var payload IngestPayload
// 	if err := c.ShouldBindJSON(&payload); err != nil {
// 		log.Printf("Error binding JSON payload: %v", err)
// 		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid JSON payload: %v", err.Error())})
// 		return
// 	}

// 	start := time.Now()
// 	var wg sync.WaitGroup

// 	for _, metric := range payload.Metrics {
// 		wg.Add(1)
// 		go func(metric MetricPayload) {
// 			defer wg.Done()

// 			log.Printf("[INGEST] name=%s labels=%v value=%.2f", metric.Name, metric.Labels, metric.Value)

// 			lsetBuilder := labels.NewBuilder(labels.Labels{})
// 			// Tambahkan label dari payload (cth: machineid="machine_0")
// 			for k, v := range metric.Labels {
// 				lsetBuilder.Set(k, v)
// 			}
// 			lsetBuilder.Set(labels.MetricName, metric.Name) // labels.MetricName = "__name__"
// 			lset := lsetBuilder.Labels()

// 			log.Printf("[INGEST] Inserting to sketch: name=%s labels=%v ts=%d value=%.2f", metric.Name, metric.Labels, payload.Timestamp, metric.Value)

// 			if err := ps.SketchInsert(lset, payload.Timestamp, metric.Value); err != nil {
// 				log.Printf("[INGEST ERROR] Failed to insert sketch: %v", err)
// 			}
// 			atomic.AddInt64(&totalIngested, 1)

// 		}(metric)
// 	}

// 	wg.Wait()
// 	totalDuration := time.Since(start).Milliseconds()
// 	log.Printf("[BATCH COMPLETED] Processed %d metrics in %dms", len(payload.Metrics), totalDuration)

// 	go forwardToCloud(payload)

// 	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": len(payload.Metrics)})
// }

func handleIngest(c *gin.Context) {
	var payload IngestPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		log.Printf("Error binding JSON payload: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid JSON payload: %v", err.Error())})
		return
	}

	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxIngestGoroutines)

	for _, metric := range payload.Metrics {
		wg.Add(1)
		sem <- struct{}{} // acquire slot

		go func(metric MetricPayload) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			// log.Printf("[INGEST] name=%s labels=%v value=%.2f", metric.Name, metric.Labels, metric.Value)

			lsetBuilder := labels.NewBuilder(labels.Labels{})
			for k, v := range metric.Labels {
				lsetBuilder.Set(k, v)
			}
			lsetBuilder.Set(labels.MetricName, metric.Name)
			lset := lsetBuilder.Labels()

			// log.Printf("[INGEST] Inserting to sketch: name=%s labels=%v ts=%d value=%.2f", metric.Name, metric.Labels, payload.Timestamp, metric.Value)

			if err := ps.SketchInsert(lset, payload.Timestamp, metric.Value); err == nil {
				atomic.AddInt64(&totalIngested, 1)
				ingestedMetrics.WithLabelValues(metric.Name, metric.Labels["machineid"]).Set(metric.Value)

				// Tambahan sinkronisasi ke registry per port
				machineID := metric.Labels["machineid"]
				if machineID != "" {
					portIndex := getOrCreatePortIndex(machineID)
					if portIndex < len(portServers) {
						portServers[portIndex].Metrics.WithLabelValues(metric.Name, machineID).Set(metric.Value)
					}
				}
			}

		}(metric)
	}

	wg.Wait()
	totalDuration := time.Since(start).Milliseconds()
	log.Printf("[BATCH COMPLETED] Processed %d metrics in %dms", len(payload.Metrics), totalDuration)

	// go forwardToCloud(payload)

	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": len(payload.Metrics)})
}

// func pushSyntheticResult(metricName string, labels map[string]string, value float64, timestamp int64) {
// 	pushgatewayURL := os.Getenv("PUSHGATEWAY_URL")
// 	if pushgatewayURL == "" {
// 		pushgatewayURL = "http://localhost:9091"
// 	}

// 	labelParts := []string{}
// 	for k, v := range labels {
// 		if k == "__name__" {
// 			continue // jangan push label __name__
// 		}
// 		labelParts = append(labelParts, fmt.Sprintf("%s=\"%s\"", k, v))
// 	}

// 	labelStr := strings.Join(labelParts, ",")

// 	body := fmt.Sprintf("%s{%s} %f\n", metricName, labelStr, value)

// 	instanceID := labels["machineid"]
// 	if instanceID == "" {
// 		instanceID = "default"
// 	}

// 	url := fmt.Sprintf("%s/metrics/job/promsketch_push/instance/%s", pushgatewayURL, instanceID)

// 	log.Printf("[PUSH DEBUG] Pushing to %s with body:\n%s", url, body)

// 	req, err := http.NewRequest("PUT", url, strings.NewReader(body))
// 	if err != nil {
// 		log.Printf("[PUSH ERROR] request: %v", err)
// 		return
// 	}
// 	req.Header.Set("Content-Type", "text/plain")

// 	client := &http.Client{Timeout: 3 * time.Second}
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		log.Printf("[PUSH ERROR] send: %v", err)
// 		return
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode >= 300 {
// 		log.Printf("[PUSH ERROR] status: %s", resp.Status)
// 	} else {
// 		log.Printf("[PUSH OK] metric=%s labels=%v value=%.2f", metricName, labels, value)
// 	}
// }

// func forwardToCloud(payload IngestPayload) {
// 	if cloudEndpoint == "" {
// 		log.Printf("[FORWARD] Skipped: FORWARD_ENDPOINT not set")
// 		return
// 	}
// 	data, err := json.Marshal(payload)
// 	if err != nil {
// 		log.Printf("[FORWARD ERROR] marshal: %v", err)
// 		return
// 	}
// 	req, err := http.NewRequest("POST", cloudEndpoint, bytes.NewBuffer(data))
// 	if err != nil {
// 		log.Printf("[FORWARD ERROR] request: %v", err)
// 		return
// 	}
// 	req.Header.Set("Content-Type", "application/json")

// 	client := &http.Client{Timeout: 5 * time.Second}
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		log.Printf("[FORWARD ERROR] send: %v", err)
// 		return
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode >= 300 {
// 		log.Printf("[FORWARD ERROR] status: %s", resp.Status)
// 	} else {
// 		log.Printf("[FORWARD] success: %d bytes sent", len(data))
// 	}
// }

// func forwardToCloud(payload IngestPayload) {
// 	for _, metric := range payload.Metrics {
// 		url := fmt.Sprintf("http://pushgateway:9091/metrics/job/promsketch/instance/%s", metric.Labels["machineid"])
// 		body := fmt.Sprintf("fake_metric{machineid=\"%s\"} %f\n", metric.Labels["machineid"], metric.Value)
// 		resp, err := http.Post(url, "text/plain", strings.NewReader(body))
// 		if err != nil {
// 			log.Printf("[PUSHGATEWAY ERROR] %v", err)
// 			continue
// 		}
// 		resp.Body.Close()
// 	}
// }

// func forwardToCloud(payload IngestPayload) {
// 	for _, metric := range payload.Metrics {
// 		machineID := metric.Labels["machineid"]
// 		url := fmt.Sprintf("http://pushgateway:9091/metrics/job/promsketch/instance/%s", machineID)

// 		pr, pw := io.Pipe()

// 		go func(mid string, val float64) {
// 			// Write directly to pipe writer
// 			fmt.Fprintf(pw, "fake_metric{machineid=\"%s\"} %f\n", mid, val)
// 			pw.Close()
// 		}(machineID, metric.Value)

// 		resp, err := http.Post(url, "text/plain", pr)
// 		if err != nil {
// 			log.Printf("[PUSHGATEWAY ERROR] %v", err)
// 			continue
// 		}
// 		resp.Body.Close()
// 	}
// }

func logIngestionRate() {
	var lastTotal int64 = 0

	// file log CSV
	file, err := os.OpenFile("throughput_log.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open throughput log file: %v", err)
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	if fileInfo.Size() == 0 {
		file.WriteString("timestamp,samples_per_sec,total_samples\n")
	}

	for {
		time.Sleep(5 * time.Second)

		current := atomic.LoadInt64(&totalIngested)
		rate := float64(current-lastTotal) / 5.0
		timestamp := time.Now().Format(time.RFC3339)

		log.Printf("[SERVER SPEED] Received %.2f samples/sec (Total: %d)", rate, current)

		// Save to CSV file
		entry := fmt.Sprintf("%s,%.2f,%d\n", timestamp, rate, current)
		if _, err := file.WriteString(entry); err != nil {
			log.Printf("[CSV LOG ERROR] %v", err)
		}

		lastTotal = current
	}
}

// func handleParse(c *gin.Context) {
// 	query := c.Query("q")
// 	if query == "" {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing query parameter 'q'"})
// 		return
// 	}

// 	expr, err := parser.ParseExpr(query)
// 	if err != nil {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Parse error: %v", err)})
// 		return
// 	}

// 	c.JSON(http.StatusOK, gin.H{
// 		"status": "success",
// 		"ast":    expr.String(),
// 	})
// }

func handleParse(c *gin.Context) {
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
	otherArgs := 0.0

	matrixSelectorArg := call.Args[len(call.Args)-1]
	rangeArg, ok := matrixSelectorArg.(*parser.MatrixSelector)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "The last argument must be a range vector (e.g., metric[60000ms])"})
		return
	}

	if len(call.Args) > 1 {
		firstArg, ok := call.Args[0].(*parser.NumberLiteral)
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Numeric argument (like quantile) must be a number"})
			return
		}
		otherArgs = firstArg.Val
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

	lsetBuilder := labels.NewBuilder(labels.Labels{})
	for k, v := range labelMap {
		lsetBuilder.Set(k, v)
	}
	lsetBuilder.Set(labels.MetricName, metricName)
	lset := lsetBuilder.Labels()

	log.Printf("[QUERY ENGINE] Parsed components: func=%s, lset=%v, otherArgs=%.2f", funcName, lset, otherArgs)

	// Calculate the time range based on the range
	queryDuration := rangeArg.Range
	maxt := time.Now().UnixMilli()
	mint := maxt - queryDuration.Milliseconds()

	// GEt sketch value
	sketchMin, sketchMax := ps.PrintCoverage(lset, funcName)
	log.Printf("[SKETCH COVERAGE] Sketch coverage: min=%d max=%d | requested mint=%d maxt=%d", sketchMin, sketchMax, mint, maxt)

	// If there's no data in sketch
	if sketchMin == -1 || sketchMax == -1 {
		log.Printf("[QUERY ENGINE] No sketch coverage available yet for %v. Creating instance.", lset)
		ps.NewSketchCacheInstance(lset, funcName, queryDuration.Milliseconds(), 100000, 10000.0)

		c.JSON(http.StatusAccepted, gin.H{
			"status":  "pending",
			"message": "Sketch data is being prepared. Please try again in a few moments.",
		})
		return
	}

	// COrrection if requested query outside the range
	if mint < sketchMin {
		mint = sketchMin
	}
	if maxt > sketchMax {
		maxt = sketchMax
	}

	// Validasi akhir
	if maxt <= mint {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "failed",
			"message": "Query time range is outside of sketch data coverage.",
		})
		return
	}

	curTime := maxt

	log.Printf("[QUERY ENGINE] Final adjusted range: mint=%d maxt=%d", mint, maxt)

	vector, annotations := ps.Eval(funcName, lset, otherArgs, mint, maxt, curTime)

	results := []map[string]interface{}{}
	for _, sample := range vector {
		if !math.IsNaN(sample.F) {
			// Perbaiki timestamp jika belum diset (nol)
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
				lset.Get("machineid"),
				fmt.Sprintf("%.2f", otherArgs),
			).Set(sample.F)
		}
	}

	log.Printf("[QUERY ENGINE] Evaluation successful. Returning %d data points.", len(results))
	response := gin.H{
		"status": "success",
		"data":   results,
	}
	if len(annotations) > 0 {
		response["annotations"] = annotations
	}

	c.JSON(http.StatusOK, response)
}

// func handleParse(c *gin.Context) {
// 	query := c.Query("q")
// 	if query == "" {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing query parameter 'q'"})
// 		return
// 	}
// 	log.Printf("[QUERY ENGINE] Received query: %s", query)

// 	expr, err := parser.ParseExpr(query)
// 	if err != nil {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Parse error: %v", err)})
// 		return
// 	}

// 	call, ok := expr.(*parser.Call)
// 	if !ok {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Query must be a function call (e.g., avg_over_time(...))"})
// 		return
// 	}

// 	funcName := call.Func.Name
// 	otherArgs := 0.0

// 	matrixSelectorArg := call.Args[len(call.Args)-1]
// 	rangeArg, ok := matrixSelectorArg.(*parser.MatrixSelector)
// 	if !ok {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "The last argument must be a range vector (e.g., metric[60000ms])"})
// 		return
// 	}

// 	if len(call.Args) > 1 {
// 		firstArg, ok := call.Args[0].(*parser.NumberLiteral)
// 		if !ok {
// 			c.JSON(http.StatusBadRequest, gin.H{"error": "Numeric argument (like quantile) must be a number"})
// 			return
// 		}
// 		otherArgs = firstArg.Val
// 	}

// 	vs, ok := rangeArg.VectorSelector.(*parser.VectorSelector)
// 	if !ok {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse vector selector inside the range vector"})
// 		return
// 	}
// 	metricName := vs.Name
// 	labelMap := make(map[string]string)
// 	for _, matcher := range vs.LabelMatchers {
// 		if matcher.Type == labels.MatchEqual {
// 			labelMap[matcher.Name] = matcher.Value
// 		} else {
// 			c.JSON(http.StatusBadRequest, gin.H{"error": "Only '=' label matchers are supported"})
// 			return
// 		}
// 	}

// 	lsetBuilder := labels.NewBuilder(labels.Labels{})
// 	for k, v := range labelMap {
// 		lsetBuilder.Set(k, v)
// 	}
// 	lsetBuilder.Set(labels.MetricName, metricName)
// 	lset := lsetBuilder.Labels()

// 	log.Printf("[QUERY ENGINE] Parsed components: func=%s, lset=%v, otherArgs=%.2f", funcName, lset, otherArgs)

// 	// Calculate the time range based on the range
// 	queryDuration := rangeArg.Range
// 	maxt := time.Now().UnixMilli()
// 	mint := maxt - queryDuration.Milliseconds()

// 	// GEt sketch value
// 	sketchMin, sketchMax := ps.PrintCoverage(lset, funcName)
// 	log.Printf("[SKETCH COVERAGE] Sketch coverage: min=%d max=%d | requested mint=%d maxt=%d", sketchMin, sketchMax, mint, maxt)

// 	// If there's no data in sketch
// 	if sketchMin == -1 || sketchMax == -1 {
// 		log.Printf("[QUERY ENGINE] No sketch coverage available yet for %v. Creating instance.", lset)
// 		ps.NewSketchCacheInstance(lset, funcName, queryDuration.Milliseconds(), 100000, 10000.0)

// 		c.JSON(http.StatusAccepted, gin.H{
// 			"status":  "pending",
// 			"message": "Sketch data is being prepared. Please try again in a few moments.",
// 		})
// 		return
// 	}

// 	// COrrection if requested query outside the range
// 	if mint < sketchMin {
// 		mint = sketchMin
// 	}
// 	if maxt > sketchMax {
// 		maxt = sketchMax
// 	}

// 	// Validasi akhir
// 	if maxt <= mint {
// 		c.JSON(http.StatusBadRequest, gin.H{
// 			"status":  "failed",
// 			"message": "Query time range is outside of sketch data coverage.",
// 		})
// 		return
// 	}

// 	curTime := maxt

// 	log.Printf("[QUERY ENGINE] Final adjusted range: mint=%d maxt=%d", mint, maxt)

// 	vector, annotations := ps.Eval(funcName, lset, otherArgs, mint, maxt, curTime)

// 	results := []map[string]interface{}{}
// 	for _, sample := range vector {
// 		if !math.IsNaN(sample.F) {
// 			results = append(results, map[string]interface{}{
// 				"timestamp": sample.T,
// 				"value":     sample.F,
// 			})
// 		}
// 	}

// 	log.Printf("[QUERY ENGINE] Evaluation successful. Returning %d data points.", len(results))
// 	response := gin.H{
// 		"status": "success",
// 		"data":   results,
// 	}
// 	if len(annotations) > 0 {
// 		response["annotations"] = annotations
// 	}

// 	c.JSON(http.StatusOK, response)
// }

// ===============================================================================================================================

// ====================================================================================================================================

// func runThroughputTest(c *gin.Context) {
// 	seriesCountStr := c.DefaultQuery("series", "10000")
// 	maxGoroutinesStr := c.DefaultQuery("max_goroutines", "50")

// 	seriesCount, err1 := strconv.Atoi(seriesCountStr)
// 	maxGoroutines, err2 := strconv.Atoi(maxGoroutinesStr)
// 	if err1 != nil || err2 != nil || seriesCount <= 0 || maxGoroutines <= 0 {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid query parameters"})
// 		return
// 	}

// 	log.Printf("[THROUGHPUT TEST] Starting test with %d timeseries and max %d goroutines", seriesCount, maxGoroutines)

// 	start := time.Now()
// 	var wg sync.WaitGroup
// 	var success int64
// 	sem := make(chan struct{}, maxGoroutines)

// 	for i := 0; i < seriesCount; i++ {
// 		wg.Add(1)
// 		sem <- struct{}{}

// 		go func(i int) {
// 			defer wg.Done()
// 			defer func() { <-sem }()

// 			machineID := fmt.Sprintf("benchstress_%d", i)
// 			lset := labels.FromStrings(
// 				"machineid", machineID,
// 				"__name__", "benchmark_metric",
// 			)

// 			timestamp := time.Now().UnixMilli()
// 			value := float64(i % 1000) // atau bisa diganti dengan rand.Float64() untuk nilai acak

// 			if err := ps.SketchInsertInsertionThroughputTest(lset, timestamp, value); err == nil {
// 				atomic.AddInt64(&success, 1)
// 			} else {
// 				log.Printf("[ERROR] Insert failed: %v", err)
// 			}
// 		}(i)
// 	}

// 	wg.Wait()
// 	elapsed := time.Since(start).Seconds()
// 	rate := float64(success) / elapsed

// 	log.Printf("[THROUGHPUT TEST] Inserted %d samples in %.2fs (%.2f samples/sec)", success, elapsed, rate)

// 	c.JSON(http.StatusOK, gin.H{
// 		"inserted_samples": success,
// 		"duration_seconds": elapsed,
// 		"throughput":       fmt.Sprintf("%.2f samples/sec", rate),
// 	})
// }
