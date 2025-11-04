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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/model/labels"
	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/promql/parser"
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
		maxIngestGoroutines = 1024 // default fallback
	}

	prometheus.MustRegister(ingestedMetrics)
	prometheus.MustRegister(queryResults)
}

var (
	portServers     []*PortServer
	portMutex       sync.Mutex
	startPort       = 7100
	machinesPerPort = 200
	lastYMLUpdate   time.Time
)

type PortServer struct {
	Port     int
	Registry *prometheus.Registry
	Metrics  *prometheus.GaugeVec
	// RAW per-port
	Raw    map[string]*prometheus.GaugeVec
	RawMux sync.Mutex
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

//		addr := fmt.Sprintf(":%d", ps.Port)
//		go func() {
//			log.Printf("[PORT SERVER] Serving /metrics on port %d", ps.Port)
//			log.Fatal(http.ListenAndServe(addr, mux))
//		}()
//	}

type RegisterPayload struct {
	NumTargets          int `json:"num_targets"`
	EstimatedTimeseries int `json:"estimated_timeseries"`
	MachinesPerPort     int `json:"machines_per_port"`
	StartPort           int `json:"start_port"`
}

// :7000/register_config
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
		srv.Start() // menjalankan /ingest + /metrics pada port
		portServers = append(portServers, srv)
		log.Printf("[PORT SERVER] Initialized /metrics and /ingest on port %d", p)
	}
	ports := len(portServers)
	portMutex.Unlock()

	// _ = UpdatePrometheusYML(".../prometheus.yml")??

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

	// NEW: tambah handler POST /ingest pada mux port
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

// var (
// 	rawMetricsMap = make(map[string]*prometheus.GaugeVec)
// 	rawMetricsMux sync.Mutex
// )

// getOrCreateRaw memastikan metric RAW terdaftar di registry port
func (psv *PortServer) getOrCreateRaw(name string, labelKeys []string) *prometheus.GaugeVec {
	psv.RawMux.Lock()
	defer psv.RawMux.Unlock()

	if g, ok := psv.Raw[name]; ok {
		return g
	}
	g := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name, // mis. "fake_machine_metric" dari exporter
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
				// cari indentasi
				for j := i + 1; j < len(lines); j++ {
					if strings.HasPrefix(lines[j], "  - job_name:") || strings.TrimSpace(lines[j]) == "" {
						continue
					}
					// sisipkan di sini
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

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	go logIngestionRate()

	router := gin.Default()
	// router.POST("/ingest", handleIngest)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	// router.GET("/throughput_test", runStressThroughputTest)
	router.GET("/parse", handleParse)
	router.POST("/ingest-query-result", handleQueryResultIngest)
	router.GET("/debug-state", handleDebugState)
	router.POST("/register_config", handleRegisterConfig)

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

// Tambahkan fungsi helper debug di bawah deklarasi import:
// func debugSampleIngested(metric string, value float64, labels map[string]string, ts int64) {
// 	log.Printf("[DEBUG-INGEST] metric=%s value=%f labels=%v timestamp=%d", metric, value, labels, ts)
// }

func debugAggregationResult(fname string, lset map[string]string, mint, maxt int64, result float64, count float64) {
	// log.Printf("[DEBUG-AGG] function=%s labels=%v mint=%d maxt=%d result=%f count=%f", fname, lset, mint, maxt, result, count)
	// Simpan ke file CSV juga (opsional)
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
		// tulis header jika file masih kosong
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

// func handleIngest(c *gin.Context) {
// 	var payload IngestPayload
// 	if err := c.ShouldBindJSON(&payload); err != nil {
// 		log.Printf("Error binding JSON payload: %v", err)
// 		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid JSON payload: %v", err.Error())})
// 		return
// 	}

// 	start := time.Now()
// 	var wg sync.WaitGroup
// 	sem := make(chan struct{}, maxIngestGoroutines)

// 	for _, metric := range payload.Metrics {
// 		wg.Add(1)
// 		sem <- struct{}{}

// 		go func(metric MetricPayload) {
// 			defer wg.Done()
// 			defer func() { <-sem }()

// 			// ========== ORIGINAL (Spesific machine id) ==========
// 			lsetBuilder := labels.NewBuilder(labels.Labels{})
// 			for k, v := range metric.Labels {
// 				lsetBuilder.Set(k, v)
// 			}
// 			lsetBuilder.Set(labels.MetricName, metric.Name)
// 			lsetOriginal := lsetBuilder.Labels()

// 			if err := ps.SketchInsert(lsetOriginal, payload.Timestamp, metric.Value); err == nil {
// 				atomic.AddInt64(&totalIngested, 1)
// 				ingestedMetrics.WithLabelValues(metric.Name, metric.Labels["machineid"]).Set(metric.Value)

// 				// Add to registry per-port
// 				machineID := metric.Labels["machineid"]
// 				if machineID != "" {
// 					portIndex := getOrCreatePortIndex(machineID)
// 					if portIndex < len(portServers) {
// 						portServers[portIndex].Metrics.WithLabelValues(metric.Name, machineID).Set(metric.Value)
// 					}
// 				}
// 			}
// 			// ====== RAW DATA EXPORTER ======
// 			labelKeys := []string{}
// 			labelVals := []string{}
// 			for k, v := range metric.Labels {
// 				labelKeys = append(labelKeys, k)
// 				labelVals = append(labelVals, v)
// 			}
// 			rawGauge := getOrCreateRawMetric(metric.Name, labelKeys)
// 			rawGauge.WithLabelValues(labelVals...).Set(metric.Value)

// 			// ========== GLOBAL (without machineid) ==========
// 			lsetGlobalBuilder := labels.NewBuilder(labels.Labels{})
// 			for k, v := range metric.Labels {
// 				if k != "machineid" {
// 					lsetGlobalBuilder.Set(k, v)
// 				}
// 			}
// 			lsetGlobalBuilder.Set(labels.MetricName, metric.Name)
// 			lsetGlobal := lsetGlobalBuilder.Labels()

// 			if err := ps.SketchInsert(lsetGlobal, payload.Timestamp, metric.Value); err == nil {
// 				atomic.AddInt64(&totalIngested, 1)
// 				ingestedMetrics.WithLabelValues(metric.Name, "global").Set(metric.Value)
// 			}
// 		}(metric)
// 	}

// 	wg.Wait()

// 	// Automaticly update prometheus.yml
// 	now := time.Now()
// 	if now.Sub(lastYMLUpdate) > 10*time.Second {
// 		ymlPath := "./prometheus/documentation/examples/prometheus.yml"
// 		if err := UpdatePrometheusYML(ymlPath); err != nil {
// 			log.Printf("Failed to update Prometheus config: %v", err)
// 		} else {
// 			log.Printf("Updated Prometheus config at %s", ymlPath)
// 			lastYMLUpdate = now
// 		}
// 	}

// 	totalDuration := time.Since(start).Milliseconds()
// 	log.Printf("[BATCH COMPLETED] Processed %d metrics in %dms", len(payload.Metrics), totalDuration)

// 	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": len(payload.Metrics)})
// }

func handleIngest(c *gin.Context) {
	var payload IngestPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid JSON payload: %v", err.Error())})
		return
	}
	_, _ = processIngest(payload)
	// Automaticly update prometheus.yml
	now := time.Now()
	if now.Sub(lastYMLUpdate) > 10*time.Second {
		ymlPath := "./prometheus/documentation/examples/prometheus.yml"
		if err := UpdatePrometheusYML(ymlPath); err != nil {
			log.Printf("Failed to update Prometheus config: %v", err)
		} else {
			log.Printf("Updated Prometheus config at %s", ymlPath)
			lastYMLUpdate = now
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": len(payload.Metrics)})
}

func processIngest(payload IngestPayload) (int, error) {
	// start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxIngestGoroutines)

	for _, metric := range payload.Metrics {
		wg.Add(1)
		sem <- struct{}{}
		go func(metric MetricPayload) {
			defer wg.Done()
			defer func() { <-sem }()
			// debugSampleIngested(metric.Name, metric.Value, metric.Labels, payload.Timestamp)
			// ========== ORIGINAL (Spesific machine id) ==========
			// lset original (per machineid)
			lsetBuilder := labels.NewBuilder(labels.Labels{})
			for k, v := range metric.Labels {
				lsetBuilder.Set(k, v)
			}
			lsetBuilder.Set(labels.MetricName, metric.Name)
			lsetOriginal := lsetBuilder.Labels()
			// INSERT to sketch global ‘ps’
			if err := ps.SketchInsert(lsetOriginal, payload.Timestamp, metric.Value); err == nil {
				atomic.AddInt64(&totalIngested, 1)
				ingestedMetrics.WithLabelValues(metric.Name, metric.Labels["machineid"]).Set(metric.Value)

				// Add to registry per-port
				machineID := metric.Labels["machineid"]
				if machineID != "" {
					portIndex := getOrCreatePortIndex(machineID)
					if portIndex < len(portServers) {
						portServers[portIndex].Metrics.WithLabelValues(metric.Name, machineID).Set(metric.Value)
					}
				}
			}
			// ====== RAW DATA EXPORTER — per-port ======
			labelKeys := make([]string, 0, len(metric.Labels))
			for k := range metric.Labels {
				labelKeys = append(labelKeys, k)
			}
			sort.Strings(labelKeys)
			labelVals := make([]string, 0, len(labelKeys))
			for _, k := range labelKeys {
				labelVals = append(labelVals, metric.Labels[k])
			}

			// Tentukan port dari machineid, lalu update RAW di registry port tsb
			portIndex := getOrCreatePortIndex(metric.Labels["machineid"])
			if portIndex < len(portServers) {
				raw := portServers[portIndex].getOrCreateRaw(metric.Name, labelKeys)
				raw.WithLabelValues(labelVals...).Set(metric.Value)
			}

			// ========== GLOBAL (without machineid) ==========
			lsetGlobalBuilder := labels.NewBuilder(labels.Labels{})
			for k, v := range metric.Labels {
				if k != "machineid" {
					lsetGlobalBuilder.Set(k, v)
				}
			}
			lsetGlobalBuilder.Set(labels.MetricName, metric.Name)
			lsetGlobal := lsetGlobalBuilder.Labels()

			if err := ps.SketchInsert(lsetGlobal, payload.Timestamp, metric.Value); err == nil {
				atomic.AddInt64(&totalIngested, 1)
				ingestedMetrics.WithLabelValues(metric.Name, "global").Set(metric.Value)
			}
		}(metric)
	}
	wg.Wait()
	// totalDuration := time.Since(start).Milliseconds()
	// log.Printf("[BATCH COMPLETED] Processed %d metrics in %dms", len(payload.Metrics), totalDuration)
	return len(payload.Metrics), nil
}

// Rechieved query results from promtools.py
func handleQueryResultIngest(c *gin.Context) {
	var result struct {
		Function       string  `json:"function"`
		OriginalMetric string  `json:"original_metric"`
		MachineID      string  `json:"machineid"`
		Quantile       string  `json:"quantile"`
		Value          float64 `json:"value"`
		Timestamp      int64   `json:"timestamp"`
	}

	if err := c.ShouldBindJSON(&result); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	queryResults.WithLabelValues(result.Function, result.OriginalMetric, result.MachineID, result.Quantile).Set(result.Value)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
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

	// 	// COrrection if requested query outside the range
	// if mint < sketchMin {
	// 	mint = sketchMin
	// }
	// if maxt > sketchMax {
	// 	maxt = sketchMax
	// }

	// Sliding-window clamp: jaga lebar ~W dan hindari efek "kumulatif" saat warm-up
	W := queryDuration.Milliseconds()

	// t2 = waktu terbaru yang benar-benar ada di sketch
	t2 := maxt
	if t2 > sketchMax {
		t2 = sketchMax
	}

	// geser t1 agar lebar window W
	t1 := t2 - W
	if t1 < sketchMin {
		// histori belum penuh: kunci t1 di sketchMin, lalu upayakan lebar ~W
		t1 = sketchMin
		if t1+W <= sketchMax {
			t2 = t1 + W
		} else {
			// coverage < W: pakai coverage yang ada (tidak membesar tiap detik)
			t2 = sketchMax
		}
	}

	mint, maxt = t1, t2
	// DEBUG: Log window penyesuaian PromSketch vs Prometheus
	log.Printf("[DEBUG WINDOW] Prometheus requested window: mint=%d maxt=%d (durasi=%d ms)", maxt-queryDuration.Milliseconds(), maxt, queryDuration.Milliseconds())
	log.Printf("[DEBUG WINDOW] PromSketch final window:     mint=%d maxt=%d (durasi=%d ms)", mint, maxt, maxt-mint)
	// ----- SAMPLE COUNT untuk 2 window -----
	// Prometheus requested window: panjang W berakhir di 'maxt' (yang sudah diselaraskan)
	promMint := maxt - W
	promMaxt := maxt

	var promCount float64 = math.NaN()
	if cntVec, _ := ps.Eval("count_over_time", lset, 0.0, promMint, promMaxt, promMaxt); len(cntVec) > 0 && !math.IsNaN(cntVec[0].F) {
		promCount = cntVec[0].F
	}

	var pskCount float64 = math.NaN()
	if cntVec, _ := ps.Eval("count_over_time", lset, 0.0, mint, maxt, maxt); len(cntVec) > 0 && !math.IsNaN(cntVec[0].F) {
		pskCount = cntVec[0].F
	}

	// Log ke console & CSV
	debugWindowCounts(funcName, lset, promMint, promMaxt, mint, maxt, promCount, pskCount)
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

	startEval := time.Now()
	vector, annotations := ps.Eval(funcName, lset, otherArgs, mint, maxt, curTime)

	// Print every exec steps
	evalLatency := time.Since(startEval)

	// Logging server-side latency
	log.Printf("[QUERY ENGINE] ps.Eval latency: %s (%.2f ms)", evalLatency, float64(evalLatency.Microseconds())/1000.0)

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
	needsExecCount := funcName == "l1_over_time" || funcName == "l2_over_time" ||
		funcName == "distinct_over_time" || funcName == "entropy_over_time"

	var execCount float64 = math.NaN()
	if needsExecCount {
		// Jalankan second pass: count_over_time(...) pada window final PromSketch
		if cntVec, _ := ps.Eval("count_over_time", lset, 0.0, mint, maxt, curTime); len(cntVec) > 0 && !math.IsNaN(cntVec[0].F) {
			execCount = cntVec[0].F
		}
		// Log + CSV
		debugExecSamples(funcName, lset, mint, maxt, curTime, execCount)
	}

	// --- build response ---
	log.Printf("[QUERY ENGINE] Evaluation successful. Returning %d data points.", len(results))
	response := gin.H{
		"status":           "success",
		"data":             results,
		"query_latency_ms": float64(evalLatency.Microseconds()) / 1000.0,
	}

	// --- MERGE/ADD annotations agar klien bisa baca window & sample count ---
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

	// (kode existing untuk debugAggregationResult dll tetap)
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
