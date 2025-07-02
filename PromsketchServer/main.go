// // package main

// // import (
// // 	"fmt"
// // 	"log"
// // 	"math"
// // 	"net/http"
// // 	"os"
// // 	"strconv"
// // 	"strings"
// // 	"sync"
// // 	"time"

// // 	"github.com/SieDeta/promsketch_std/promsketch"       // Replace with your actual module path
// // 	"github.com/gin-gonic/gin"                           // Popular Go web framework
// // 	"github.com/zzylol/prometheus-sketches/model/labels" // This path may need to match your project structure
// // )

// // // Structure for the metric data payload received from the Python Ingester
// // type IngestPayload struct {
// // 	Timestamp int64           `json:"timestamp"` // Time in milliseconds (Prometheus-compatible)
// // 	Metrics   []MetricPayload `json:"metrics"`
// // }

// // // Structure for each metric in the payload
// // type MetricPayload struct {
// // 	Name   string            `json:"name"`   // Metric name (e.g., "fake_machine_metric")
// // 	Labels map[string]string `json:"labels"` // Metric labels (e.g., {"machineid": "machine_0"})
// // 	Value  float64           `json:"value"`  // Metric value
// // }

// // // Global PromSketches instance
// // var ps *promsketch.PromSketches

// // func init() {
// // 	ps = promsketch.NewPromSketches()
// // 	log.Println("PromSketches instance initialized.")

// // 	// Get the number of time series from environment variable or default (for testing)
// // 	// For production use, this may come from configuration.
// // 	// If no value is provided, use a safe default.
// // 	numTimeseriesStr := os.Getenv("NUM_TIMESERIES_INIT")
// // 	numTimeseriesInit, err := strconv.Atoi(numTimeseriesStr)
// // 	if err != nil || numTimeseriesInit == 0 {
// // 		numTimeseriesInit = 1000 // Default, matching your EvalData.py example
// // 	}
// // 	if numTimeseriesInit > 2000 { // Limit to avoid excessive memory use on startup
// // 		numTimeseriesInit = 2000
// // 	}

// // 	defaultTimeWindow := int64(60 * 1000) // 60 seconds * 1000 ms/second = 60000 ms
// // 	defaultItemWindow := int64(100000)
// // 	defaultValueScale := float64(10000)

// // 	// Initialize sketches for all expected time series
// // 	for i := 0; i < numTimeseriesInit; i++ {
// // 		machineID := fmt.Sprintf("machine_%d", i)
// // 		lset := labels.FromStrings("machineid", machineID, "fake_metric", "fake_machine_metric")

// // 		// Initialize sketches for all expected query functions
// // 		if err := ps.NewSketchCacheInstance(lset, "avg_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale); err != nil {
// // 			log.Printf("Error creating sketch for avg_over_time on %v: %v", lset, err)
// // 		}
// // 		if err := ps.NewSketchCacheInstance(lset, "quantile_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale); err != nil {
// // 			log.Printf("Error creating sketch for quantile_over_time on %v: %v", lset, err)
// // 		}
// // 		if err := ps.NewSketchCacheInstance(lset, "entropy_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale); err != nil {
// // 			log.Printf("Error creating sketch for entropy_over_time on %v: %v", lset, err)
// // 		}
// // 	}
// // 	log.Printf("Initial sketches created for %d time series.", numTimeseriesInit)
// // }

// // func main() {
// // 	router := gin.Default()

// // 	// Endpoint to receive metric data from the Python Ingester
// // 	// Data is sent as a JSON-formatted POST request
// // 	router.POST("/ingest", handleIngest)

// // 	// Endpoint to query sketch data (acts as a PromQL replacement)
// // 	// Queries use a GET request with URL parameters
// // 	router.GET("/query", handleQuery)

// // 	// Simple endpoint to check server status
// // 	router.GET("/health", func(c *gin.Context) {
// // 		c.JSON(http.StatusOK, gin.H{"status": "UP", "message": "PromSketch Go server is running."})
// // 	})

// // 	log.Printf("PromSketch Go server listening on :7000")
// // 	if err := router.Run(":7000"); err != nil {
// // 		log.Fatalf("Failed to run server: %v", err)
// // 	}
// // }

// // var totalIngested int64

// // // handleIngest receives metric data from custom_data_ingester.py
// // func handleIngest(c *gin.Context) {
// // 	var payload IngestPayload
// // 	if err := c.ShouldBindJSON(&payload); err != nil {
// // 		log.Printf("Error binding JSON payload: %v", err)
// // 		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid JSON payload: %v", err.Error())})
// // 		return
// // 	}

// // 	var (
// // 		ingestedCount int64
// // 		wg            sync.WaitGroup
// // 		mutex         sync.Mutex
// // 	)

// // 	for _, metric := range payload.Metrics {
// // 		wg.Add(1)
// // 		go func(metric MetricPayload) {
// // 			defer wg.Done()

// // 			lsetBuilder := labels.NewBuilder(labels.Labels{})
// // 			for k, v := range metric.Labels {
// // 				lsetBuilder.Set(k, v)
// // 			}
// // 			lsetBuilder.Set("fake_metric", metric.Name)
// // 			lset := lsetBuilder.Labels()

// // 			if err := ps.SketchInsert(lset, payload.Timestamp, metric.Value); err != nil {
// // 				log.Printf("Insert failed for %v: %v", lset, err)
// // 				return
// // 			}

// // 			mutex.Lock()
// // 			ingestedCount++
// // 			mutex.Unlock()
// // 		}(metric)
// // 	}

// // 	wg.Wait()

// // 	totalIngested += ingestedCount
// // 	log.Printf("Batch ingested: %d, Total ingested: %d", ingestedCount, totalIngested)

// // 	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": ingestedCount})
// // }

// // // handleQuery processes query requests from EvalData.py
// // func handleQuery(c *gin.Context) {
// // 	funcName := c.Query("func")
// // 	metricName := c.Query("metric")

// // 	mintStr := c.Query("mint")
// // 	maxtStr := c.Query("maxt")

// // 	log.Printf("DEBUG Query: func=%s, metric=%s, mintStr='%s', maxtStr='%s'", funcName, metricName, mintStr, maxtStr)

// // 	mint, err := strconv.ParseInt(mintStr, 10, 64)
// // 	if err != nil {
// // 		log.Printf("ERROR: Failed to parse 'mint' parameter '%s': %v", mintStr, err)
// // 		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'mint' parameter. Must be an integer timestamp in milliseconds."})
// // 		return
// // 	}
// // 	maxt, err := strconv.ParseInt(maxtStr, 10, 64)
// // 	if err != nil {
// // 		log.Printf("ERROR: Failed to parse 'maxt' parameter '%s': %v", maxtStr, err)
// // 		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'maxt' parameter. Must be an integer timestamp in milliseconds."})
// // 		return
// // 	}

// // 	otherArgsStr := c.Query("args")
// // 	otherArgs := 0.0
// // 	if otherArgsStr != "" {
// // 		parsedArgs, err := strconv.ParseFloat(otherArgsStr, 64)
// // 		if err != nil {
// // 			log.Printf("[Error] Failed to parse args: %v", err)
// // 			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'args' parameter. Must be a float."})
// // 			return
// // 		}
// // 		otherArgs = parsedArgs
// // 	}
// // 	log.Printf("[Query] args=%.4f", otherArgs)

// // 	// Build label set
// // 	lsetBuilder := labels.NewBuilder(labels.Labels{})
// // 	for k, v := range c.Request.URL.Query() {
// // 		if strings.HasPrefix(k, "label_") {
// // 			labelKey := k[len("label_"):]
// // 			labelValue := v[0]
// // 			lsetBuilder.Set(labelKey, labelValue)
// // 			log.Printf("[Label] %s=%s", labelKey, labelValue)
// // 		}
// // 	}
// // 	lsetBuilder.Set("fake_metric", metricName)
// // 	lset := lsetBuilder.Labels()
// // 	log.Printf("[LabelSet] Final lset: %v", lset)

// // 	curTime := time.Now().UnixMilli()
// // 	isCovered := ps.LookUp(lset, funcName, mint, maxt)
// // 	if !isCovered {
// // 		log.Printf("[Sketch] Data NOT covered for range [%d, %d] on %v for func=%s", mint, maxt, lset, funcName)
// // 		c.JSON(http.StatusAccepted, gin.H{
// // 			"status":  "pending",
// // 			"message": "Sketch data not yet available. Try again later.",
// // 		})
// // 		return
// // 	}
// // 	log.Printf("[Sketch] Data covered for range [%d, %d] on %v for func=%s", mint, maxt, lset, funcName)

// // 	vector, annotations := ps.Eval(funcName, lset, otherArgs, mint, maxt, curTime)
// // 	log.Printf("[Eval] Raw result length: %d", len(vector))

// // 	// Filter out NaN or invalid results
// // 	results := []map[string]interface{}{}
// // 	for i, sample := range vector {
// // 		if math.IsNaN(sample.F) || sample.T == 0 {
// // 			log.Printf("[Eval] Skipping invalid sample #%d: timestamp=%d, value=%.4f", i, sample.T, sample.F)
// // 			continue
// // 		}
// // 		results = append(results, map[string]interface{}{
// // 			"value":     sample.F,
// // 			"timestamp": sample.T,
// // 		})
// // 	}

// // 	// Prepare JSON response
// // 	response := gin.H{
// // 		"status": "success",
// // 		"data":   results,
// // 	}
// // 	if annotations != nil && len(annotations) > 0 {
// // 		response["annotations"] = annotations
// // 		log.Printf("[Eval] Annotations: %+v", annotations)
// // 	}

// // 	if len(results) == 0 {
// // 		log.Printf("[Query] All samples are invalid or sketch not yet populated. Returning empty result.")
// // 	}

// // 	c.Header("Content-Type", "application/json")
// // 	c.JSON(http.StatusOK, response)
// // 	log.Printf("[Query] func=%s on lset=%v (range %d-%d) returned %d valid result(s).", funcName, lset, mint, maxt, len(results))
// // }

// ===============================================================================================================
// ===============================================================================================================
// ===============================================================================================================
// ===============================================================================================================

// package main

// import (
// 	"fmt"
// 	"log"
// 	"math"
// 	"net/http"
// 	"os"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	_ "net/http/pprof"

// 	"github.com/SieDeta/promsketch_std/promsketch"       // Replace with your actual module path
// 	"github.com/gin-gonic/gin"                           // Popular Go web framework
// 	"github.com/zzylol/prometheus-sketches/model/labels" // This path may need to match your project structure
// )

// // Structure for the metric data payload received from the Python Ingester
// type IngestPayload struct {
// 	Timestamp int64           `json:"timestamp"`
// 	Metrics   []MetricPayload `json:"metrics"`
// }

// type MetricPayload struct {
// 	Name   string            `json:"name"`
// 	Labels map[string]string `json:"labels"`
// 	Value  float64           `json:"value"`
// }

// var ps *promsketch.PromSketches

// func init() {
// 	ps = promsketch.NewPromSketches()
// 	log.Println("PromSketches instance initialized.")

// 	numTimeseriesStr := os.Getenv("NUM_TIMESERIES_INIT")
// 	numTimeseriesInit, err := strconv.Atoi(numTimeseriesStr)
// 	if err != nil || numTimeseriesInit == 0 {
// 		numTimeseriesInit = 1000
// 	}
// 	if numTimeseriesInit > 2000 {
// 		numTimeseriesInit = 2000
// 	}

// 	defaultTimeWindow := int64(60 * 1000)
// 	defaultItemWindow := int64(100000)
// 	defaultValueScale := float64(10000)

// 	for i := 0; i < numTimeseriesInit; i++ {
// 		machineID := fmt.Sprintf("machine_%d", i)
// 		lset := labels.FromStrings("machineid", machineID, "fake_metric", "fake_machine_metric")

// 		_ = ps.NewSketchCacheInstance(lset, "avg_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale)
// 		_ = ps.NewSketchCacheInstance(lset, "quantile_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale)
// 		_ = ps.NewSketchCacheInstance(lset, "entropy_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale)
// 	}
// 	log.Printf("Initial sketches created for %d time series.", numTimeseriesInit)
// }

// func main() {
// 	go logIngestionRate()

// 	router := gin.Default()
// 	router.POST("/ingest", handleIngest)
// 	router.GET("/query", handleQuery)
// 	router.GET("/health", func(c *gin.Context) {
// 		c.JSON(http.StatusOK, gin.H{"status": "UP", "message": "PromSketch Go server is running."})
// 	})

// 	go func() {
// 		log.Println(http.ListenAndServe("localhost:6060", nil))
// 	}()

// 	log.Printf("PromSketch Go server listening on :7000")
// 	if err := router.Run(":7000"); err != nil {
// 		log.Fatalf("Failed to run server: %v", err)
// 	}
// }

// var totalIngested int64

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
// 			lsetBuilder := labels.NewBuilder(labels.Labels{})
// 			for k, v := range metric.Labels {
// 				lsetBuilder.Set(k, v)
// 			}
// 			lsetBuilder.Set("fake_metric", metric.Name)
// 			lset := lsetBuilder.Labels()

// 			if err := ps.SketchInsert(lset, payload.Timestamp, metric.Value); err == nil {
// 				atomic.AddInt64(&totalIngested, 1)
// 			}
// 		}(metric)
// 	}
// 	wg.Wait()
// 	duration := time.Since(start).Milliseconds()
// 	log.Printf("[RECEIVED] %d metrics in %.2fms", len(payload.Metrics), float64(duration))

// 	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": len(payload.Metrics)})
// }

// func logIngestionRate() {
// 	var lastTotal int64 = 0
// 	for {
// 		time.Sleep(5 * time.Second)
// 		current := atomic.LoadInt64(&totalIngested)
// 		rate := float64(current-lastTotal) / 5.0
// 		log.Printf("[SERVER SPEED] Received %.2f samples/sec (Total: %d)", rate, current)
// 		lastTotal = current
// 	}
// }

// func handleQuery(c *gin.Context) {
// 	funcName := c.Query("func")
// 	metricName := c.Query("metric")
// 	mintStr := c.Query("mint")
// 	maxtStr := c.Query("maxt")

// 	log.Printf("DEBUG Query: func=%s, metric=%s, mintStr='%s', maxtStr='%s'", funcName, metricName, mintStr, maxtStr)

// 	mint, err := strconv.ParseInt(mintStr, 10, 64)
// 	if err != nil {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'mint' parameter. Must be an integer timestamp in milliseconds."})
// 		return
// 	}
// 	maxt, err := strconv.ParseInt(maxtStr, 10, 64)
// 	if err != nil {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'maxt' parameter. Must be an integer timestamp in milliseconds."})
// 		return
// 	}

// 	otherArgsStr := c.Query("args")
// 	otherArgs := 0.0
// 	if otherArgsStr != "" {
// 		parsedArgs, err := strconv.ParseFloat(otherArgsStr, 64)
// 		if err != nil {
// 			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'args' parameter. Must be a float."})
// 			return
// 		}
// 		otherArgs = parsedArgs
// 	}

// 	lsetBuilder := labels.NewBuilder(labels.Labels{})
// 	for k, v := range c.Request.URL.Query() {
// 		if strings.HasPrefix(k, "label_") {
// 			labelKey := k[len("label_"):len(k)]
// 			labelValue := v[0]
// 			lsetBuilder.Set(labelKey, labelValue)
// 		}
// 	}
// 	lsetBuilder.Set("fake_metric", metricName)
// 	lset := lsetBuilder.Labels()

// 	curTime := time.Now().UnixMilli()
// 	if !ps.LookUp(lset, funcName, mint, maxt) {
// 		c.JSON(http.StatusAccepted, gin.H{"status": "pending", "message": "Sketch data not yet available. Try again later."})
// 		return
// 	}

// 	vector, annotations := ps.Eval(funcName, lset, otherArgs, mint, maxt, curTime)
// 	results := []map[string]interface{}{}
// 	for _, sample := range vector {
// 		if !math.IsNaN(sample.F) && sample.T != 0 {
// 			results = append(results, map[string]interface{}{"value": sample.F, "timestamp": sample.T})
// 		}
// 	}
// 	response := gin.H{"status": "success", "data": results}
// 	if len(annotations) > 0 {
// 		response["annotations"] = annotations
// 	}
// 	c.JSON(http.StatusOK, response)
// }

// ===================================== NEW MAIN CODE ====================================================
// ===============================================================================================================
// ===============================================================================================================
// ===============================================================================================================

// package main

// import (
// 	"bytes"
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"math"
// 	"net/http"
// 	"os"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	_ "net/http/pprof"

// 	"github.com/SieDeta/promsketch_std/promsketch"
// 	"github.com/gin-gonic/gin"
// 	"github.com/zzylol/prometheus-sketches/model/labels"
// )

// // Structure for the metric data payload received from the Python Ingester
// type IngestPayload struct {
// 	Timestamp int64           `json:"timestamp"`
// 	Metrics   []MetricPayload `json:"metrics"`
// }

// type MetricPayload struct {
// 	Name   string            `json:"name"`
// 	Labels map[string]string `json:"labels"`
// 	Value  float64           `json:"value"`
// }

// var ps *promsketch.PromSketches
// var cloudEndpoint = os.Getenv("FORWARD_ENDPOINT")

// func init() {
// 	ps = promsketch.NewPromSketches()
// 	log.Println("PromSketches instance initialized.")

// 	numTimeseriesStr := os.Getenv("NUM_TIMESERIES_INIT")
// 	numTimeseriesInit, err := strconv.Atoi(numTimeseriesStr)
// 	if err != nil || numTimeseriesInit == 0 {
// 		numTimeseriesInit = 1000
// 	}
// 	if numTimeseriesInit > 2000 {
// 		numTimeseriesInit = 2000
// 	}

// 	defaultTimeWindow := int64(60 * 1000)
// 	defaultItemWindow := int64(100000)
// 	defaultValueScale := float64(10000)

// 	for i := 0; i < numTimeseriesInit; i++ {
// 		machineID := fmt.Sprintf("machine_%d", i)
// 		lset := labels.FromStrings("machineid", machineID, "fake_metric", "fake_machine_metric")

// 		_ = ps.NewSketchCacheInstance(lset, "avg_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale)
// 		_ = ps.NewSketchCacheInstance(lset, "quantile_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale)
// 		_ = ps.NewSketchCacheInstance(lset, "entropy_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale)
// 	}
// 	log.Printf("Initial sketches created for %d time series.", numTimeseriesInit)
// }

// func main() {
// 	go logIngestionRate()

// 	router := gin.Default()
// 	router.POST("/ingest", handleIngest)
// 	router.GET("/query", handleQuery)
// 	router.GET("/health", func(c *gin.Context) {
// 		c.JSON(http.StatusOK, gin.H{"status": "UP", "message": "PromSketch Go server is running."})
// 	})

// 	// Start pprof server for profiling
// 	go func() {
// 		log.Println(http.ListenAndServe("localhost:6060", nil))
// 	}()

// 	log.Printf("PromSketch Go server listening on :7000")
// 	if err := router.Run(":7000"); err != nil {
// 		log.Fatalf("Failed to run server: %v", err)
// 	}
// }

// var totalIngested int64

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
// 			lsetBuilder := labels.NewBuilder(labels.Labels{})
// 			for k, v := range metric.Labels {
// 				lsetBuilder.Set(k, v)
// 			}
// 			lsetBuilder.Set("fake_metric", metric.Name)
// 			lset := lsetBuilder.Labels()

// 			if err := ps.SketchInsert(lset, payload.Timestamp, metric.Value); err == nil {
// 				atomic.AddInt64(&totalIngested, 1)
// 			}
// 		}(metric)
// 	}
// 	wg.Wait()
// 	duration := time.Since(start).Milliseconds()
// 	log.Printf("[RECEIVED] %d metrics in %.2fms", len(payload.Metrics), float64(duration))

// 	go forwardToCloud(payload)

// 	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": len(payload.Metrics)})
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

// func logIngestionRate() {
// 	var lastTotal int64 = 0
// 	for {
// 		time.Sleep(5 * time.Second)
// 		current := atomic.LoadInt64(&totalIngested)
// 		rate := float64(current-lastTotal) / 5.0
// 		log.Printf("[SERVER SPEED] Received %.2f samples/sec (Total: %d)", rate, current)
// 		lastTotal = current
// 	}
// }

// func handleQuery(c *gin.Context) {
// 	funcName := c.Query("func")
// 	metricName := c.Query("metric")
// 	mintStr := c.Query("mint")
// 	maxtStr := c.Query("maxt")

// 	log.Printf("DEBUG Query: func=%s, metric=%s, mintStr='%s', maxtStr='%s'", funcName, metricName, mintStr, maxtStr)

// 	mint, err := strconv.ParseInt(mintStr, 10, 64)
// 	if err != nil {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'mint' parameter. Must be an integer timestamp in milliseconds."})
// 		return
// 	}
// 	maxt, err := strconv.ParseInt(maxtStr, 10, 64)
// 	if err != nil {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'maxt' parameter. Must be an integer timestamp in milliseconds."})
// 		return
// 	}

// 	otherArgsStr := c.Query("args")
// 	otherArgs := 0.0
// 	if otherArgsStr != "" {
// 		parsedArgs, err := strconv.ParseFloat(otherArgsStr, 64)
// 		if err != nil {
// 			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'args' parameter. Must be a float."})
// 			return
// 		}
// 		otherArgs = parsedArgs
// 	}

// 	lsetBuilder := labels.NewBuilder(labels.Labels{})
// 	for k, v := range c.Request.URL.Query() {
// 		if strings.HasPrefix(k, "label_") {
// 			labelKey := k[len("label_"):]
// 			labelValue := v[0]
// 			lsetBuilder.Set(labelKey, labelValue)
// 		}
// 	}
// 	lsetBuilder.Set("fake_metric", metricName)
// 	lset := lsetBuilder.Labels()

// 	curTime := time.Now().UnixMilli()
// 	if !ps.LookUp(lset, funcName, mint, maxt) {
// 		c.JSON(http.StatusAccepted, gin.H{"status": "pending", "message": "Sketch data not yet available. Try again later."})
// 		return
// 	}

// 	vector, annotations := ps.Eval(funcName, lset, otherArgs, mint, maxt, curTime)
// 	results := []map[string]interface{}{}
// 	for _, sample := range vector {
// 		if !math.IsNaN(sample.F) && sample.T != 0 {
// 			results = append(results, map[string]interface{}{"value": sample.F, "timestamp": sample.T})
// 		}
// 	}
// 	response := gin.H{"status": "success", "data": results}
// 	if len(annotations) > 0 {
// 		response["annotations"] = annotations
// 	}
// 	c.JSON(http.StatusOK, response)
// }

// =================================== TEST Throughput ==========================================================
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "net/http/pprof"

	"github.com/SieDeta/promsketch_std/promsketch"
	"github.com/gin-gonic/gin"
	"github.com/zzylol/prometheus-sketches/model/labels"
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

func init() {
	ps = promsketch.NewPromSketches()
	log.Println("PromSketches instance initialized.")

	numTimeseriesStr := os.Getenv("NUM_TIMESERIES_INIT")
	numTimeseriesInit, err := strconv.Atoi(numTimeseriesStr)
	if err != nil || numTimeseriesInit == 0 {
		numTimeseriesInit = 1000
	}
	if numTimeseriesInit > 2000 {
		numTimeseriesInit = 2000
	}

	defaultTimeWindow := int64(60 * 1000)
	defaultItemWindow := int64(100000)
	defaultValueScale := float64(10000)

	for i := 0; i < numTimeseriesInit; i++ {
		machineID := fmt.Sprintf("machine_%d", i)
		lset := labels.FromStrings("machineid", machineID, "fake_metric", "fake_machine_metric")
		_ = ps.NewSketchCacheInstance(lset, "avg_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale)
		_ = ps.NewSketchCacheInstance(lset, "quantile_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale)
		_ = ps.NewSketchCacheInstance(lset, "entropy_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale)
	}
	log.Printf("Initial sketches created for %d time series.", numTimeseriesInit)
}

func main() {
	go logIngestionRate()

	router := gin.Default()
	router.POST("/ingest", handleIngest)
	router.GET("/query", handleQuery)
	router.GET("/throughput_test", runThroughputTest)
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP", "message": "PromSketch Go server is running."})
	})

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.Printf("PromSketch Go server listening on :7000")
	if err := router.Run(":7000"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}

}

var totalIngested int64

func handleIngest(c *gin.Context) {
	var payload IngestPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		log.Printf("Error binding JSON payload: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid JSON payload: %v", err.Error())})
		return
	}

	start := time.Now()
	var wg sync.WaitGroup

	for _, metric := range payload.Metrics {
		wg.Add(1)
		go func(metric MetricPayload) {
			defer wg.Done()

			lsetBuilder := labels.NewBuilder(labels.Labels{})
			for k, v := range metric.Labels {
				lsetBuilder.Set(k, v)
			}
			lsetBuilder.Set("fake_metric", metric.Name)
			lset := lsetBuilder.Labels()

			if err := ps.SketchInsert(lset, payload.Timestamp, metric.Value); err != nil {
				log.Printf("[ERROR] Failed to insert sketch for labels %v: %v", lset, err)
				return
			}

			atomic.AddInt64(&totalIngested, 1)

		}(metric)
	}

	wg.Wait()
	totalDuration := time.Since(start).Milliseconds()
	log.Printf("[BATCH COMPLETED] Processed %d metrics in %dms", len(payload.Metrics), totalDuration)

	go forwardToCloud(payload)

	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": len(payload.Metrics)})
}

func forwardToCloud(payload IngestPayload) {
	if cloudEndpoint == "" {
		log.Printf("[FORWARD] Skipped: FORWARD_ENDPOINT not set")
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[FORWARD ERROR] marshal: %v", err)
		return
	}
	req, err := http.NewRequest("POST", cloudEndpoint, bytes.NewBuffer(data))
	if err != nil {
		log.Printf("[FORWARD ERROR] request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[FORWARD ERROR] send: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		log.Printf("[FORWARD ERROR] status: %s", resp.Status)
	} else {
		log.Printf("[FORWARD] success: %d bytes sent", len(data))
	}
}

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

func handleQuery(c *gin.Context) {
	funcName := c.Query("func")
	metricName := c.Query("metric")
	mintStr := c.Query("mint")
	maxtStr := c.Query("maxt")

	mint, err := strconv.ParseInt(mintStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'mint' parameter."})
		return
	}
	maxt, err := strconv.ParseInt(maxtStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'maxt' parameter."})
		return
	}

	otherArgs := 0.0
	if argStr := c.Query("args"); argStr != "" {
		otherArgs, _ = strconv.ParseFloat(argStr, 64)
	}

	lsetBuilder := labels.NewBuilder(labels.Labels{})
	for k, v := range c.Request.URL.Query() {
		if strings.HasPrefix(k, "label_") {
			labelKey := k[len("label_"):len(k)]
			lsetBuilder.Set(labelKey, v[0])
		}
	}
	lsetBuilder.Set("fake_metric", metricName)
	lset := lsetBuilder.Labels()

	curTime := time.Now().UnixMilli()
	if !ps.LookUp(lset, funcName, mint, maxt) {
		c.JSON(http.StatusAccepted, gin.H{"status": "pending"})
		return
	}
	vector, annotations := ps.Eval(funcName, lset, otherArgs, mint, maxt, curTime)
	results := []map[string]interface{}{}
	for _, s := range vector {
		if !math.IsNaN(s.F) && s.T != 0 {
			results = append(results, map[string]interface{}{"value": s.F, "timestamp": s.T})
		}
	}
	resp := gin.H{"status": "success", "data": results}
	if len(annotations) > 0 {
		resp["annotations"] = annotations
	}
	c.JSON(http.StatusOK, resp)
}

func runThroughputTest(c *gin.Context) {
	num := 10000
	start := time.Now()

	var wg sync.WaitGroup
	var success int64

	for i := 0; i < num; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			machineID := fmt.Sprintf("bench_%d", i)
			lset := labels.FromStrings("machineid", machineID, "fake_metric", "benchmark_metric")
			timestamp := time.Now().UnixMilli()
			value := float64(i % 1000)

			if err := ps.SketchInsertInsertionThroughputTest(lset, timestamp, value); err == nil {
				atomic.AddInt64(&success, 1)
			} else {
				log.Printf("[Error] SketchInsertInsertionThroughputTest failed: %v", err)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()
	rate := float64(success) / elapsed

	log.Printf("[THROUGHPUT TEST] Inserted %d samples in %.2fs (%.2f samples/sec)", success, elapsed, rate)

	c.JSON(http.StatusOK, gin.H{
		"inserted_samples": success,
		"duration_seconds": elapsed,
		"throughput":       fmt.Sprintf("%.2f samples/sec", rate),
	})
}

// func runThroughputTestTimed(c *gin.Context) {
// 	var (
// 		success int64
// 		stop    int32
// 		wg      sync.WaitGroup
// 	)

// 	// Timer stop: set stop = 1 after 5 seconds
// 	go func() {
// 		time.Sleep(5 * time.Second)
// 		atomic.StoreInt32(&stop, 1)
// 	}()

// 	start := time.Now()
// 	for i := 0; ; i++ {
// 		if atomic.LoadInt32(&stop) == 1 {
// 			break
// 		}
// 		wg.Add(1)
// 		go func(i int) {
// 			defer wg.Done()
// 			machineID := fmt.Sprintf("bench_timed_%d", i)
// 			lset := labels.FromStrings("machineid", machineID, "fake_metric", "benchmark_metric")
// 			timestamp := time.Now().UnixMilli()
// 			value := float64(i % 1000)

// 			if err := ps.SketchInsertInsertionThroughputTest(lset, timestamp, value); err == nil {
// 				atomic.AddInt64(&success, 1)
// 			}
// 		}(i)
// 	}

// 	wg.Wait()
// 	elapsed := time.Since(start).Seconds()
// 	rate := float64(success) / elapsed

// 	log.Printf("[TIMED THROUGHPUT TEST] Inserted %d samples in %.2fs (%.2f samples/sec)", success, elapsed, rate)

// 	c.JSON(http.StatusOK, gin.H{
// 		"inserted_samples": success,
// 		"duration_seconds": elapsed,
// 		"throughput":       fmt.Sprintf("%.2f samples/sec", rate),
// 	})
// }
