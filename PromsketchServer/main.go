package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SieDeta/promsketch_std/promsketch"    // Replace with your actual module path
	"github.com/gin-gonic/gin"                           // Popular Go web framework
	"github.com/zzylol/prometheus-sketches/model/labels" // This path may need to match your project structure
)

// Structure for the metric data payload received from the Python Ingester
type IngestPayload struct {
	Timestamp int64           `json:"timestamp"` // Time in milliseconds (Prometheus-compatible)
	Metrics   []MetricPayload `json:"metrics"`
}

// Structure for each metric in the payload
type MetricPayload struct {
	Name   string            `json:"name"`   // Metric name (e.g., "fake_machine_metric")
	Labels map[string]string `json:"labels"` // Metric labels (e.g., {"machineid": "machine_0"})
	Value  float64           `json:"value"`  // Metric value
}

// Global PromSketches instance
var ps *promsketch.PromSketches

func init() {
	ps = promsketch.NewPromSketches()
	log.Println("PromSketches instance initialized.")

	// Get the number of time series from environment variable or default (for testing)
	// For production use, this may come from configuration.
	// If no value is provided, use a safe default.
	numTimeseriesStr := os.Getenv("NUM_TIMESERIES_INIT")
	numTimeseriesInit, err := strconv.Atoi(numTimeseriesStr)
	if err != nil || numTimeseriesInit == 0 {
		numTimeseriesInit = 1000 // Default, matching your EvalData.py example
	}
	if numTimeseriesInit > 2000 { // Limit to avoid excessive memory use on startup
		numTimeseriesInit = 2000
	}

	defaultTimeWindow := int64(60 * 1000) // 60 seconds * 1000 ms/second = 60000 ms
	defaultItemWindow := int64(100000)
	defaultValueScale := float64(10000)

	// Initialize sketches for all expected time series
	for i := 0; i < numTimeseriesInit; i++ {
		machineID := fmt.Sprintf("machine_%d", i)
		lset := labels.FromStrings("machineid", machineID, "fake_metric", "fake_machine_metric")

		// Initialize sketches for all expected query functions
		if err := ps.NewSketchCacheInstance(lset, "avg_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale); err != nil {
			log.Printf("Error creating sketch for avg_over_time on %v: %v", lset, err)
		}
		if err := ps.NewSketchCacheInstance(lset, "quantile_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale); err != nil {
			log.Printf("Error creating sketch for quantile_over_time on %v: %v", lset, err)
		}
		if err := ps.NewSketchCacheInstance(lset, "entropy_over_time", defaultTimeWindow, defaultItemWindow, defaultValueScale); err != nil {
			log.Printf("Error creating sketch for entropy_over_time on %v: %v", lset, err)
		}
	}
	log.Printf("Initial sketches created for %d time series.", numTimeseriesInit)
}

func main() {
	router := gin.Default()

	// Endpoint to receive metric data from the Python Ingester
	// Data is sent as a JSON-formatted POST request
	router.POST("/ingest", handleIngest)

	// Endpoint to query sketch data (acts as a PromQL replacement)
	// Queries use a GET request with URL parameters
	router.GET("/query", handleQuery)

	// Simple endpoint to check server status
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP", "message": "PromSketch Go server is running."})
	})

	log.Printf("PromSketch Go server listening on :7000")
	if err := router.Run(":7000"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}

var totalIngested int64

// handleIngest receives metric data from custom_data_ingester.py
func handleIngest(c *gin.Context) {
	var payload IngestPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		log.Printf("Error binding JSON payload: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid JSON payload: %v", err.Error())})
		return
	}

	var ingestedCount int
	for _, metric := range payload.Metrics {
		// Build labels.Labels from map[string]string
		lsetBuilder := labels.NewBuilder(labels.Labels{})
		// Ensure all important labels for time series identification are included
		// You may want to ensure "fake_metric" and "machineid" are always present
		for k, v := range metric.Labels {
			lsetBuilder.Set(k, v)
		}
		lsetBuilder.Set("fake_metric", metric.Name)
		lset := lsetBuilder.Labels()

		// Call SketchInsert from your PromSketches implementation
		// This updates all relevant sketches for the given lset, timestamp, and value
		// Assumes NewSketchCacheInstance has already been called during startup
		// or is called on-demand if you support dynamic creation

		err := ps.SketchInsert(lset, payload.Timestamp, metric.Value)
		if err != nil {
			log.Printf("Failed to insert data for %v (timestamp %d, value %.2f): %v", lset, payload.Timestamp, metric.Value, err)
			// Don't return error here to allow batch processing to continue
			continue
		}
		ingestedCount++
	}

	totalIngested += int64(ingestedCount)
	log.Printf("Batch ingested: %d, Total ingested: %d", ingestedCount, totalIngested)

	c.JSON(http.StatusOK, gin.H{"status": "success", "ingested_metrics_count": ingestedCount})
	log.Printf("Successfully ingested %d metrics.", ingestedCount)
}

// handleQuery processes query requests from EvalData.py
func handleQuery(c *gin.Context) {
	funcName := c.Query("func")
	metricName := c.Query("metric")

	mintStr := c.Query("mint")
	maxtStr := c.Query("maxt")

	log.Printf("DEBUG Query: func=%s, metric=%s, mintStr='%s', maxtStr='%s'", funcName, metricName, mintStr, maxtStr)

	mint, err := strconv.ParseInt(mintStr, 10, 64)
	if err != nil {
		log.Printf("ERROR: Failed to parse 'mint' parameter '%s': %v", mintStr, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'mint' parameter. Must be an integer timestamp in milliseconds."})
		return
	}
	maxt, err := strconv.ParseInt(maxtStr, 10, 64)
	if err != nil {
		log.Printf("ERROR: Failed to parse 'maxt' parameter '%s': %v", maxtStr, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'maxt' parameter. Must be an integer timestamp in milliseconds."})
		return
	}

	otherArgsStr := c.Query("args")
	otherArgs := 0.0
	if otherArgsStr != "" {
		parsedArgs, err := strconv.ParseFloat(otherArgsStr, 64)
		if err != nil {
			log.Printf("[Error] Failed to parse args: %v", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'args' parameter. Must be a float."})
			return
		}
		otherArgs = parsedArgs
	}
	log.Printf("[Query] args=%.4f", otherArgs)

	// Build label set
	lsetBuilder := labels.NewBuilder(labels.Labels{})
	for k, v := range c.Request.URL.Query() {
		if strings.HasPrefix(k, "label_") {
			labelKey := k[len("label_"):]
			labelValue := v[0]
			lsetBuilder.Set(labelKey, labelValue)
			log.Printf("[Label] %s=%s", labelKey, labelValue)
		}
	}
	lsetBuilder.Set("fake_metric", metricName)
	lset := lsetBuilder.Labels()
	log.Printf("[LabelSet] Final lset: %v", lset)

	curTime := time.Now().UnixMilli()
	isCovered := ps.LookUp(lset, funcName, mint, maxt)
	if !isCovered {
		log.Printf("[Sketch] Data NOT covered for range [%d, %d] on %v for func=%s", mint, maxt, lset, funcName)
		c.JSON(http.StatusAccepted, gin.H{
			"status":  "pending",
			"message": "Sketch data not yet available. Try again later.",
		})
		return
	}
	log.Printf("[Sketch] Data covered for range [%d, %d] on %v for func=%s", mint, maxt, lset, funcName)

	vector, annotations := ps.Eval(funcName, lset, otherArgs, mint, maxt, curTime)
	log.Printf("[Eval] Raw result length: %d", len(vector))

	// Filter out NaN or invalid results
	results := []map[string]interface{}{}
	for i, sample := range vector {
		if math.IsNaN(sample.F) || sample.T == 0 {
			log.Printf("[Eval] Skipping invalid sample #%d: timestamp=%d, value=%.4f", i, sample.T, sample.F)
			continue
		}
		results = append(results, map[string]interface{}{
			"value":     sample.F,
			"timestamp": sample.T,
		})
	}

	// Prepare JSON response
	response := gin.H{
		"status": "success",
		"data":   results,
	}
	if annotations != nil && len(annotations) > 0 {
		response["annotations"] = annotations
		log.Printf("[Eval] Annotations: %+v", annotations)
	}

	if len(results) == 0 {
		log.Printf("[Query] All samples are invalid or sketch not yet populated. Returning empty result.")
	}

	c.Header("Content-Type", "application/json")
	c.JSON(http.StatusOK, response)
	log.Printf("[Query] func=%s on lset=%v (range %d-%d) returned %d valid result(s).", funcName, lset, mint, maxt, len(results))
}
