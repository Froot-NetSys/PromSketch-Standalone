// Stress test harness that hammers PromSketch inserts with synthetic data.

// package main

// import (
// 	"bytes"
// 	"encoding/json"
// 	"fmt"
// 	"math/rand"
// 	"net/http"
// 	"sync"
// 	"time"
// )

// const (
// 	endpoint     = "http://localhost:7000/ingest"
// 	duration     = 10 * time.Second
// 	sketchCount  = 2
// 	machineCount = 10
// )

// var (
// 	sketchTypes = []string{"EHKLL", "EHUNIV"}
// )

// type Metric struct {
// 	Name   string            `json:"name"`
// 	Labels map[string]string `json:"labels"`
// 	Ts     int64             `json:"ts"`
// 	Value  float64           `json:"value"`
// }

// type Payload struct {
// 	Metrics []Metric `json:"metrics"`
// }

// func generatePayload() Payload {
// 	ts := time.Now().UnixMilli()
// 	var metrics []Metric
// 	for _, sketchType := range sketchTypes {
// 		for i := 0; i < machineCount; i++ {
// 			metric := Metric{
// 				Name: "fake_machine_metric",
// 				Labels: map[string]string{
// 					"machineid":  fmt.Sprintf("machine_%d", i),
// 					"sketchtype": sketchType,
// 				},
// 				Ts:    ts,
// 				Value: float64(rand.Intn(100_000)),
// 			}
// 			metrics = append(metrics, metric)
// 		}
// 	}
// 	return Payload{Metrics: metrics}
// }

// func stressLoop(wg *sync.WaitGroup, id int, counter *int) {
// 	defer wg.Done()
// 	ticker := time.NewTicker(500 * time.Millisecond)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		payload := generatePayload()
// 		jsonData, _ := json.Marshal(payload)

// 		resp, err := http.Post(endpoint, "application/json", bytes.NewBuffer(jsonData))
// 		if err == nil {
// 			resp.Body.Close()
// 		}
// 		*counter += len(payload.Metrics)
// 		if time.Since(startTime) > duration {
// 			break
// 		}
// 	}
// }

// var startTime time.Time

// func main() {
// 	startTime = time.Now()
// 	var wg sync.WaitGroup
// 	totalInserted := 0

// 	threadCount := 5
// 	wg.Add(threadCount)

// 	for i := 0; i < threadCount; i++ {
// 		go stressLoop(&wg, i, &totalInserted)
// 	}

// 	wg.Wait()

// 	elapsed := time.Since(startTime).Seconds()
// 	fmt.Printf("Stress Test Finished: %.2f seconds\n", elapsed)
// 	fmt.Printf("Total samples inserted: %d\n", totalInserted)
// 	fmt.Printf("Average ingestion speed: %.2f samples/sec\n", float64(totalInserted)/elapsed)
// }

package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Froot-NetSys/Promsketch-Standalone/promsketch"

	"github.com/zzylol/prometheus-sketch-VLDB/prometheus-sketches/model/labels"
)

const (
	numClients      = 100 // Number of parallel goroutines
	insertPerClient = 10000
)

var (
	ps *promsketch.PromSketches
)

func generateLabel(i int) labels.Labels {
	builder := labels.NewBuilder(labels.Labels{})
	builder.Set("machineid", fmt.Sprintf("machine_%d", i))
	builder.Set(labels.MetricName, "cpu_usage")
	return builder.Labels()
}

func insertToSketch(clientID int, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := 0; j < insertPerClient; j++ {
		lset := generateLabel(j)
		timestamp := time.Now().UnixMilli()
		value := rand.Float64() * 100

		err := ps.SketchInsertDefinedRules(lset, timestamp, value)
		if err != nil {
			log.Printf("[Client %d] Failed to insert: %v", clientID, err)
		}
	}
}

func main() {
	fmt.Println("Initializing sketch system...")
	ps = promsketch.NewPromSketches()

	// STRESS TEST OVERVIEW:
	// - Pre-create 1,000 logical series with four sketch types.
	// - Launch 100 parallel goroutines; each issues 10,000 inserts.
	// - Measures server robustness by flooding SketchInsertDefinedRules with concurrent writes.
	// OPTIONAL: Proactively create sketches ahead of time (if they don’t exist yet)
	fmt.Println("Creating sketches for initial series...")
	for i := 0; i < 1000; i++ {
		lset := generateLabel(i)
		series, _, _ := ps.GetOrCreateWrapper(lset.Hash(), lset)

		// Pre-create every sketch variant so inserts exercise populated structures
		sketchTypes := []promsketch.SketchType{
			promsketch.EHUniv,
			promsketch.EHCount,
			promsketch.EHDD,
			promsketch.EffSum,
		}
		config := &promsketch.SketchConfig{
			EH_univ_config:  promsketch.EHUnivConfig{K: 20, Time_window_size: 1000000},
			EH_kll_config:   promsketch.EHKLLConfig{K: 50, Kll_k: 256, Time_window_size: 1000000},
			Sampling_config: promsketch.SamplingConfig{Sampling_rate: 0.05, Time_window_size: 1000000, Max_size: 50000},
		}

		for _, stype := range sketchTypes {
			_ = ps.NewSketchInstanceWrapper(series, stype, config)
		}
	}

	fmt.Printf("Running %d parallel ingestion clients, each inserting %d times...\n", numClients, insertPerClient)

	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go insertToSketch(i, &wg)
	}
	wg.Wait()

	fmt.Println("Stress insert completed.")
}
