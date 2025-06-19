package promsketch

import (
	"hash"
	"hash/fnv"

	// "bytes"
	// "encoding/binary"
	// "encoding/json"
	// "io"
	"math"
	// "os"
	"errors"
	// "fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/spaolacci/murmur3"
)

// CountSketch struct. row is the number of hashing functions.
// col is the size of every hash table
// count, a matrix, is used to store the count.
// int is used to store the count, the maximum count is  (1<<32)-1 in 32-bit OS, and (1<<64)-1 in 64-bit OS.
type CountSketch struct {
	row        int
	col        int
	count      [][]float64
	hasher     hash.Hash64
	topK       *TopKHeap
	seeds      []uint32
	sign_seeds []uint32
	bucketsize int   // for size-based sliding window model; per sketch
	min_time   int64 // for time-based sliding window model; per sketch
	max_time   int64 // for time-based sliding window model; per sketch
	l2         []float64
}

// New create a new Count Sketch with row hasing funtions and col counters per row.
func NewCountSketch(row int, col int, seed1, seed2 []uint32) (s *CountSketch, err error) {
	if row <= 0 || col <= 0 {
		return nil, errors.New("CountSketch New: values of row and col should be positive.")
	}

	if row > CS_ROW_NO_Univ_ELEPHANT {
		row = CS_ROW_NO_Univ_ELEPHANT
	}

	if col > CS_COL_NO_Univ_ELEPHANT {
		col = CS_COL_NO_Univ_ELEPHANT
	}

	s = &CountSketch{
		row:    row,
		col:    col,
		hasher: fnv.New64(),
		topK:   NewTopKHeap(TOPK_SIZE),
	}

	// Create matrix
	/*
		s.count = make([][]float64, row)
		s.sum = make([][]float64, row)
		s.sum2 = make([][]float64, row)
		s.l1 = make([]float64, row)
		s.l2 = make([]float64, row)
		for r := 0; r < row; r++ {
			s.count[r] = make([]float64, col)
			s.sum[r] = make([]float64, col)
			s.sum2[r] = make([]float64, col)
			for c := 0; c < col; c++ {
				s.count[r][c] = 0
				s.sum[r][c] = 0
				s.sum2[r][c] = 0
			}
			s.l1[r] = 0
			s.l2[r] = 0
		}
	*/

	s.count = farr2Pool.Get()
	s.l2 = farrPool.Get()

	// Generate seeds for positions and signs
	/*
		rand.Seed(time.Now().UnixNano())
		for r := 0; r < row; r++ {
			s.seeds[r] = rand.Uint32()
			s.sign_seeds[r] = rand.Uint32()
		}
	*/
	s.seeds = make([]uint32, row)
	s.sign_seeds = make([]uint32, row)
	for r := 0; r < row; r++ {
		s.seeds[r] = seed1[r]
		s.sign_seeds[r] = seed2[r]
	}

	return s, nil
}

func (s *CountSketch) FreeCountSketch() error {
	for r := 0; r < s.row; r++ {
		for c := 0; c < s.col; c++ {
			s.count[r][c] = 0
		}
		s.l2[r] = 0
	}
	farr2Pool.Put(s.count)
	farrPool.Put(s.l2)
	s.topK.Clean()
	return nil
}

// NewWithEstimates creates a new Count Sketch with given error rate and condifence.
// Accuracy guarantees will be made in terms of a pair of user specified parameters,
// ε and δ, meaning that the error in answering a query is within a factor of ε with
// probability δ.
func NewCountSketchWithEstimates(epsilon, delta float64) (s *CountSketch, err error) {
	if epsilon <= 0 || epsilon >= 1 {
		return nil, errors.New("CountSketch NewWithEstiamtes: value of epsilon should be in range (0,1).")
	}
	if delta <= 0 || delta >= 1 {
		return nil, errors.New("CountSketch NewWithEstimates: value of delta should be in range (0,1).")
	}

	row := int(math.Ceil(2.72 / epsilon / epsilon))
	col := int(math.Ceil(math.Log(delta) / math.Log(0.5))) // e.g., delta = 0.05

	seed1 := make([]uint32, row)
	seed2 := make([]uint32, row)
	rand.Seed(time.Now().UnixNano())
	for r := 0; r < row; r++ {
		seed1[r] = rand.Uint32()
		seed2[r] = rand.Uint32()
	}

	return NewCountSketch(row, col, seed1, seed2)
}

// Row returns the number of rows (hash functions)
func (s *CountSketch) Row() int { return s.row }

// Col returns the number of colums
func (s *CountSketch) Col() int { return s.col }

func (s *CountSketch) position_and_sign(key []byte) (pos []int32, sign []int32) {
	pos = make([]int32, s.row)
	sign = make([]int32, s.row)
	var hash1, hash2 uint32
	for i := uint32(0); i < uint32(s.row); i++ {
		hash1 = murmur3.Sum32WithSeed(key, s.seeds[i])
		hash2 = murmur3.Sum32WithSeed(key, hash1)
		pos[i] = int32((hash1 + i*hash2) % uint32(s.col))
		sign[i] = int32(murmur3.Sum32WithSeed(key, s.sign_seeds[i]) % 2)
		sign[i] = sign[i]*2 - 1
	}
	return pos, sign
}

func (s *CountSketch) UpdateString(key string, count float64) {
	pos, sign := s.position_and_sign([]byte(key))
	counters := make([]float64, s.row)
	for r, c := range pos {
		cur_count := s.count[r][c]
		s.count[r][c] += float64(sign[r])
		s.l2[r] += s.count[r][c]*s.count[r][c] - cur_count*cur_count
		counters[r] = float64(sign[r]) * s.count[r][c]
	}

	sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
	var median float64 = 0
	if s.row%2 == 0 {
		median = (counters[s.row/2] + counters[s.row/2-1]) / 2.0
	} else {
		median = counters[s.row/2]
	}
	s.topK.UpdateCS(key, int64(median))
}

func (s *CountSketch) EstimateStringCount(key string) int64 {
	pos, sign := s.position_and_sign([]byte(key))
	counters := make([]float64, s.row)
	for r, c := range pos {
		counters[r] = float64(sign[r]) * s.count[r][c]
	}

	sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
	var median float64 = 0
	if s.row%2 == 0 {
		median = (counters[s.row/2] + counters[s.row/2-1]) / 2.0
	} else {
		median = counters[s.row/2]
	}

	return int64(median)
}

func (s *CountSketch) UpdateAndEstimateString(key string, count float64) float64 {
	pos, sign := s.position_and_sign([]byte(key))
	for r, c := range pos {
		s.count[r][c] += float64(sign[r]) * count
	}

	counters := make([]float64, s.row)
	for r, c := range pos {
		counters[r] = float64(sign[r]) * s.count[r][c]
	}

	sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
	var median float64 = 0
	if s.row%2 == 0 {
		median = (counters[s.row/2] + counters[s.row/2-1]) / 2.0
	} else {
		median = counters[s.row/2]
	}
	s.topK.UpdateCS(key, int64(median))
	return median
}

func (s *CountSketch) cs_l1() float64 {

	sos := make([]float64, s.row)
	for i := 0; i < s.row; i++ {
		sos[i] = 0
		for j := 0; j < s.col; j++ {
			sos[i] += AbsFloat64(s.count[i][j])
		}
	}

	sort.Slice(sos, func(i, j int) bool { return sos[i] < sos[j] })
	f1_value := sos[s.row/2]

	return f1_value
}

func (s *CountSketch) cs_l2() float64 {

	sos := make([]float64, s.row)
	for i := 0; i < s.row; i++ {
		sos[i] = s.l2[i]
	}

	sort.Slice(sos, func(i, j int) bool { return sos[i] < sos[j] })
	f2_value := sos[s.row/2]

	return math.Sqrt(f2_value)
}

func (s *CountSketch) cs_l2_new() float64 {
	sos := make([]float64, s.row)
	for i := 0; i < s.row; i++ {
		sos[i] = 0
		for j := 0; j < s.col; j++ {
			sos[i] += s.count[i][j] * s.count[i][j]
		}
	}
	sort.Slice(sos, func(i, j int) bool { return sos[i] < sos[j] })
	f2_value := sos[s.row/2]
	return math.Sqrt(f2_value)
}

func (s *CountSketch) MergeWith(other *CountSketch) {
	for i := 0; i < s.row; i++ {
		for j := 0; j < s.col; j++ {
			s.count[i][j] += other.count[i][j]
		}
	}

	if s.topK != nil {

		topk := NewTopKHeap(s.topK.k)
		for _, item := range s.topK.heap {
			topk.Update(item.key, item.count)
		}

		for _, item := range other.topK.heap {
			index, find := topk.Find(item.key)
			var count int64 = 0
			if find {
				count = topk.heap[index].count + item.count
			} else {
				count = item.count
			}
			topk.Update(item.key, count)
			// fmt.Println("!!", item.key, us.cs_layers[i].EstimateStringCount(item.key))
		}
		s.topK = NewTopKFromHeap(topk)
	}
}
