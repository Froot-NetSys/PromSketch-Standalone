package promsketch

import (

	// "bytes"
	// "encoding/binary"
	// "encoding/json"
	// "io"
	"math"
	// "os"
	"errors"
	// "fmt"
	"math/rand"
	"time"

	"github.com/OneOfOne/xxhash"
)

// CountSketchUniv struct. row is the number of hashing functions.
// col is the size of every hash table
// count, a matrix, is used to store the count.
// int is used to store the count, the maximum count is  (1<<32)-1 in 32-bit OS, and (1<<64)-1 in 64-bit OS.
type CountSketchUniv struct {
	row        int
	col        int
	count      [][]int64
	l2         []int64
	seeds      []uint32
	sign_seeds []uint32
}

// New create a new Count Sketch with row hasing funtions and col counters per row.
func NewCountSketchUniv(row int, col int, seed1, seed2 []uint32) (s *CountSketchUniv, err error) {
	if row <= 0 || col <= 0 {
		return nil, errors.New("CountSketchUniv New: values of row and col should be positive")
	}

	if row > CS_ROW_NO_Univ_ELEPHANT {
		row = CS_ROW_NO_Univ_ELEPHANT
	}

	if col > CS_COL_NO_Univ_ELEPHANT {
		col = CS_COL_NO_Univ_ELEPHANT
	}

	s = &CountSketchUniv{
		row: row,
		col: col,
	}

	if row == CS_ROW_NO_Univ_ELEPHANT {
		s.count = iarr2Pool_ele.Get()
		s.l2 = iarrPool_ele.Get()
	} else {
		s.count = iarr2Pool_mice.Get()
		s.l2 = iarrPool_mice.Get()
	}

	s.seeds = make([]uint32, row)
	s.sign_seeds = make([]uint32, row)
	for r := 0; r < row; r++ {
		s.seeds[r] = seed1[r]
		s.sign_seeds[r] = seed2[r]
	}

	return s, nil
}

func (s *CountSketchUniv) CleanCountSketchUniv() error {
	for r := 0; r < s.row; r++ {
		s.count[r][0] = 0
		for c := 1; c < s.col; c *= 2 {
			copy(s.count[r][c:], s.count[r][:c])
		}
		s.l2[r] = 0
	}
	return nil
}

func (s *CountSketchUniv) FreeCountSketchUniv() error {
	for r := 0; r < s.row; r++ {
		s.count[r][0] = 0
		for c := 1; c < s.col; c *= 2 {
			copy(s.count[r][c:], s.count[r][:c])
		}
		s.l2[r] = 0
	}
	if s.row == CS_ROW_NO_Univ_ELEPHANT {
		iarr2Pool_ele.Put(s.count)
		iarrPool_ele.Put(s.l2)
	} else {
		iarr2Pool_mice.Put(s.count)
		iarrPool_mice.Put(s.l2)
	}
	return nil
}

// NewWithEstimates creates a new Count Sketch with given error rate and condifence.
// Accuracy guarantees will be made in terms of a pair of user specified parameters,
// ε and δ, meaning that the error in answering a query is within a factor of ε with
// probability δ.
func NewCountSketchUnivWithEstimates(epsilon, delta float64) (s *CountSketchUniv, err error) {
	if epsilon <= 0 || epsilon >= 1 {
		return nil, errors.New("CountSketchUniv NewWithEstiamtes: value of epsilon should be in range (0,1)")
	}
	if delta <= 0 || delta >= 1 {
		return nil, errors.New("CountSketchUniv NewWithEstimates: value of delta should be in range (0,1)")
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

	return NewCountSketchUniv(row, col, seed1, seed2)
}

// Row returns the number of rows (hash functions)
func (s *CountSketchUniv) Row() int { return s.row }

// Col returns the number of colums
func (s *CountSketchUniv) Col() int { return s.col }

func (s *CountSketchUniv) position_and_sign(key []byte) (pos []int16, sign []int8) {
	pos = make([]int16, s.row)
	sign = make([]int8, s.row)
	for i := uint32(0); i < uint32(s.row); i++ {
		hash1 := xxhash.Checksum32S(key, s.seeds[i])
		hash2 := xxhash.Checksum32S(key, s.sign_seeds[i])
		pos[i] = int16(hash1 % uint32(s.col))
		// hash2 := wyhash.Hash(key, uint64(s.sign_seeds[i]))
		sign[i] = int8((hash2 % 2))
		sign[i] = 1 - sign[i]*2
	}
	return pos, sign
}

func (s *CountSketchUniv) UpdateString(key string, count int64, pos []int16, sign []int8) {
	for r, c := range pos {
		cur_count := s.count[r][c]
		s.count[r][c] += int64(sign[r]) * count
		s.l2[r] += s.count[r][c]*s.count[r][c] - cur_count*cur_count
	}
}

func (s *CountSketchUniv) UpdateStringNoL2(key string, count int64, pos []int16, sign []int8) {
	for r, c := range pos {
		s.count[r][c] += int64(sign[r]) * count
	}
}

func (s *CountSketchUniv) EstimateStringCount(key string) int64 {
	pos, sign := s.position_and_sign([]byte(key))
	// return int64(sign[0]) * int64(s.count[0][pos[0]])
	counters := make([]int64, s.row)
	for r, c := range pos {
		counters[r] = int64(sign[r]) * int64(s.count[r][c])
	}

	return MedianOfThree(counters[0], counters[1], counters[2])

	/*
		sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
		var median int64 = 0
		if s.row%2 == 0 {
			median = (counters[s.row/2] + counters[s.row/2-1]) / 2.0
		} else {
			median = counters[s.row/2]
		}
		return median
	*/
}

func (s *CountSketchUniv) UpdateAndEstimateString(key string, count int64, pos *([]int16), sign *([]int8)) int64 {

	for r, c := range *pos {
		cur_count := s.count[r][c]
		s.count[r][c] += int64((*sign)[r]) * count
		s.l2[r] += s.count[r][c]*s.count[r][c] - cur_count*cur_count
	}
	// fmt.Println("l2 in update:", s.l2)

	// return int64(sign[0]) * int64(s.count[0][pos[0]])

	counters := make([]int64, s.row)
	for r, c := range *pos {
		counters[r] = int64((*sign)[r]) * int64(s.count[r][c])
	}

	return MedianOfThree(counters[0], counters[1], counters[2])

	/*
		sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
		var median int64 = 0
		if s.row%2 == 0 {
			median = (counters[s.row/2] + counters[s.row/2-1]) / 2.0
		} else {
			median = counters[s.row/2]
		}
		return median
	*/
}

func (s *CountSketchUniv) UpdateAndEstimateStringNoL2(key string, count int64, pos *([]int16), sign *([]int8)) int64 {

	for r, c := range *pos {
		s.count[r][c] += int64((*sign)[r]) * count
	}

	// return int64(sign[0]) * int64(s.count[0][pos[0]])

	counters := make([]int64, s.row)
	for r, c := range *pos {
		counters[r] = int64((*sign)[r]) * int64(s.count[r][c])
	}

	return MedianOfThree(counters[0], counters[1], counters[2])

	/*
		sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
		var median int64 = 0
		if s.row%2 == 0 {
			median = (counters[s.row/2] + counters[s.row/2-1]) / 2.0
		} else {
			median = counters[s.row/2]
		}
		return median
	*/
}

func (s *CountSketchUniv) cs_l22() float64 {
	/*
		l2 := make([]int64, CS_ROW_NO_Univ_ELEPHANT)
		for r := 0; r < CS_ROW_NO_Univ_ELEPHANT; r++ {
			for c := 0; c < CS_COL_NO_Univ_ELEPHANT; c++ {
				l2[r] += s.count[r][c] * s.count[r][c]
			}
		}

		f2_value := MedianOfThree(l2[0], l2[1], l2[2])
	*/
	f2_value := MedianOfThree(s.l2[0], s.l2[1], s.l2[2])

	return float64(f2_value)
}

func (s *CountSketchUniv) cs_l2() float64 {
	/*
		l2 := make([]int64, CS_ROW_NO_Univ_ELEPHANT)
		for r := 0; r < CS_ROW_NO_Univ_ELEPHANT; r++ {
			for c := 0; c < CS_COL_NO_Univ_ELEPHANT; c++ {
				l2[r] += s.count[r][c] * s.count[r][c]
			}
		}

		f2_value := MedianOfThree(l2[0], l2[1], l2[2])
	*/
	f2_value := MedianOfThree(s.l2[0], s.l2[1], s.l2[2])

	return math.Sqrt(float64(f2_value))
}

func (s *CountSketchUniv) UpdateIntCount(key uint32, count int64) {
	pos, sign := s.position_and_sign(i32tob(key))
	for r, c := range pos {
		// cur_count := s.count[r][c]
		s.count[r][c] += int64(sign[r]) * count
		// s.sums[r] += s.count[r][c] * s.count[r][c] - cur_count * cur_count
	}
}

func (s *CountSketchUniv) EstimateIntCount(key uint32) float64 {
	pos, sign := s.position_and_sign(i32tob(key))
	counters := make([]int64, s.row)
	for r, c := range pos {
		counters[r] = int64(sign[r]) * s.count[r][c]
	}

	return float64(MedianOfThree(counters[0], counters[1], counters[2]))
	/*
		sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
		var median float64 = 0
		if s.row%2 == 0 {
			median = float64(counters[s.row/2]+counters[s.row/2-1]) / 2.0
		} else {
			median = float64(counters[s.row/2])
		}
		return median
	*/
}

func (s *CountSketchUniv) MergeWith(other CountSketchUniv) {
	for i := 0; i < s.row; i++ {
		for j := 0; j < s.col; j++ {
			s.count[i][j] += other.count[i][j]
		}
	}
}
