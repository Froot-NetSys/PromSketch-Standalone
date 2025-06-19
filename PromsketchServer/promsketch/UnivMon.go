package promsketch

import (
	// "fmt"

	"fmt"
	"math"
	"unsafe"

	"github.com/OneOfOne/xxhash"
)

/*
Can be used for Prometheus functions: count_over_time, entropy_over_time (newly added), hh(topk)_over_time (newly added),
 card_over_time (newly added), sum_over_time, avg_over_time, stddev_over_time, stdvar_over_time, min_over_time, max_over_time
*/

type UnivSketch struct {
	k           int // topK
	row         int
	col         int
	layer       int
	cs_layers   []*CountSketchUniv
	HH_layers   []*TopKHeap
	seed        uint32 // one hash for all layers
	max_time    int64  // for sliding window model based on time; per sketch
	min_time    int64  // for sliding window model based on time; per sketch
	pool_idx    int64
	heap_update int
	bucket_size int64
}

func (us *UnivSketch) GetBucketSize() int64 {
	return us.bucket_size
}

// New create a new Universal Sketch with row hashing funtions and col counters per row of a Count Sketch.
func NewUnivSketch(k, row, col, layer int, seed1, seed2 []uint32, seed3 uint32, pool_idx int64) (us *UnivSketch, err error) {

	us = &UnivSketch{
		k:           k,
		row:         row,
		col:         col,
		layer:       layer,
		pool_idx:    pool_idx,
		heap_update: 0,
		seed:        seed3,
		bucket_size: 0,
	}

	us.cs_layers = make([]*CountSketchUniv, layer)
	us.HH_layers = make([]*TopKHeap, layer)

	for i := 0; i < layer; i++ {
		us.cs_layers[i], _ = NewCountSketchUniv(row, col, seed1, seed2)
	}

	for i := 0; i < layer; i++ {
		us.HH_layers[i] = NewTopKHeap(k)
	}

	return us, nil
}

func NewUnivSketchPyramid(k, row, col, layer int, seed1, seed2 []uint32, seed3 uint32, pool_idx int64) (us *UnivSketch, err error) {

	us = &UnivSketch{
		k:           k,
		row:         row,
		col:         col,
		layer:       layer,
		pool_idx:    pool_idx,
		heap_update: 0,
		seed:        seed3,
		bucket_size: 0,
	}

	us.cs_layers = make([]*CountSketchUniv, layer)
	us.HH_layers = make([]*TopKHeap, layer)

	if layer <= ELEPHANT_LAYER {
		for i := 0; i < layer; i++ {
			us.cs_layers[i], _ = NewCountSketchUniv(CS_ROW_NO_Univ_ELEPHANT, CS_COL_NO_Univ_ELEPHANT, seed1, seed2)
		}
		for i := 0; i < layer; i++ {
			us.HH_layers[i] = NewTopKHeap(k)
		}
	} else {
		for i := 0; i < ELEPHANT_LAYER; i++ {
			us.cs_layers[i], _ = NewCountSketchUniv(CS_ROW_NO_Univ_ELEPHANT, CS_COL_NO_Univ_ELEPHANT, seed1, seed2)
		}
		for i := 0; i < ELEPHANT_LAYER; i++ {
			us.HH_layers[i] = NewTopKHeap(TOPK_SIZE)
		}
		for i := ELEPHANT_LAYER; i < layer; i++ {
			us.cs_layers[i], _ = NewCountSketchUniv(CS_ROW_NO_Univ_MICE, CS_COL_NO_Univ_MICE, seed1, seed2)
		}
		for i := ELEPHANT_LAYER; i < layer; i++ {
			us.HH_layers[i] = NewTopKHeap(TOPK_SIZE_MICE)
		}
	}

	return us, nil
}

func (us *UnivSketch) Free() {
	us.bucket_size = 0

	for i := 0; i < us.layer; i++ {
		us.cs_layers[i].CleanCountSketchUniv()
		us.HH_layers[i].Clean()
	}
}

func (us *UnivSketch) GetMemoryKB() float64 {
	var total_topk float64 = 0

	for i := 0; i < us.layer; i++ {
		total_topk += float64(unsafe.Sizeof(us.HH_layers[i]))
	}
	return (float64(CS_COL_NO_Univ_ELEPHANT)*float64(CS_ROW_NO_Univ_ELEPHANT)*float64(us.layer)*8 + total_topk) / 1024
}

func (us *UnivSketch) GetMemoryKBPyramid() float64 {
	var total_topk float64 = 0

	total_topk += (32 + 32 + 32 + 32 + 32 + 64 + 64 + 64 + 32 + 64) / 8
	for i := 0; i < us.layer; i++ {
		total_topk += float64(unsafe.Sizeof(us.HH_layers[i]))
	}
	if us.layer <= ELEPHANT_LAYER {
		return (float64(CS_COL_NO_Univ_ELEPHANT)*float64(CS_ROW_NO_Univ_ELEPHANT)*float64(us.layer)*8 + total_topk) / 1024
	} else {
		return ((float64(CS_COL_NO_Univ_ELEPHANT)*float64(CS_ROW_NO_Univ_ELEPHANT)*float64(ELEPHANT_LAYER)+float64(CS_COL_NO_Univ_MICE)*float64(CS_ROW_NO_Univ_MICE)*float64(us.layer-ELEPHANT_LAYER))*8 + total_topk) / 1024
	}
}

// Update Universal Sketch

// find the last possible layer for each key
func findBottomLayerNum(hash uint64, layer int) int {
	// optimization -- hash only once
	// if hash mod 2 == 1, go down
	for l := 1; l < layer; l++ {
		if ((hash >> l) & 1) == 0 {
			return l - 1
		}
	}
	return layer - 1
}

// update multiple layers from top to bottom_layer_num
// insert a key into Universal Sketch
func (us *UnivSketch) update(key string, value int64, bottom_layer_num int, pos *([]int16), sign *([]int8)) {

	for l := 0; l <= bottom_layer_num; l++ {
		median_count := int64(0)
		if l == 0 {
			median_count = us.cs_layers[l].UpdateAndEstimateString(key, value, pos, sign)
		} else {
			median_count = us.cs_layers[l].UpdateAndEstimateStringNoL2(key, value, pos, sign)
		}
		us.HH_layers[l].Update(key, median_count)
	}
}

// update multiple layers from top to bottom_layer_num
// only update the first and bottom layer CS in UnivMon
func (us *UnivSketch) update_optimized(key string, value int64, bottom_layer_num int, pos *([]int16), sign *([]int8)) {

	if bottom_layer_num < ELEPHANT_LAYER {
		if bottom_layer_num > 0 {
			median_count := us.cs_layers[bottom_layer_num].UpdateAndEstimateStringNoL2(key, value, pos, sign)

			// use bottom layer heap to update upper layer heaps, but not update its counters to save compute
			for l := bottom_layer_num; l > 0; l-- {
				us.HH_layers[l].Update(key, median_count)
			}
			median_count = us.cs_layers[0].UpdateAndEstimateString(key, value, pos, sign)
			us.HH_layers[0].Update(key, median_count)
		} else {
			median_count := us.cs_layers[0].UpdateAndEstimateString(key, value, pos, sign)
			us.HH_layers[0].Update(key, median_count)
		}
	} else {
		pos_mice, sign_mice := us.cs_layers[bottom_layer_num].position_and_sign([]byte(key))
		median_count := us.cs_layers[bottom_layer_num].UpdateAndEstimateStringNoL2(key, value, &pos_mice, &sign_mice)

		// use bottom layer heap to update upper layer heaps, but not update its counters to save compute
		for l := bottom_layer_num; l > 0; l-- {
			us.HH_layers[l].Update(key, median_count)
		}
		median_count = us.cs_layers[0].UpdateAndEstimateString(key, value, pos, sign)
		us.HH_layers[0].Update(key, median_count)
	}

}

// update multiple layers from top to bottom_layer_num
// only update the first and bottom layer CS in UnivMon
func (us *UnivSketch) update_pyramid(key string, value int64, bottom_layer_num int, pos *([]int16), sign *([]int8)) {

	if bottom_layer_num < ELEPHANT_LAYER {
		// use bottom layer heap to update upper layer heaps, but not update its counters to save compute
		for l := bottom_layer_num; l >= 0; l-- {
			median_count := int64(0)
			if l == 0 {
				median_count = us.cs_layers[l].UpdateAndEstimateString(key, value, pos, sign)
			} else {
				median_count = us.cs_layers[l].UpdateAndEstimateStringNoL2(key, value, pos, sign)
			}
			us.HH_layers[l].Update(key, median_count)
		}

	} else {

		for l := ELEPHANT_LAYER - 1; l >= 0; l-- {
			median_count := int64(0)
			if l == 0 {
				median_count = us.cs_layers[l].UpdateAndEstimateString(key, value, pos, sign)
			} else {
				median_count = us.cs_layers[l].UpdateAndEstimateStringNoL2(key, value, pos, sign)
			}
			us.HH_layers[l].Update(key, median_count)
		}

		pos_mice, sign_mice := us.cs_layers[bottom_layer_num].position_and_sign([]byte(key))
		for l := bottom_layer_num; l >= ELEPHANT_LAYER; l-- {
			median_count := us.cs_layers[l].UpdateAndEstimateStringNoL2(key, value, &pos_mice, &sign_mice)
			us.HH_layers[l].Update(key, median_count)
		}
	}

}

func (us *UnivSketch) univmon_processing_optimized(key string, value int64, bottom_layer_num int, pos *([]int16), sign *([]int8)) {
	us.bucket_size += value
	us.update_optimized(key, value, bottom_layer_num, pos, sign)
}

func (us *UnivSketch) univmon_processing(key string, value int64, bottom_layer_num int, pos *([]int16), sign *([]int8)) {
	us.bucket_size += value
	us.update(key, value, bottom_layer_num, pos, sign)
}

func (us *UnivSketch) PrintHHlayers() {
	for i := 0; i < us.layer; i++ {
		fmt.Println("layer:", i)
		for _, item := range us.HH_layers[i].heap {
			fmt.Println(item.key, item.count)
		}
	}
	fmt.Println()
}

func (us *UnivSketch) QueryTopK(K int) *TopKHeap {
	topk := NewTopKHeap(K)
	for i := (us.layer - 1); i >= 0; i-- {
		var l2_value float64 = us.cs_layers[i].cs_l2()
		var threshold int64 = int64(l2_value * 0.01)
		for _, item := range us.HH_layers[i].heap {
			if item.count > threshold {
				hash := xxhash.ChecksumString64S(item.key, uint64(us.seed))
				hash = ((hash >> (i + 1)) & 1)
				topk.Update(item.key, item.count)
			}
		}
	}
	return topk
}

// Query Universal Sketch
func (us *UnivSketch) calcGSumHeuristic(g func(float64) float64, isCard bool) float64 {
	Y := make([]float64, us.layer)
	var coe float64 = 1
	var tmp float64 = 0

	Y[us.layer-1] = 0

	var l2_value float64 = us.cs_layers[us.layer-1].cs_l2()
	var threshold int64 = int64(l2_value * 0.01)
	if !isCard {
		threshold = 0
	}
	for _, item := range us.HH_layers[us.layer-1].heap {
		if item.count > threshold {
			tmp += g(float64(item.count))
		}
	}
	Y[us.layer-1] = tmp

	for i := (us.layer - 2); i >= 0; i-- {
		tmp = 0
		var l2_value float64 = us.cs_layers[i].cs_l2()
		var threshold int64 = int64(l2_value * 0.01)
		if !isCard {
			threshold = 0
		}
		for _, item := range us.HH_layers[i].heap {
			if item.count > threshold {
				hash := xxhash.ChecksumString64S(item.key, uint64(us.seed))
				hash = ((hash >> (i + 1)) & 1)
				coe = 1 - 2*float64(hash)
				tmp += coe * g(float64(item.count))
			}
		}
		Y[i] = 2*Y[i+1] + tmp
	}

	return Y[0]
}

func (us *UnivSketch) calcGSum(g func(float64) float64, isCard bool) float64 {
	return us.calcGSumHeuristic(g, isCard)
}

func (us *UnivSketch) calcL1() float64 {
	return us.calcGSum(func(x float64) float64 { return x }, false)
}

func (us *UnivSketch) calcL2() float64 {
	tmp := us.calcGSum(func(x float64) float64 { return x * x }, false)
	return math.Sqrt(tmp)
}

func (us *UnivSketch) calcEntropy() float64 {
	tmp := us.calcGSum(func(x float64) float64 {
		if x > 0 {
			return x * math.Log2(x)
		} else {
			return 0
		}
	}, false)
	return math.Log2(float64(us.bucket_size)) - tmp/float64(us.bucket_size)
}

func (us *UnivSketch) calcCard() float64 {
	return us.calcGSum(func(x float64) float64 { return 1 }, true)
}

func (us *UnivSketch) MergeWith(other *UnivSketch) { // Addition
	// fmt.Println("us HH:")
	// us.PrintHHlayers()

	// fmt.Println("other HH:")
	// other.PrintHHlayers()
	for i := 0; i < us.layer; i++ {
		row := len(us.cs_layers[i].count)
		col := len(us.cs_layers[i].count[0])
		for j := 0; j < row; j++ {
			for k := 0; k < col; k++ {
				us.cs_layers[i].count[j][k] = us.cs_layers[i].count[j][k] + other.cs_layers[i].count[j][k]
			}
		}

		topk := NewTopKHeap(us.k)
		// fmt.Println("!!layer:", i)
		for _, item := range us.HH_layers[i].heap {
			topk.Update(item.key, item.count)
			// fmt.Println("!!", item.key, us.cs_layers[i].EstimateStringCount(item.key))
		}

		for _, item := range other.HH_layers[i].heap {
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
		us.HH_layers[i] = NewTopKFromHeap(topk)
	}

	// fmt.Println("merged us HH:")
	// us.PrintHHlayers()
}
