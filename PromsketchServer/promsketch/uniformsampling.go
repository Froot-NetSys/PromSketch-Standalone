package promsketch

import (
	"math"
	"math/rand"
	"sort"
	"sync"
)

type UniformSampling struct {
	Arr              []Sample
	Max_size         int
	Time_window_size int64
	Sampling_rate    float64
	Cur_time         int64
	K                int
	mutex            sync.RWMutex
}

func NewUniformSampling(Time_window_size int64, Sampling_rate float64, Max_size int) *UniformSampling {
	s := UniformSampling{
		Arr:              make([]Sample, 0),
		Max_size:         Max_size,
		Time_window_size: Time_window_size,
		Sampling_rate:    Sampling_rate,
		Cur_time:         0,
		K:                0,
	}
	return &s
}

func (s *UniformSampling) UpdateWindow(window int64) {
	s.mutex.Lock()
	s.Max_size = int(float64(s.Max_size) * float64(s.Time_window_size) / float64(window))
	s.Time_window_size = window
	// fmt.Println("cur window=", s.Time_window_size, s.Max_size)
	s.mutex.Unlock()
}

func (s *UniformSampling) GetMemory() float64 {
	return (float64(len(s.Arr))*16 + 24) / 1024
}

func (s *UniformSampling) Insert(t int64, x float64) {
	s.mutex.Lock()

	s.Cur_time = t
	shift := 0
	// fmt.Println(s.Arr)
	for i := range s.Arr {

		if s.Arr[i].T < s.Cur_time-s.Time_window_size {
			shift = i
		} else {
			shift = i
			break
		}
	}
	if len(s.Arr) > s.Max_size {
		shift = len(s.Arr) - s.Max_size
	}

	s.Arr = s.Arr[shift:]
	r := rand.Float64()
	if r <= s.Sampling_rate {
		s.Arr = append(s.Arr, Sample{T: t, F: x})
	}
	s.mutex.Unlock()
}

/*
func (s *UniformSampling) Insert(t int64, x float64) {
	// Reservoir sampling
	s.Cur_time = t
	shift := 0
	fmt.Println(s.Arr)
	for i := range s.Arr {
		if s.Arr[i].T < s.Cur_time-s.Time_window_size {
			shift = i
		} else {
			shift = i
			break
		}
	}
	s.Arr = s.Arr[shift:]
	fmt.Println(shift)
	fmt.Println(s.Arr)
	s.K -= shift
	if s.K < s.Max_size {
		s.Arr = append(s.Arr, Sample{T: t, F: x})
	} else {
		r := rand.Float64()
		if r < float64(s.Max_size)/float64(s.K+1) {
			if len(s.Arr) < s.Max_size {
				s.Arr = append(s.Arr, Sample{T: t, F: x})
			} else {
				r_idx := rand.Intn(len(s.Arr))
				s.Arr = append(s.Arr[:r_idx], s.Arr[r_idx+1:]...)
				s.Arr = append(s.Arr, Sample{T: t, F: x})
			}
		}
	}
	s.K += 1
}
*/

// quantile calculates the given quantile of a vector of samples.
//
// The Vector will be sorted.
// If 'values' has zero elements, NaN is returned.
// If q==NaN, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
func quantile(q float64, values []float64) float64 {
	if len(values) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	sort.Float64s(values)

	n := float64(len(values))
	// When the quantile lies between two samples,
	// we use a weighted average of the two samples.
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)
	return values[int(lowerIndex)]*(1-weight) + values[int(upperIndex)]*weight
}

// quantile calculates the given quantile of a vector of samples.
//
// The Vector will be sorted.
// If 'values' has zero elements, NaN is returned.
// If q==NaN, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
func quantiles(qs []float64, values []float64) (quantiles []float64) {
	sort.Float64s(values)
	n := float64(len(values))
	if n == 0 {
		for range qs {
			quantiles = append(quantiles, math.NaN())
		}
		return quantiles
	}
	for _, q := range qs {
		if q < 0 {
			quantiles = append(quantiles, float64(math.Inf(-1)))
		} else if q > 1 {
			quantiles = append(quantiles, float64(math.Inf(+1)))
		} else {
			// When the quantile lies between two samples,
			// we use a weighted average of the two samples.
			rank := q * (n - 1)
			lowerIndex := math.Max(0, math.Floor(rank))
			upperIndex := math.Min(n-1, lowerIndex+1)
			weight := rank - math.Floor(rank)
			quantiles = append(quantiles, values[int(lowerIndex)]*(1-weight)+values[int(upperIndex)]*weight)
		}
	}
	return quantiles
}

func (s *UniformSampling) QueryQuantile(phis []float64, t1, t2 int64) []float64 {
	values := make([]float64, 0)
	s.mutex.RLock()
	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return []float64{math.NaN()}
	}

	for i := idx_1; i <= idx_2; i++ {
		values = append(values, s.Arr[i].F)
	}
	s.mutex.RUnlock()
	return quantiles(phis, values)
}

func (s *UniformSampling) Cover(t1, t2 int64) bool {
	s.mutex.RLock()
	if len(s.Arr) == 0 {
		s.mutex.RUnlock()
		return false
	}
	isCovered := s.Cur_time >= t2 && s.Cur_time-s.Time_window_size <= t1

	s.mutex.RUnlock()
	return isCovered
}

func (s *UniformSampling) GetMinTime() int64 {
	return s.Cur_time - s.Time_window_size
}

func (s *UniformSampling) GetMaxTime() int64 {
	return s.Cur_time
}

func (s *UniformSampling) GetSamples(t1, t2 int64) []float64 {
	s.mutex.RLock()
	values := make([]float64, 0)
	for _, v := range s.Arr {
		if v.T < t1 {
			continue
		}
		if v.T > t2 {
			break
		}
		values = append(values, v.F)
	}
	s.mutex.RUnlock()
	return values
}

func (s *UniformSampling) QueryAvg(t1, t2 int64) float64 {
	s.mutex.RLock()
	var sum float64 = 0
	var n float64 = 0

	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	for i := idx_1; i <= idx_2; i++ {
		sum += s.Arr[i].F
		n += 1
	}
	s.mutex.RUnlock()
	return sum / n
}

func (s *UniformSampling) QuerySum(t1, t2 int64) float64 {
	s.mutex.RLock()
	var sum float64 = 0

	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	for i := idx_1; i <= idx_2; i++ {
		sum += s.Arr[i].F
	}
	s.mutex.RUnlock()
	return sum / float64(s.Sampling_rate)
}

func (s *UniformSampling) QueryStddev(t1, t2 int64) float64 {
	s.mutex.RLock()
	var sum float64 = 0

	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	var sum2 float64 = 0
	var count float64 = 0
	for i := idx_1; i <= idx_2; i++ {
		sum += s.Arr[i].F
		sum2 += s.Arr[i].F * s.Arr[i].F
		count += 1
	}
	stddev := math.Sqrt(sum2/count - math.Pow(sum/count, 2))
	s.mutex.RUnlock()
	return stddev
}

func (s *UniformSampling) QueryStdvar(t1, t2 int64) float64 {
	s.mutex.RLock()
	var sum float64 = 0

	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	var sum2 float64 = 0
	var count float64 = 0
	for i := idx_1; i <= idx_2; i++ {
		sum += s.Arr[i].F
		sum2 += s.Arr[i].F * s.Arr[i].F
		count += 1
	}
	stdvar := sum2/count - math.Pow(sum/count, 2)
	s.mutex.RUnlock()
	return stdvar
}

func (s *UniformSampling) QueryMax(t1, t2 int64) float64 {
	s.mutex.RLock()
	var max float64 = 0

	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}
	for i := idx_1; i <= idx_2; i++ {
		if s.Arr[i].F > max {
			max = s.Arr[i].F
		}
	}
	s.mutex.RUnlock()
	return max
}

func (s *UniformSampling) QueryMin(t1, t2 int64) float64 {
	s.mutex.RLock()

	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	var min float64 = s.Arr[idx_1].F

	for i := idx_1; i <= idx_2; i++ {
		if s.Arr[i].F < min {
			min = s.Arr[i].F
		}
	}
	s.mutex.RUnlock()
	return min
}

func (s *UniformSampling) QuerySum2(t1, t2 int64) float64 {
	s.mutex.RLock()
	var sum2 float64 = 0
	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	for i := idx_1; i <= idx_2; i++ {
		sum2 += s.Arr[i].F * s.Arr[i].F
	}
	s.mutex.RUnlock()
	return sum2 / float64(s.Sampling_rate)
}

func (s *UniformSampling) QueryCount(t1, t2 int64) float64 {
	s.mutex.RLock()
	var count float64 = 0
	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}
	count = float64(idx_2 - idx_1 + 1)

	s.mutex.RUnlock()
	return float64(count) / float64(s.Sampling_rate)
}

func (s *UniformSampling) QueryL1(t1, t2 int64) float64 {
	s.mutex.RLock()
	m := make(map[float64]int)
	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	for i := idx_1; i <= idx_2; i++ {
		if _, ok := m[s.Arr[i].F]; !ok {
			m[s.Arr[i].F] = 1
		} else {
			m[s.Arr[i].F] += 1
		}
	}
	var l1 float64 = 0
	for _, v := range m {
		l1 += float64(v)
	}
	s.mutex.RUnlock()
	return l1 / float64(s.Sampling_rate)
}

func (s *UniformSampling) QueryGSum(t1, t2 int64) (float64, float64, float64, float64) {
	s.mutex.RLock()
	m := make(map[float64]int)
	n := float64(0)
	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN(), math.NaN(), math.NaN(), math.NaN()
	}

	for i := idx_1; i <= idx_2; i++ {
		n += 1
		if _, ok := m[s.Arr[i].F]; !ok {
			m[s.Arr[i].F] = 1
		} else {
			m[s.Arr[i].F] += 1
		}
	}
	var l1, l2, entropy float64 = 0, 0, 0
	for _, v := range m {
		l1 += float64(v)
		l2 += float64(v * v)
		entropy += float64(v) * math.Log2(float64(v))
	}
	distinct := float64(len(m))
	s.mutex.RUnlock()
	return distinct / float64(s.Sampling_rate), l1 / float64(s.Sampling_rate), math.Log2(n/float64(s.Sampling_rate)) - entropy/n, math.Sqrt(l2 / float64(s.Sampling_rate))
}

func (s *UniformSampling) QueryL2(t1, t2 int64) float64 {
	s.mutex.RLock()
	m := make(map[float64]int)
	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	for i := idx_1; i <= idx_2; i++ {
		if _, ok := m[s.Arr[i].F]; !ok {
			m[s.Arr[i].F] = 1
		} else {
			m[s.Arr[i].F] += 1
		}
	}
	var l2 float64 = 0
	for _, v := range m {
		l2 += float64(v * v)
	}
	s.mutex.RUnlock()
	return math.Sqrt(l2 / float64(s.Sampling_rate))
}

func (s *UniformSampling) QueryEntropy(t1, t2 int64) float64 {
	s.mutex.RLock()
	m := make(map[float64]int)
	n := float64(0)
	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	for i := idx_1; i <= idx_2; i++ {
		n += 1
		if _, ok := m[s.Arr[i].F]; !ok {
			m[s.Arr[i].F] = 1
		} else {
			m[s.Arr[i].F] += 1
		}
	}
	var entropy float64 = 0
	for _, v := range m {
		entropy += float64(v) * math.Log2(float64(v))
	}
	s.mutex.RUnlock()
	return math.Log2(n/float64(s.Sampling_rate)) - entropy/n
}

func (s *UniformSampling) QueryDistinct(t1, t2 int64) float64 {
	s.mutex.RLock()
	m := make(map[float64]int)
	idx_1 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T >= t1 })
	idx_2 := sort.Search(len(s.Arr), func(i int) bool { return s.Arr[i].T > t2 }) - 1

	if idx_1 >= len(s.Arr) {
		s.mutex.RUnlock()
		return math.NaN()
	}

	for i := idx_1; i <= idx_2; i++ {
		m[s.Arr[i].F] = 1
	}
	s.mutex.RUnlock()
	return float64(len(m))
}
