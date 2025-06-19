package promsketch

import (
	"math"
)

const (
	LargeError = iota
	SmallError
)

type SumArr struct {
	bit      int8
	max_time int64
}

type EfficientSum struct {
	item_window_size int64 // this indicates the number of items in the sliding window
	time_window_size int64 // this indicates the time window size
	epsilon          float64
	v                float64
	T                float64
	b                []SumArr
	R                float64
	y                float64
	B                int64
	i                int64
	m                int64
	algo             int8
	max_time         int64
}

func NewEfficientSum(item_window_size int64, time_window_size int64, epsilon float64, R float64) *EfficientSum {
	s := EfficientSum{
		item_window_size: item_window_size,
		time_window_size: time_window_size,
		epsilon:          epsilon,
		R:                R,
		y:                0,
		B:                0,
		i:                0,
		m:                0,
		max_time:         0,
	}
	if epsilon*float64(item_window_size)*(2-1/math.Log2(float64(item_window_size))) >= 1 {
		s.v = math.Ceil(math.Log2(1 / epsilon * math.Log2(float64(item_window_size))))
		s.T = float64(item_window_size) * (2*epsilon - math.Pow(2, -s.v))
		s.algo = LargeError
		s.b = make([]SumArr, int(math.Ceil(float64(item_window_size)/s.T)))
		// fmt.Println(epsilon, time_window_size, "use large error algorithm")
	} else if epsilon*2*float64(item_window_size)*(1-1/math.Log2(float64(item_window_size))) < 1 {
		s.algo = SmallError
		s.b = make([]SumArr, item_window_size)
		s.v = math.Ceil(math.Log2(1 / epsilon * math.Log2(float64(item_window_size))))
		s.T = float64(item_window_size) * (2*epsilon - math.Pow(2, -s.v))
		// fmt.Println(epsilon, time_window_size, "use small error algorithm")
	} else {
		s.algo = SmallError
		s.b = make([]SumArr, item_window_size)
		s.v = math.Ceil(math.Log2(1 / epsilon * math.Log2(float64(item_window_size))))
		s.T = float64(item_window_size) * (2*epsilon - math.Pow(2, -s.v))
		// fmt.Println(epsilon, time_window_size, "use small error algorithm")
	}

	for i := range s.b {
		s.b[i].bit = 0
		s.b[i].max_time = 0
	}
	return &s
}

func (s *EfficientSum) GetMemory() float64 {
	return (float64(len(s.b)*9) + 81) / 1024 // KB
}

func (s *EfficientSum) Cover(mint, maxt int64) bool {
	// fmt.Println("mint, maxt, max_time, windowsize", mint, maxt, s.max_time, s.time_window_size)
	return mint >= s.max_time-s.time_window_size // && maxt <= s.max_time
}

func (s *EfficientSum) InsertLargeError(t int64, x float64) {
	x1 := x / s.R
	s.m = (s.m + 1) % int64(s.T)
	s.y = s.y + x1
	s.b[s.i].max_time = t
	if s.m == 0 {
		s.B = s.B - int64(s.b[s.i].bit)
		s.b[s.i].bit = int8(math.Floor((s.y) / s.T))
		s.y = s.y - s.T*float64(s.b[s.i].bit)
		s.B += int64(s.b[s.i].bit)
		s.i = (s.i + 1) % int64(len(s.b))
	}
}

func (s *EfficientSum) QueryLargeError(t1, t2 int64, subinterval bool) float64 {
	if subinterval == false {
		return s.R * (float64(s.T)*float64(s.B) - s.T/2 - float64(s.m)*float64(s.b[s.i].bit) + math.Pow(2, -s.v-1))
	}
	sum_B := int64(0)
	idx := (s.i + 1) % int64(len(s.b))
	for step := 0; step < len(s.b); step++ {
		if s.b[idx].max_time >= t1 && s.b[idx].max_time < t2 {
			sum_B += int64(s.b[idx].bit)
		}
		idx = (idx + 1) % int64(len(s.b))
	}
	// sum_B += int64(s.b[idx].bit)
	return s.R * (float64(s.T)*float64(sum_B) - s.T/2 + math.Pow(2, -s.v-1))
}

func (s *EfficientSum) InsertSmallError(t int64, x float64) {
	x1 := x / s.R
	s.B = s.B - int64(s.b[s.i].bit)
	s.b[s.i].bit = int8(math.Floor((s.y + x1) / s.T))
	s.b[s.i].max_time = t
	s.y = s.y + x1 - float64(s.b[s.i].bit)*s.T
	s.B += int64(s.b[s.i].bit)
	s.i = (s.i + 1) % s.item_window_size
}

func (s *EfficientSum) QuerySmallError(t1, t2 int64, subinterval bool) float64 {
	if subinterval == false {
		return s.R * (float64(s.T)*float64(s.B) + s.y - s.T/2 + math.Pow(2, -s.v-1))
	}
	sum_B := int64(0)
	idx := (s.i + 1) % int64(len(s.b))
	for step := 0; step < len(s.b); step++ {
		if s.b[idx].max_time >= t1 && s.b[idx].max_time < t2 {
			sum_B += int64(s.b[idx].bit)
		}
		idx = (idx + 1) % int64(len(s.b))
	}
	// sum_B += int64(s.b[idx].bit)
	return s.R * (float64(s.T)*float64(sum_B) - s.T/2 + math.Pow(2, -s.v-1))
}

func (s *EfficientSum) Insert(t int64, x float64) {
	s.max_time = t
	if s.algo == SmallError {
		s.InsertSmallError(t, x)
	} else {
		s.InsertLargeError(t, x)
	}
}

func (s *EfficientSum) Query(t1, t2 int64, subinterval bool) float64 {
	if s.algo == SmallError {
		return s.QuerySmallError(t1, t2, subinterval)
	} else {
		return s.QueryLargeError(t1, t2, subinterval)
	}
}
