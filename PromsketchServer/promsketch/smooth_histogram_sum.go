package promsketch

import (
	"errors"
	"fmt"
	"math"
)

type CountBucket struct {
	count      int64
	sum        float64
	sum2       float64
	bucketsize int
	min_time   int64
	max_time   int64
}

type SmoothHistogramCount struct {
	s_count          int
	buckets          []*CountBucket
	beta             float64
	time_window_size int64
}

/*----------------------------------------------------------
				Smooth Histogram for count, sum, and avg, sum2
----------------------------------------------------------*/

func SmoothInitCount(beta float64, time_window_size int64) (sh *SmoothHistogramCount) {
	sh = &SmoothHistogramCount{
		beta:             beta,
		s_count:          0,
		time_window_size: time_window_size,
	}

	sh.buckets = make([]*CountBucket, sh.s_count)

	return sh
}

func (sh *SmoothHistogramCount) GetMemory() float64 {
	return 40 * float64(sh.s_count) / 1024 // KBytes
}

func (sh *SmoothHistogramCount) Update(time int64, value float64) {
	for i := 0; i < sh.s_count; i++ {
		sh.buckets[i].count += 1   // count
		sh.buckets[i].sum += value // sum
		// sh.buckets[i].sum2 += value * value // sum2
		sh.buckets[i].max_time = time
		sh.buckets[i].bucketsize += 1
	}

	tmp := &CountBucket{
		count: 1,
		sum:   value,
		// sum2:       value * value,
		max_time:   time,
		min_time:   time,
		bucketsize: 1,
	}
	sh.buckets = append(sh.buckets, tmp)
	sh.s_count++

	for i := 0; i < sh.s_count-2; i++ {
		maxj := i + 1
		var compare_value float64 = float64(1.0-sh.beta) * (sh.buckets[i].sum)

		for j := i + 1; j < sh.s_count-2; j++ {
			if (maxj < j) && (float64(sh.buckets[j].sum) >= compare_value) {
				maxj = j
			} else {
				break
			}
		}

		shift := maxj - i - 1
		if shift > 0 {
			sh.s_count = sh.s_count - shift
			sh.buckets = append(sh.buckets[:i+1], sh.buckets[maxj:]...)
		}
	}

	removed := 0
	for i := 0; i < sh.s_count; i++ {
		if sh.buckets[i].max_time-sh.buckets[i].min_time >= sh.time_window_size {
			removed++
		} else {
			break
		}
	}
	if removed > 0 {
		sh.s_count -= removed
		sh.buckets = sh.buckets[removed:]
	}

	// sh.print_sh_count()
}

func (sh *SmoothHistogramCount) Cover(mint, maxt int64) bool {
	if sh.s_count == 0 {
		return false
	}
	return (sh.buckets[0].min_time <= mint) && sh.buckets[sh.s_count-1].max_time >= maxt
}

func (sh *SmoothHistogramCount) print_sh_count() {
	fmt.Println("s_count =", sh.s_count)
	for i := 0; i < sh.s_count; i++ {
		fmt.Println("i =", i, "min_time =", sh.buckets[i].min_time, "max_time = ", sh.buckets[i].max_time)
	}
}

func (sh *SmoothHistogramCount) QueryIntervalCount(t1 int64, t int64) (*CountBucket, error) {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - (t - sh.buckets[i].min_time))
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}
	if sh.s_count > 0 {
		return sh.buckets[return_bucket], nil
	} else {
		return nil, errors.New("bucket not found")
	}
}

func (sh *SmoothHistogramCount) QueryT1T2IntervalCount(t1, t2, t int64) float64 {
	bucket_1, err1 := sh.QueryIntervalCount(t1, t)
	bucket_2, err2 := sh.QueryIntervalCount(t2, t)
	if err1 == nil && err2 == nil {
		return float64(bucket_2.count - bucket_1.count)
	} else {
		return 0
	}
}

func (sh *SmoothHistogramCount) QueryIntervalSum(t1, t int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0

	sh.print_sh_count()

	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - (t - sh.buckets[i].min_time))
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	fmt.Println(return_bucket)

	return sh.buckets[return_bucket].sum
}

func (sh *SmoothHistogramCount) QueryT1T2IntervalSum(t1, t2, t int64) float64 {
	return sh.QueryIntervalSum(t1, t) - sh.QueryIntervalSum(t2, t)
}

func (sh *SmoothHistogramCount) QueryT1T2IntervalAvg(t1, t2, t int64) float64 {
	return sh.QueryT1T2IntervalSum(t1, t2, t) / sh.QueryT1T2IntervalCount(t1, t2, t)
}

func (sh *SmoothHistogramCount) QueryIntervalSum2(t1, t int64) float64 {
	var diff int64 = math.MaxInt64
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt64(t1 - (t - sh.buckets[i].min_time))
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	return sh.buckets[return_bucket].sum2
}

func (sh *SmoothHistogramCount) QueryT1T2IntervalSum2(t1, t2, t int64) float64 {
	return sh.QueryIntervalSum2(t1, t) - sh.QueryIntervalSum2(t2, t)
}
