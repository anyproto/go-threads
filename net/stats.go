package net

import (
	"math"
	"sort"
	"sync"
)

type slidingWindow struct {
	buf      []float64
	max, min float64
	size     int
	idx      int
	sync.Mutex
}

func NewSlidingWindow(size int) *slidingWindow {
	return &slidingWindow{
		buf:  make([]float64, size),
		max:  math.Inf(-1),
		min:  math.Inf(1),
		size: size,
	}
}

func (w *slidingWindow) Push(x float64) {
	w.Lock()

	// store
	w.buf[w.idx] = x

	// move index
	w.idx++
	if w.idx == w.size {
		w.idx = 0
	}

	// update max/min
	if x > w.max {
		w.max = x
	}
	if x < w.min {
		w.min = x
	}

	w.Unlock()
}

func (w *slidingWindow) Quantiles(qs []float64) []float64 {
	if len(qs) == 0 {
		return nil
	}

	w.Lock()
	data := make([]float64, len(w.buf))
	copy(data, w.buf)
	w.Unlock()

	sort.Float64s(data)

	// remove all leading zeros (window wasn't filled yet)
	var startIdx int
	for i := 0; i < len(data); i++ {
		if data[i] > 0 {
			break
		}
		startIdx++
	}
	data = data[startIdx:]
	if len(data) == 0 {
		return nil
	}

	results := make([]float64, len(qs))
	for i := 0; i < len(qs); i++ {
		idx := int(math.Min(math.Round(qs[i]*float64(len(data))), float64(len(data)-1)))
		results[i] = data[idx]
	}

	return results
}

func (w *slidingWindow) Extrema() (max, min float64) {
	w.Lock()
	max, min = w.max, w.min
	w.Unlock()
	return
}
