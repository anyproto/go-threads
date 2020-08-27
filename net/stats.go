package net

import (
	"math"
	"sort"
	"sync"
)

type slidingWindow struct {
	buf  []float64
	size int
	idx  int
	sync.Mutex
}

func NewSlidingWindow(size int) *slidingWindow {
	return &slidingWindow{size: size, buf: make([]float64, size)}
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
