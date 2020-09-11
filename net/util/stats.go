package util

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

func NewStatsTracker(bufferSize int, quantiles []float64, samplingPeriod time.Duration, log func(format string, args ...interface{})) *StatsTracker {
	st := &StatsTracker{
		quantiles:          quantiles,
		timeToGetSemaphore: NewSlidingWindow(bufferSize),
		acquireDuration:    NewSlidingWindow(bufferSize),
		releaseDuration:    NewSlidingWindow(bufferSize),
		log:                log,
	}

	go st.runSampling(samplingPeriod)
	return st
}

type StatsTracker struct {
	quantiles                                            []float64
	numSemaphores, acquireAttempts                       int32
	timeToGetSemaphore, acquireDuration, releaseDuration *slidingWindow
	log                                                  func(format string, args ...interface{})
}

// Track new semaphore was created
func (s *StatsTracker) IncSemaphores() {
	atomic.AddInt32(&s.numSemaphores, 1)
}

// Track time to get thread semaphore
func (s *StatsTracker) TrackGetSemaphore(d time.Duration) {
	s.timeToGetSemaphore.Push(d.Seconds())
}

// Track attempt to acquire semaphore
func (s *StatsTracker) TrackAcquireAttempt() {
	atomic.AddInt32(&s.acquireAttempts, 1)
}

// Track time to acquire semaphore
func (s *StatsTracker) TrackAcquire(d time.Duration) {
	s.acquireDuration.Push(d.Seconds())
}

// Track time to release previously acquired semaphore
func (s *StatsTracker) TrackRelease(d time.Duration) {
	s.releaseDuration.Push(d.Seconds())
}

func (s *StatsTracker) runSampling(period time.Duration) {
	t := time.NewTicker(period)

	for range t.C {
		var res strings.Builder

		res.WriteString(fmt.Sprintf("active threads: %d, ", atomic.LoadInt32(&s.numSemaphores)))
		attemptsForPeriod := atomic.SwapInt32(&s.acquireAttempts, 0)
		res.WriteString(fmt.Sprintf("acquire rate: %f /sec (%d for %v), ",
			float64(attemptsForPeriod)/period.Seconds(), attemptsForPeriod, period))

		s.writeStats(&res, "semaphore acquire durations", s.acquireDuration)
		res.WriteString(", ")
		s.writeStats(&res, "semaphore hold durations", s.releaseDuration)
		res.WriteString(", ")
		s.writeStats(&res, "time to get semaphore", s.timeToGetSemaphore)

		// output in logs
		s.log("thread semaphore statistics: %s", res.String())
	}
}

func (s *StatsTracker) writeStats(sb *strings.Builder, name string, w *slidingWindow) {
	sb.WriteString(name)
	sb.WriteString(": [")

	max, min := w.Extrema()
	sb.WriteString(fmt.Sprintf("max: %v, min: %v",
		time.Duration(max*float64(time.Second)),
		time.Duration(min*float64(time.Second))))

	for i, sec := range w.Quantiles(s.quantiles) {
		sb.WriteString(fmt.Sprintf("q%d: %v", int(s.quantiles[i]*100), time.Duration(sec*float64(time.Second))))
		sb.WriteString(" / ")
	}
	sb.WriteByte(']')
}
