package net

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/textileio/go-threads/core/thread"
)

func NewStatsTracker(bufferSize int, quantiles []float64, samplingPeriod time.Duration) *statsTracker {
	st := &statsTracker{
		quantiles:          quantiles,
		timeToGetSemaphore: NewSlidingWindow(bufferSize),
		acquireDuration:    NewSlidingWindow(bufferSize),
		releaseDuration:    NewSlidingWindow(bufferSize),
	}

	go st.runSampling(samplingPeriod)
	return st
}

type statsTracker struct {
	quantiles                                            []float64
	numSemaphores, acquireAttempts                       int32
	timeToGetSemaphore, acquireDuration, releaseDuration *slidingWindow
}

// Track new semaphore was created
func (s *statsTracker) IncSemaphores() {
	atomic.AddInt32(&s.numSemaphores, 1)
}

// Track time to get thread semaphore
func (s *statsTracker) TrackGetSemaphore(d time.Duration) {
	s.timeToGetSemaphore.Push(d.Seconds())
}

// Track attempt to acquire semaphore
func (s *statsTracker) TrackAcquireAttempt() {
	atomic.AddInt32(&s.acquireAttempts, 1)
}

// Track time to acquire semaphore
func (s *statsTracker) TrackAcquire(d time.Duration) {
	s.acquireDuration.Push(d.Seconds())
}

// Track time to release previously acquired semaphore
func (s *statsTracker) TrackRelease(d time.Duration) {
	s.releaseDuration.Push(d.Seconds())
}

func (s *statsTracker) runSampling(period time.Duration) {
	t := time.NewTicker(period)

	for range t.C {
		var res strings.Builder

		res.WriteString(fmt.Sprintf("active threads: %d, ", atomic.LoadInt32(&s.numSemaphores)))
		attemptsForPeriod := atomic.SwapInt32(&s.acquireAttempts, 0)
		res.WriteString(fmt.Sprintf("acquire rate: %f /sec (%d for %v), ",
			float64(attemptsForPeriod)/period.Seconds(), attemptsForPeriod, period))

		s.writeQuantiles(&res, "semaphore acquire durations", s.acquireDuration.Quantiles(s.quantiles))
		res.WriteString(", ")
		s.writeQuantiles(&res, "semaphore hold durations", s.releaseDuration.Quantiles(s.quantiles))
		res.WriteString(", ")
		s.writeQuantiles(&res, "time to get semaphore", s.timeToGetSemaphore.Quantiles(s.quantiles))

		// output in logs
		log.Warnf("thread semaphore statistics: %s", res.String())
	}
}

func (s *statsTracker) writeQuantiles(sb *strings.Builder, name string, results []float64) {
	sb.WriteString(name)
	sb.WriteString(": ")
	if len(results) == 0 {
		sb.WriteByte('-')
		return
	}

	sb.WriteByte('[')
	for i, sec := range results {
		sb.WriteString(fmt.Sprintf("q%d: %v", int(s.quantiles[i]*100), time.Duration(sec*float64(time.Second))))
		sb.WriteString(" / ")
	}
	sb.WriteByte(']')
}

/* Semaphore with stats tracking */

func newSemaphore(size int, stats *statsTracker) *semaphore {
	return &semaphore{inner: make(chan struct{}, size), stats: stats}
}

type semaphore struct {
	inner      chan struct{}
	acquiredAt time.Time
	stats      *statsTracker
}

func (s *semaphore) Acquire() {
	s.stats.TrackAcquireAttempt()
	started := time.Now()
	s.inner <- struct{}{}
	acquired := time.Now()
	s.acquiredAt = acquired
	s.stats.TrackAcquire(acquired.Sub(started))
}

func (s *semaphore) Release() {
	<-s.inner
	s.stats.TrackRelease(time.Since(s.acquiredAt))
}

/* Pool of per-thread semaphores with stats tracking */

func NewSemaphorePool(stats *statsTracker) *semaphorePool {
	return &semaphorePool{stats: stats, ss: make(map[thread.ID]*semaphore)}
}

type semaphorePool struct {
	ss    map[thread.ID]*semaphore
	stats *statsTracker
	sync.Mutex
}

func (p *semaphorePool) getThreadSemaphore(id thread.ID) *semaphore {
	var (
		s       *semaphore
		exist   bool
		started = time.Now()
	)

	p.Lock()
	if s, exist = p.ss[id]; !exist {
		s = newSemaphore(1, p.stats)
		p.ss[id] = s
		p.stats.IncSemaphores()
	}
	p.Unlock()

	p.stats.TrackGetSemaphore(time.Now().Sub(started))

	return s
}
