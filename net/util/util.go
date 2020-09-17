package util

import (
	"sync"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	apipb "github.com/textileio/go-threads/net/api/pb"
	netpb "github.com/textileio/go-threads/net/pb"
)

var (
	semaphoreAcquireCounter = prom.NewCounter(prom.CounterOpts{
		Namespace: "threads",
		Subsystem: "net",
		Name:      "semaphore_acquire_total",
		Help:      "Number of semaphore acquire attempts",
	})

	semaphoreAcquireDuration = prom.NewHistogram(prom.HistogramOpts{
		Namespace: "threads",
		Subsystem: "net",
		Name:      "semaphore_acquire_duration_seconds",
		Help:      "Time to acquire semaphore",
		Buckets:   MetricTimeBuckets(semaDurationScale),
	})

	semaphoreHoldDuration = prom.NewHistogram(prom.HistogramOpts{
		Namespace: "threads",
		Subsystem: "net",
		Name:      "semaphore_hold_duration_seconds",
		Help:      "Time while semaphore is held by the process",
		Buckets:   MetricTimeBuckets(semaDurationScale),
	})

	semaDurationScale = []time.Duration{
		2 * time.Microsecond,
		8 * time.Microsecond,
		32 * time.Microsecond,
		128 * time.Microsecond,
		512 * time.Microsecond,
		2 * time.Millisecond,
		8 * time.Millisecond,
		32 * time.Millisecond,
		128 * time.Millisecond,
		512 * time.Millisecond,
		2 * time.Second,
		8 * time.Second,
		30 * time.Second,
		2 * time.Minute,
		4 * time.Minute,
		6 * time.Minute,
		8 * time.Minute,
	}
)

func MetricTimeBuckets(scale []time.Duration) []float64 {
	buckets := make([]float64, len(scale))
	for i, b := range scale {
		buckets[i] = b.Seconds()
	}
	return buckets
}

func MetricObserveSeconds(hist prom.Histogram, started time.Time) {
	hist.Observe(float64(time.Since(started)) / float64(time.Second))
}

func RecFromServiceRec(r *netpb.Log_Record) *apipb.Record {
	return &apipb.Record{
		RecordNode: r.RecordNode,
		EventNode:  r.EventNode,
		HeaderNode: r.HeaderNode,
		BodyNode:   r.BodyNode,
	}
}

func RecToServiceRec(r *apipb.Record) *netpb.Log_Record {
	return &netpb.Log_Record{
		RecordNode: r.RecordNode,
		EventNode:  r.EventNode,
		HeaderNode: r.HeaderNode,
		BodyNode:   r.BodyNode,
	}
}

func NewSemaphore(capacity int) *Semaphore {
	return &Semaphore{inner: make(chan struct{}, capacity)}
}

type Semaphore struct {
	inner    chan struct{}
	acquired time.Time
}

// Blocking acquire
func (s *Semaphore) Acquire() {
	semaphoreAcquireCounter.Inc()
	started := time.Now()

	s.inner <- struct{}{}

	s.acquired = time.Now()
	MetricObserveSeconds(semaphoreAcquireDuration, started)
}

// Non-blocking acquire
func (s *Semaphore) TryAcquire() bool {
	semaphoreAcquireCounter.Inc()

	select {
	case s.inner <- struct{}{}:
		s.acquired = time.Now()
		return true
	default:
		return false
	}
}

func (s *Semaphore) Release() {
	acquired := s.acquired

	select {
	case <-s.inner:
		MetricObserveSeconds(semaphoreHoldDuration, acquired)
	default:
		panic("thread semaphore inconsistency: release before acquire!")
	}
}

type SemaphoreKey interface {
	Key() string
}

func NewSemaphorePool(semaCap int) *SemaphorePool {
	return &SemaphorePool{ss: make(map[string]*Semaphore), semaCap: semaCap}
}

type SemaphorePool struct {
	ss      map[string]*Semaphore
	semaCap int
	mu      sync.Mutex
}

func (p *SemaphorePool) Get(k SemaphoreKey) *Semaphore {
	var (
		s     *Semaphore
		exist bool
		key   = k.Key()
	)

	p.mu.Lock()
	if s, exist = p.ss[key]; !exist {
		s = NewSemaphore(p.semaCap)
		p.ss[key] = s
	}
	p.mu.Unlock()

	return s
}

func (p *SemaphorePool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// grab all semaphores and hold
	for _, s := range p.ss {
		s.Acquire()
	}
}

func init() {
	prom.MustRegister(semaphoreAcquireCounter)
	prom.MustRegister(semaphoreAcquireDuration)
	prom.MustRegister(semaphoreHoldDuration)
}
