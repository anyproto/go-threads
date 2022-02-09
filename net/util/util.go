package util

import (
	"sync"
	"time"

	"github.com/textileio/go-threads/metrics"
	apipb "github.com/textileio/go-threads/net/api/pb"
	netpb "github.com/textileio/go-threads/net/pb"
)

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

func NewSemaphore(capacity int, m metrics.Metrics) *Semaphore {
	return &Semaphore{inner: make(chan struct{}, capacity), m: m}
}

type Semaphore struct {
	inner    chan struct{}
	m        metrics.Metrics
	acquired time.Time
}

// Blocking acquire
func (s *Semaphore) Acquire() {
	s.m.SemaphoreAcquire()
	started := time.Now()

	s.inner <- struct{}{}

	s.acquired = time.Now()
	s.m.SemaphoreAcquireDuration(time.Since(started))
}

// Non-blocking acquire
func (s *Semaphore) TryAcquire() bool {
	s.m.SemaphoreAcquire()

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
		s.m.SemaphoreHoldDuration(time.Since(acquired))
	default:
		panic("thread semaphore inconsistency: release before acquire!")
	}
}

type SemaphoreKey interface {
	Key() string
}

func NewSemaphorePool(semaCap int, m metrics.Metrics) *SemaphorePool {
	return &SemaphorePool{ss: make(map[string]*Semaphore), semaCap: semaCap, m: m}
}

type SemaphorePool struct {
	ss      map[string]*Semaphore
	semaCap int
	mu      sync.Mutex
	m       metrics.Metrics
}

func (p *SemaphorePool) Get(k SemaphoreKey) *Semaphore {
	var (
		s     *Semaphore
		exist bool
		key   = k.Key()
	)

	p.mu.Lock()
	if s, exist = p.ss[key]; !exist {
		s = NewSemaphore(p.semaCap, p.m)
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
