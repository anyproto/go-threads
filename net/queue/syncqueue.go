package queue

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/textileio/go-threads/core/thread"
)

type peerEntry struct {
	queue    []PeerCall
	notifier chan struct{}
	sync.Mutex
}

type SyncQueue struct {
	entryMap map[peer.ID]*peerEntry
	ctx      context.Context
	sync.RWMutex
}

func NewSyncQueue(ctx context.Context) *SyncQueue {
	return &SyncQueue{
		entryMap: make(map[peer.ID]*peerEntry),
		ctx:      ctx,
		RWMutex:  sync.RWMutex{},
	}
}

func (s *SyncQueue) pollQueue(pid peer.ID, tid thread.ID, entry *peerEntry) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-entry.notifier:
			entry.Lock()
			if len(entry.queue) == 0 {
				entry.Unlock()
				continue
			}
			call := entry.queue[0]
			entry.queue = entry.queue[1:]
			entry.Unlock()
			err := call(s.ctx, pid, tid)
			if err != nil {
				log.With("thread", tid.String()).With("peer", pid.String()).
					Errorf("action failed with: %v", err)
			}
			s.notify(entry.notifier)
		}
	}
}

func (s *SyncQueue) notify(notifier chan struct{}) {
	select {
	case notifier <- struct{}{}:
	default:
	}
}

func (s *SyncQueue) Schedule(p peer.ID, t thread.ID, c PeerCall) {
	s.RLock()
	entry, exists := s.entryMap[p]
	s.RUnlock()

	if !exists {
		s.Lock()
		// checking to be sure that somebody didn't update this concurrently
		if entry, exists = s.entryMap[p]; !exists {
			entry = &peerEntry{
				queue:    make([]PeerCall, 0, 10),
				notifier: make(chan struct{}, 1),
				Mutex:    sync.Mutex{},
			}
			s.entryMap[p] = entry
			go s.pollQueue(p, t, entry)
		}
		s.Unlock()
	}
	entry.Lock()
	defer entry.Unlock()
	entry.queue = append(entry.queue, c)
	s.notify(entry.notifier)
}
