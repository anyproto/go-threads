package queue

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/textileio/go-threads/core/thread"
)

type SyncQueueAction int

const (
	PushBack SyncQueueAction = iota
	PushFront
	ReplaceAll
)

type threadEntry struct {
	call PeerCall
	id   thread.ID
}

type peerEntry struct {
	queue    []threadEntry
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

func (s *SyncQueue) pollQueue(pid peer.ID, entry *peerEntry) {
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
			te := entry.queue[0]
			entry.queue = entry.queue[1:]
			entry.Unlock()
			err := te.call(s.ctx, pid, te.id)
			if err != nil {
				log.With("thread", te.id.String()).With("peer", pid.String()).
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

func (s *SyncQueue) PushBack(pid peer.ID, tid thread.ID, c PeerCall) {
	s.performAction(pid, tid, c, PushBack)
}

func (s *SyncQueue) PushFront(pid peer.ID, tid thread.ID, c PeerCall) {
	s.performAction(pid, tid, c, PushFront)
}

func (s *SyncQueue) ReplaceQueue(pid peer.ID, tid thread.ID, c PeerCall) {
	s.performAction(pid, tid, c, ReplaceAll)
}

func (s *SyncQueue) performAction(pid peer.ID, tid thread.ID, c PeerCall, action SyncQueueAction) {
	s.RLock()
	entry, exists := s.entryMap[pid]
	s.RUnlock()

	if !exists {
		s.Lock()
		// checking to be sure that somebody didn't update this concurrently
		if entry, exists = s.entryMap[pid]; !exists {
			entry = &peerEntry{
				queue:    make([]threadEntry, 0, 10),
				notifier: make(chan struct{}, 1),
				Mutex:    sync.Mutex{},
			}
			s.entryMap[pid] = entry
			go s.pollQueue(pid, entry)
		}
		s.Unlock()
	}
	entry.Lock()
	defer entry.Unlock()
	te := threadEntry{
		call: c,
		id:   tid,
	}

	switch action {
	case PushBack:
		entry.queue = append(entry.queue, te)
	case PushFront:
		entry.queue = append([]threadEntry{te}, entry.queue...)
	case ReplaceAll:
		entry.queue = []threadEntry{te}
	}

	s.notify(entry.notifier)
}
