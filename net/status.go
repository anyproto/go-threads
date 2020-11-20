package net

import (
	"hash/fnv"
	"sync"
	"time"

	lnet "github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	core "github.com/textileio/go-threads/core/logstore"
	tnet "github.com/textileio/go-threads/core/net"
	"github.com/textileio/go-threads/core/thread"
)

/* Thread synchronization status */

const shards = 8

type (
	// Event changing thread status
	threadStatusEvent uint8

	// Tracking of threads sync with peers
	threadStatusRegistry struct {
		syncBook  core.SyncBook
		peers     map[peer.ID][shards]*threadStatusShard
		onNewPeer []func(peer.ID)
		mu        sync.RWMutex
	}

	// Sharded thread sync information
	threadStatusShard struct {
		data map[uint64]tnet.SyncStatus
		sync.Mutex
	}
)

const (
	threadStatusDownloadStarted threadStatusEvent = 1 + iota
	threadStatusDownloadDone
	threadStatusDownloadFailed
	threadStatusUploadStarted
	threadStatusUploadDone
	threadStatusUploadFailed
)

func newPeerStatus() [shards]*threadStatusShard {
	var ps [shards]*threadStatusShard
	for i := 0; i < shards; i++ {
		ps[i] = &threadStatusShard{
			data: make(map[uint64]tnet.SyncStatus),
		}
	}
	return ps
}

func NewThreadStatusRegistry(
	syncBook core.SyncBook,
	onNewPeer ...func(pid peer.ID),
) (*threadStatusRegistry, error) {
	registry := &threadStatusRegistry{
		peers:     make(map[peer.ID][shards]*threadStatusShard),
		syncBook:  syncBook,
		onNewPeer: onNewPeer,
	}

	if syncBook != nil {
		// initialize registry from persistence layer
		dump, err := syncBook.DumpSync()
		if err != nil {
			return nil, err
		}
		registry.initFromDump(dump)
	}

	return registry, nil
}

func (t *threadStatusRegistry) Apply(pid peer.ID, tid thread.ID, event threadStatusEvent) {
	shard, hash, _ := t.shard(pid, tid, true)

	shard.Lock()
	defer shard.Unlock()

	shard.data[hash] = t.apply(shard.data[hash], event)
}

func (t *threadStatusRegistry) Status(tid thread.ID, pid peer.ID) tnet.SyncStatus {
	shard, hash, found := t.shard(pid, tid, false)
	if !found {
		return tnet.SyncStatus{}
	}

	shard.Lock()
	defer shard.Unlock()

	return shard.data[hash]
}

func (t *threadStatusRegistry) View(tid thread.ID) map[peer.ID]tnet.SyncStatus {
	type peerStatus struct {
		pid    peer.ID
		status tnet.SyncStatus
	}

	var (
		threadHash = t.hash(tid.Bytes())
		shardIdx   = int(threadHash % shards)
	)

	t.mu.RLock()
	var (
		numPeers     = len(t.peers)
		threadShards = make(map[peer.ID]*threadStatusShard, numPeers)
	)
	for pid, ss := range t.peers {
		threadShards[pid] = ss[shardIdx]
	}
	t.mu.RUnlock()

	var (
		sink = make(chan peerStatus, numPeers)
		view = make(map[peer.ID]tnet.SyncStatus)
		sema = make(chan struct{}, shards)
	)
	defer close(sink)

	for pid, shard := range threadShards {
		// limiting concurrency
		sema <- struct{}{}
		go func(p peer.ID, sh *threadStatusShard) {
			sh.Lock()
			var status = sh.data[threadHash]
			sh.Unlock()
			sink <- peerStatus{pid: p, status: status}
			<-sema
		}(pid, shard)
	}

	// not every peer is involved into a thread, so we
	// should filter and collect non-empty statuses only
	for i := 0; i < numPeers; i++ {
		if ps := <-sink; !zeroStatus(ps.status) {
			view[ps.pid] = ps.status
		}
	}

	return view
}

func (t *threadStatusRegistry) PeerSummary(pid peer.ID) tnet.SyncSummary {
	t.mu.RLock()
	ps, found := t.peers[pid]
	t.mu.RUnlock()
	if !found {
		return tnet.SyncSummary{}
	}

	var (
		sink = make(chan tnet.SyncStatus, shards)
		wg   sync.WaitGroup
	)

	wg.Add(len(ps))
	go func() { wg.Wait(); close(sink) }()

	for _, sh := range ps {
		go func(shard *threadStatusShard) {
			shard.Lock()
			var ss = make([]tnet.SyncStatus, 0, len(shard.data))
			for _, status := range shard.data {
				if status.Down != tnet.Unknown {
					ss = append(ss, status)
				}
			}
			shard.Unlock()

			for _, status := range ss {
				sink <- status
			}
			wg.Done()
		}(sh)
	}

	return t.summary(sink)
}

func (t *threadStatusRegistry) ThreadSummary(tid thread.ID) tnet.SyncSummary {
	var (
		view = t.View(tid)
		sink = make(chan tnet.SyncStatus, len(view))
	)

	for _, status := range view {
		sink <- status
	}
	close(sink)

	return t.summary(sink)
}

func (t *threadStatusRegistry) Close() error {
	if t.syncBook == nil {
		return nil
	}

	var syncData = make(map[peer.ID]map[uint64]tnet.SyncStatus)

	t.mu.RLock()
	for pid, ps := range t.peers {
		syncData[pid] = make(map[uint64]tnet.SyncStatus)
		for _, shard := range ps {
			shard.Lock()
			for tHash, status := range shard.data {
				syncData[pid][tHash] = status
			}
			shard.Unlock()
		}
	}
	t.mu.RUnlock()

	return t.syncBook.RestoreSync(core.DumpSyncBook{Data: syncData})
}

func (t *threadStatusRegistry) initFromDump(dump core.DumpSyncBook) {
	for pid, ts := range dump.Data {
		t.peers[pid] = newPeerStatus()
		for tHash, s := range ts {
			shard := int(tHash % shards)
			t.peers[pid][shard].data[tHash] = s
		}
	}
}

func (t *threadStatusRegistry) hash(b []byte) uint64 {
	hasher := fnv.New64a()
	hasher.Write(b)
	return hasher.Sum64()
}

func (t *threadStatusRegistry) shard(
	pid peer.ID,
	tid thread.ID,
	update bool,
) (*threadStatusShard, uint64, bool) {
	t.mu.RLock()
	ps, found := t.peers[pid]
	t.mu.RUnlock()

	if !found {
		if !update {
			return nil, 0, false
		}

		ps = newPeerStatus()
		t.mu.Lock()
		if psExisting, ok := t.peers[pid]; !ok {
			t.peers[pid] = ps
			for _, cb := range t.onNewPeer {
				go cb(pid)
			}

		} else {
			// was already created by another process
			ps = psExisting
		}
		t.mu.Unlock()
	}

	threadHash := t.hash(tid.Bytes())
	shard := ps[int(threadHash%shards)]
	return shard, threadHash, true
}

func (t *threadStatusRegistry) apply(status tnet.SyncStatus, event threadStatusEvent) tnet.SyncStatus {
	switch event {
	case threadStatusDownloadStarted:
		status.Down = tnet.InProgress
	case threadStatusDownloadDone:
		status.Down = tnet.Success
		status.LastPull = time.Now().Unix()
	case threadStatusDownloadFailed:
		status.Down = tnet.Failure
	case threadStatusUploadStarted:
		status.Up = tnet.InProgress
	case threadStatusUploadDone:
		status.Up = tnet.Success
	case threadStatusUploadFailed:
		status.Up = tnet.Failure
	}

	return status
}

func (t *threadStatusRegistry) summary(ss <-chan tnet.SyncStatus) tnet.SyncSummary {
	var sum tnet.SyncSummary

	for status := range ss {
		// Here we're extracting summary from the receiving
		// status part only b/c uploading stats are inherently
		// less stable. Irregular pushing data to the nodes is
		// more likely to be in an irrelevant failed state than
		// just periodic pulls.
		switch status.Down {
		case tnet.InProgress:
			sum.InProgress += 1
		case tnet.Failure:
			sum.Failed += 1
		case tnet.Success:
			sum.Synced += 1
		}
		if status.LastPull > sum.LastSync {
			sum.LastSync = status.LastPull
		}
	}

	return sum
}

func zeroStatus(s tnet.SyncStatus) bool {
	return s.Up == tnet.Unknown && s.Down == tnet.Unknown && s.LastPull == 0
}

/* Peer connectivity */

var _ lnet.Notifiee = (*connTracker)(nil)

type connTracker struct {
	net       lnet.Network
	conns     map[peer.ID]bool
	listeners []chan<- tnet.ConnectionStatus
	mu        sync.RWMutex
}

// Tracks status of libp2p-provided connection to the peers
// participating in threads operations.
// Note: here we track connections on a network layer only, and don't
// care about protocol negotiation, identity verification etc. For a
// more fine-grained control we should subscribe to host's EventBus
// and watch all the relevant events. More details about specific
// event types in the package github.com/go-libp2p-core/event.
func NewConnTracker(net lnet.Network) *connTracker {
	var ct = connTracker{
		net:   net,
		conns: make(map[peer.ID]bool),
	}

	// register for network changes
	net.Notify(&ct)
	return &ct
}

// Start tracking connectivity of a given peer.
func (n *connTracker) Track(pid peer.ID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exist := n.conns[pid]; exist {
		return
	}

	var connStatus bool
	switch n.net.Connectedness(pid) {
	case lnet.Connected:
		connStatus = true
	default:
		connStatus = false
	}

	// set current connection status
	n.conns[pid] = connStatus

	// send current status to all listeners
	n.notify(pid, connStatus)
}

// Do not use connTracker after Close!
func (n *connTracker) Close() {
	n.net.StopNotify(n)

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, listener := range n.listeners {
		close(listener)
	}
}

func (n *connTracker) Notify() <-chan tnet.ConnectionStatus {
	n.mu.Lock()
	defer n.mu.Unlock()

	var listener = make(chan tnet.ConnectionStatus, len(n.conns))

	// add a new listener
	n.listeners = append(n.listeners, listener)

	// notify about current statuses immediately
	for pid, status := range n.conns {
		listener <- tnet.ConnectionStatus{
			Peer:      pid,
			Connected: status,
		}
	}

	return listener
}

// internal methods should be invoked under the lock acquired!

func (n *connTracker) notify(pid peer.ID, connStatus bool) {
	for _, listener := range n.listeners {
		select {
		case listener <- tnet.ConnectionStatus{
			Peer:      pid,
			Connected: connStatus,
		}:
		default:
			log.Errorf("connTracker notification failed")
		}
	}
}

func (n *connTracker) tracked(pid peer.ID) bool {
	_, found := n.conns[pid]
	return found
}

/* notifiee implementation */

func (n *connTracker) Connected(_ lnet.Network, conn lnet.Conn) {
	var pid = conn.RemotePeer()

	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.tracked(pid) {
		n.notify(pid, true)

	}
}

func (n *connTracker) Disconnected(_ lnet.Network, conn lnet.Conn) {
	var pid = conn.RemotePeer()

	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.tracked(pid) {
		n.notify(pid, false)
	}
}

func (n *connTracker) Listen(lnet.Network, ma.Multiaddr)      {}
func (n *connTracker) ListenClose(lnet.Network, ma.Multiaddr) {}
func (n *connTracker) OpenedStream(lnet.Network, lnet.Stream) {}
func (n *connTracker) ClosedStream(lnet.Network, lnet.Stream) {}
