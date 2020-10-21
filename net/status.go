package net

import (
	"hash/fnv"
	"sync"
	"sync/atomic"

	lnet "github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	tnet "github.com/textileio/go-threads/core/net"
	"github.com/textileio/go-threads/core/thread"
)

/* Per-thread synchronization status */

const shards = 32

type (
	// Event changing thread status
	ThreadStatusEvent uint8

	// internal compact representation
	threadStatus uint8

	// Tracking of threads sync with given peer
	threadStatusRegistry struct {
		shards   [shards]*threadStatusShard
		syncPeer peer.ID
	}

	threadStatusShard struct {
		data map[uint64]threadStatus
		sync.Mutex
	}
)

const (
	ThreadStatusDownloadStarted ThreadStatusEvent = 1 + iota
	ThreadStatusDownloadDone
	ThreadStatusDownloadFailed
	ThreadStatusUploadStarted
	ThreadStatusUploadDone
	ThreadStatusUploadFailed
)

//                             Thread status encoding scheme
//        +----------+-------------+---------+-------------+---------+-------------+
//        |          |          Uploading    |      Downloading      |    Status   |
// Field: | reserved +-------------+---------+-------------+---------+-------------+
//        |          | in-progress | success | in-progress | success | initialized |
//        +----------+-------------+---------+-------------+---------+-------------+
// Bits:  |    7-5   |      4      |    3    |      2      |    1    |      0      |
//        +----------+-------------+---------+-------------+---------+-------------+
const (
	statusInitialized = 1 << iota
	statusDownloadSuccess
	statusDownloadInProgress
	statusUploadSuccess
	statusUploadInProgress
)

func (s threadStatus) decode() tnet.ThreadSyncStatus {
	return tnet.ThreadSyncStatus{
		Initialized:        s&statusInitialized != 0,
		DownloadSuccess:    s&statusDownloadSuccess != 0,
		DownloadInProgress: s&statusDownloadInProgress != 0,
		UploadSuccess:      s&statusUploadSuccess != 0,
		UploadInProgress:   s&statusUploadInProgress != 0,
	}
}

func NewThreadStatusRegistry(pid peer.ID) *threadStatusRegistry {
	var ss [shards]*threadStatusShard
	for i := 0; i < shards; i++ {
		ss[i] = &threadStatusShard{
			data: make(map[uint64]threadStatus),
		}
	}

	return &threadStatusRegistry{shards: ss, syncPeer: pid}
}

func (t threadStatusRegistry) Get(tid thread.ID) tnet.ThreadSyncStatus {
	shard, hash := t.target(tid)

	shard.Lock()
	defer shard.Unlock()

	return shard.data[hash].decode()
}

func (t threadStatusRegistry) Apply(tid thread.ID, event ThreadStatusEvent) {
	shard, hash := t.target(tid)

	shard.Lock()
	defer shard.Unlock()

	shard.data[hash] = apply(shard.data[hash], event)
}

func (t threadStatusRegistry) Tracked(id peer.ID) bool {
	return id == t.syncPeer
}

// Count total number of initialized threads
func (t threadStatusRegistry) Total() int {
	var (
		total int32
		wg    sync.WaitGroup
	)

	for i := 0; i < shards; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			shard := t.shards[idx]
			shard.Lock()
			atomic.AddInt32(&total, int32(len(shard.data)))
			shard.Unlock()
		}(i)
	}
	wg.Wait()

	return int(total)
}

func (t threadStatusRegistry) target(tid thread.ID) (*threadStatusShard, uint64) {
	hasher := fnv.New64a()
	hasher.Write(tid.Bytes())
	hash := hasher.Sum64()
	shard := t.shards[int(hash%shards)]
	return shard, hash
}

func apply(status threadStatus, event ThreadStatusEvent) threadStatus {
	switch event {
	case ThreadStatusDownloadStarted:
		status = (status & (statusUploadInProgress | statusUploadSuccess)) | statusDownloadInProgress
	case ThreadStatusDownloadDone:
		status = (status & (statusUploadInProgress | statusUploadSuccess)) | statusDownloadSuccess
	case ThreadStatusDownloadFailed:
		status = status & (statusUploadInProgress | statusUploadSuccess)
	case ThreadStatusUploadStarted:
		status = (status & (statusDownloadInProgress | statusDownloadSuccess)) | statusUploadInProgress
	case ThreadStatusUploadDone:
		status = (status & (statusDownloadInProgress | statusDownloadSuccess)) | statusUploadSuccess
	case ThreadStatusUploadFailed:
		status = status & (statusDownloadInProgress | statusDownloadSuccess)
	}

	// thread status considered to be initialized on any applied event
	return status | statusInitialized
}

/* Connection to sync peer */

var _ lnet.Notifiee = (*connTracker)(nil)

type connTracker struct {
	net       lnet.Network
	syncPeer  peer.ID
	listeners []chan<- bool
	connected bool
	mu        sync.Mutex
}

// Tracks status of libp2p-provided connection to the specified peer.
// Note: here we track connections on a network layer only, and don't
// care about protocol negotiation, identity verification etc. For a
// more fine-grained control we should subscribe to host's EventBus
// and watch all the relevant events. More details about specific
// event types in the package github.com/go-libp2p-core/event.
func NewConnTracker(net lnet.Network, syncPeer peer.ID) *connTracker {
	return &connTracker{net: net, syncPeer: syncPeer}
}

func (n *connTracker) Start() {
	var connStatus bool
	switch n.net.Connectedness(n.syncPeer) {
	case lnet.Connected:
		connStatus = true
	default:
		connStatus = false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// set current connection status
	n.notify(connStatus)

	// register for network changes
	n.net.Notify(n)
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

func (n *connTracker) Notify() <-chan bool {
	var listener = make(chan bool, 1)

	n.mu.Lock()
	defer n.mu.Unlock()

	// notify about current status immediately
	listener <- n.connected
	n.listeners = append(n.listeners, listener)

	return listener
}

// should be invoked under the lock acquired
func (n *connTracker) notify(connStatus bool) {
	n.connected = connStatus
	for _, listener := range n.listeners {
		select {
		case listener <- connStatus:
		default:
			log.Errorf("connTracker notification failed")
		}
	}
}

func (n *connTracker) Connected(_ lnet.Network, conn lnet.Conn) {
	if conn.RemotePeer() != n.syncPeer {
		return
	}
	n.mu.Lock()
	n.notify(true)
	n.mu.Unlock()
}

func (n *connTracker) Disconnected(_ lnet.Network, conn lnet.Conn) {
	if conn.RemotePeer() != n.syncPeer {
		return
	}
	n.mu.Lock()
	n.notify(false)
	n.mu.Unlock()
}

func (n *connTracker) Listen(lnet.Network, ma.Multiaddr)      {}
func (n *connTracker) ListenClose(lnet.Network, ma.Multiaddr) {}
func (n *connTracker) OpenedStream(lnet.Network, lnet.Stream) {}
func (n *connTracker) ClosedStream(lnet.Network, lnet.Stream) {}
