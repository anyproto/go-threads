package net

import (
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/textileio/go-threads/core/thread"
)

const shards = 32

type (
	// Event changing thread status
	ThreadStatusEvent uint8

	// interpreted as a set of flags
	ThreadStatus struct {
		Initialized, UploadInProgress, UploadSuccess, DownloadInProgress, DownloadSuccess bool
	}

	// internal compact representation
	threadStatus uint8

	// Tracking of threads sync with given peer
	ThreadStatusRegistry struct {
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

//                        Thread status encoding scheme
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

func (s threadStatus) decode() ThreadStatus {
	return ThreadStatus{
		Initialized:        s&statusInitialized != 0,
		DownloadSuccess:    s&statusDownloadSuccess != 0,
		DownloadInProgress: s&statusDownloadInProgress != 0,
		UploadSuccess:      s&statusUploadSuccess != 0,
		UploadInProgress:   s&statusUploadInProgress != 0,
	}
}

func NewThreadStatusRegistry(pid peer.ID) *ThreadStatusRegistry {
	var ss [shards]*threadStatusShard
	for i := 0; i < shards; i++ {
		ss[i] = &threadStatusShard{
			data: make(map[uint64]threadStatus),
		}
	}

	return &ThreadStatusRegistry{shards: ss, syncPeer: pid}
}

func (t ThreadStatusRegistry) Get(tid thread.ID) ThreadStatus {
	shard, hash := t.target(tid)

	shard.Lock()
	defer shard.Unlock()

	return shard.data[hash].decode()
}

func (t ThreadStatusRegistry) Apply(tid thread.ID, event ThreadStatusEvent) {
	shard, hash := t.target(tid)

	shard.Lock()
	defer shard.Unlock()

	shard.data[hash] = apply(shard.data[hash], event)
}

func (t ThreadStatusRegistry) Tracked(id peer.ID) bool {
	return id == t.syncPeer
}

// Count total number of initialized threads
func (t ThreadStatusRegistry) Total() int {
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

func (t ThreadStatusRegistry) target(tid thread.ID) (*threadStatusShard, uint64) {
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
