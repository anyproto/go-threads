package net

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/textileio/go-threads/core/thread"
)

type (
	ConnectionStatus struct {
		Peer      peer.ID
		Connected bool
	}

	SyncStatus struct {
		LastPull int64
		Up, Down SyncState
	}

	SyncState uint8

	SyncSummary struct {
		InProgress int
		Synced     int
		Failed     int
		LastSync   int64
	}
)

const (
	Unknown SyncState = iota
	InProgress
	Success
	Failure
)

func (s SyncState) String() string {
	switch s {
	case InProgress:
		return "in-progress"
	case Failure:
		return "failure"
	case Success:
		return "success"
	default:
		return "unknown"
	}
}

type SyncInfo interface {
	// Watch connectivity with threads participants.
	Connectivity() (<-chan ConnectionStatus, error)

	// Status of thread sync with specified peer.
	Status(tid thread.ID, pid peer.ID) (SyncStatus, error)

	// Status of thread sync with all involved peers.
	View(id thread.ID) ([]SyncStatus, error)

	// Get sync summary for all threads participants.
	PeerSummary(id peer.ID) (SyncSummary, error)

	// Get sync summary for all peers involved in the thread.
	ThreadSummary(id thread.ID) (SyncSummary, error)
}
