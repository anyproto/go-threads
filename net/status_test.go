package net

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	core "github.com/textileio/go-threads/core/net"
	"github.com/textileio/go-threads/core/thread"
)

func TestThreadStatusRegistry_Status(t *testing.T) {
	var (
		pid = peer.ID("test-peer")
		tid = thread.NewIDV1(thread.Raw, 24)

		newPeerDetected      = false
		connectivityCallback = func(id peer.ID) {
			if id == pid {
				newPeerDetected = true
			} else {
				panic("unexpected peer")
			}
		}
	)

	reg, err := NewThreadStatusRegistry(nil, connectivityCallback)
	if err != nil {
		t.Fatalf("thread status registry construction failed: %v", err)
	}

	status := reg.Status(tid, pid)
	if newPeerDetected {
		t.Errorf("new peer should not be tracked until first event")
	}
	if status.Up != core.Unknown || status.Down != core.Unknown {
		t.Errorf("status must be unknown until first event")
	}

	// send events
	runScenario1(reg, pid, tid)

	// let callback a chance to be invoked
	time.Sleep(20 * time.Millisecond)
	if !newPeerDetected {
		t.Errorf("new peer must be detected")
	}

	status = reg.Status(tid, pid)
	if expected, actual := core.Failure, status.Up; expected != actual {
		t.Errorf("bad upload status, expected: %s, actual: %s", expected, actual)
	}
	if expected, actual := core.Success, status.Down; expected != actual {
		t.Errorf("bad download status, expected: %s, actual: %s", expected, actual)
	}
	if status.LastPull <= 0 {
		t.Errorf("last pull time must be defined after successfull download")
	}
}

func TestThreadStatusRegistry_View(t *testing.T) {
	var (
		pid1 = peer.ID("test-peer-1")
		pid2 = peer.ID("test-peer-2")
		pid3 = peer.ID("test-peer-3")
		tid1 = thread.NewIDV1(thread.Raw, 24)
		tid2 = thread.NewIDV1(thread.Raw, 24)
	)

	reg, err := NewThreadStatusRegistry(nil)
	if err != nil {
		t.Fatalf("thread status registry construction failed: %v", err)
	}

	runScenario2(reg, pid1, pid2, pid3, tid1, tid2)

	view := reg.View(tid1)
	if len(view) != 2 {
		t.Errorf("incomplete view")
	}

	if view[pid1].Up != core.Success || view[pid1].Down != core.InProgress ||
		view[pid3].Up != core.Failure || view[pid3].Down != core.Success {
		t.Errorf("bad view: %+v", view)
	}
}

func TestThreadStatusRegistry_Summary(t *testing.T) {
	var (
		pid1 = peer.ID("test-peer-1")
		pid2 = peer.ID("test-peer-2")
		pid3 = peer.ID("test-peer-3")
		tid1 = thread.NewIDV1(thread.Raw, 24)
		tid2 = thread.NewIDV1(thread.Raw, 24)
	)

	reg, err := NewThreadStatusRegistry(nil)
	if err != nil {
		t.Fatalf("thread status registry construction failed: %v", err)
	}

	runScenario2(reg, pid1, pid2, pid3, tid1, tid2)

	tests := []struct {
		name             string
		expected, actual core.SyncSummary
	}{
		{
			name:     "thread #1",
			expected: core.SyncSummary{InProgress: 1, Synced: 1, Failed: 0},
			actual:   reg.ThreadSummary(tid1),
		},
		{
			name:     "thread #2",
			expected: core.SyncSummary{InProgress: 1, Synced: 1, Failed: 0},
			actual:   reg.ThreadSummary(tid2),
		},
		{
			name:     "peer-1",
			expected: core.SyncSummary{InProgress: 1, Synced: 0, Failed: 0},
			actual:   reg.PeerSummary(pid1),
		},
		{
			name:     "peer-2",
			expected: core.SyncSummary{InProgress: 0, Synced: 1, Failed: 0},
			actual:   reg.PeerSummary(pid2),
		},
		{
			name:     "peer-3",
			expected: core.SyncSummary{InProgress: 1, Synced: 1, Failed: 0},
			actual:   reg.PeerSummary(pid3),
		},
	}

	for _, tt := range tests {
		if !equalSummary(tt.expected, tt.actual) {
			t.Errorf("bad summary for %s, expected: %+v, actual: %+v",
				tt.name, tt.expected, tt.actual)
		}
	}
}

func runScenario1(reg *threadStatusRegistry, p1 peer.ID, t1 thread.ID) {
	// Scenario:
	//
	// Peer starts syncing single thread, pulling
	// succeeded, but push changes failed.
	reg.Apply(p1, t1, threadStatusDownloadStarted)
	reg.Apply(p1, t1, threadStatusUploadStarted)
	reg.Apply(p1, t1, threadStatusDownloadDone)
	reg.Apply(p1, t1, threadStatusUploadFailed)
}

func runScenario2(reg *threadStatusRegistry, p1, p2, p3 peer.ID, t1, t2 thread.ID) {
	// Scenario:
	//
	// Peers 1 participate in thread #1 only, peer 2 - in thread #2 only,
	// while peer 3 sync both threads.
	// Peer 3 failed to upload changes, peer 1 is still downloading,
	// peer 3 sync with thread #2 is in progress, as well. Remaining
	// syncs successfully finished.
	reg.Apply(p1, t1, threadStatusDownloadStarted)
	reg.Apply(p3, t1, threadStatusDownloadStarted)
	reg.Apply(p1, t1, threadStatusUploadStarted)
	reg.Apply(p3, t1, threadStatusUploadStarted)
	reg.Apply(p3, t1, threadStatusDownloadDone)
	reg.Apply(p1, t1, threadStatusUploadDone)
	reg.Apply(p3, t1, threadStatusUploadFailed)

	reg.Apply(p2, t2, threadStatusDownloadStarted)
	reg.Apply(p2, t2, threadStatusUploadStarted)
	reg.Apply(p2, t2, threadStatusDownloadDone)
	reg.Apply(p2, t2, threadStatusUploadDone)
	reg.Apply(p3, t2, threadStatusDownloadStarted)
}

// just ignore sync time field
func equalSummary(s1, s2 core.SyncSummary) bool {
	return s1.InProgress == s2.InProgress &&
		s1.Failed == s2.Failed &&
		s1.Synced == s2.Synced
}
