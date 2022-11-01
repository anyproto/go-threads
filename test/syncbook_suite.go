package test

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	core "github.com/textileio/go-threads/core/logstore"
	"github.com/textileio/go-threads/core/net"
)

var syncBookSuite = map[string]func(kb core.SyncBook) func(*testing.T){
	"ExportSyncBook": testSyncBookExport,
}

type SyncBookFactory func() (core.SyncBook, func())

func SyncBookTest(t *testing.T, factory SyncBookFactory) {
	for name, test := range syncBookSuite {
		// Create a new book.
		sb, closeFunc := factory()

		// Run the test.
		t.Run(name, test(sb))

		// Cleanup.
		if closeFunc != nil {
			closeFunc()
		}
	}
}

func testSyncBookExport(sb core.SyncBook) func(t *testing.T) {
	return func(t *testing.T) {
		var (
			now      = time.Now().Unix()
			syncInfo = map[peer.ID]map[uint64]net.SyncStatus{
				"a": {
					1: {
						LastPull: now - 30,
						Up:       net.InProgress,
						Down:     net.Success,
					},
					2: {
						LastPull: now - 60,
						Up:       net.Failure,
						Down:     net.InProgress,
					},
					4: {
						LastPull: now - 120,
						Up:       net.Success,
						Down:     net.Success,
					},
				},
				"b": {
					2: {
						LastPull: now - 15,
						Up:       net.Unknown,
						Down:     net.Failure,
					},
					3: {
						LastPull: now - 45,
						Up:       net.Success,
						Down:     net.Success,
					},
					4: {
						LastPull: now - 90,
						Up:       net.InProgress,
						Down:     net.InProgress,
					},
				},
			}
		)

		if err := sb.RestoreSync(core.DumpSyncBook{Data: syncInfo}); err != nil {
			t.Fatal(err)
		}

		dump, err := sb.DumpSync()
		if err != nil {
			t.Fatal(err)
		}

		if !equalSyncDumps(dump, core.DumpSyncBook{Data: syncInfo}) {
			t.Error("dump is different from original sync information")
		}
	}
}

func equalSyncDumps(d1, d2 core.DumpSyncBook) bool {
	if len(d1.Data) != len(d2.Data) {
		return false
	}

	for pid, ps1 := range d1.Data {
		ps2, ok := d2.Data[pid]
		if !ok || len(ps1) != len(ps2) {
			return false
		}

		for tHash, s1 := range ps1 {
			s2, ok := ps2[tHash]
			if !ok || s1 != s2 {
				return false
			}
		}
	}

	return true
}
