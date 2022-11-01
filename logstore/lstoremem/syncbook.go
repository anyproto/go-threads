package lstoremem

import (
	"github.com/libp2p/go-libp2p/core/peer"
	core "github.com/textileio/go-threads/core/logstore"
	"github.com/textileio/go-threads/core/net"
)

type memorySyncBook struct {
	peers map[peer.ID]map[uint64]net.SyncStatus
}

var _ core.SyncBook = (*memorySyncBook)(nil)

func NewSyncBook() core.SyncBook {
	return &memorySyncBook{}
}

func (m memorySyncBook) DumpSync() (core.DumpSyncBook, error) {
	return core.DumpSyncBook{Data: m.peers}, nil
}

func (m *memorySyncBook) RestoreSync(dump core.DumpSyncBook) error {
	m.peers = dump.Data
	return nil
}
