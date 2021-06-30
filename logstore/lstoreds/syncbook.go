package lstoreds

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-core/peer"
	core "github.com/textileio/go-threads/core/logstore"
	"github.com/textileio/go-threads/core/net"
	pb "github.com/textileio/go-threads/net/pb"
)

type dsSyncBook struct {
	ds ds.TxnDatastore
}

// Sync information is stored in db key pattern:
// /thread/sync/<b32 log id no padding>
var (
	sbBase               = ds.NewKey("/thread/sync")
	_      core.SyncBook = (*dsSyncBook)(nil)
)

// NewSyncBook returns a new SyncBook backed by a datastore.
func NewSyncBook(ds ds.TxnDatastore) core.SyncBook {
	return &dsSyncBook{ds: ds}
}

func (sb dsSyncBook) DumpSync() (core.DumpSyncBook, error) {
	var dump = core.DumpSyncBook{Data: make(map[peer.ID]map[uint64]net.SyncStatus)}

	results, err := sb.ds.Query(query.Query{Prefix: sbBase.String()})
	if err != nil {
		return dump, err
	}
	defer results.Close()

	for entry := range results.Next() {
		var sr pb.SyncBookRecord
		if err := proto.Unmarshal(entry.Value, &sr); err != nil {
			return dump, fmt.Errorf("cannot decode syncbook record: %w", err)
		}

		var pid = sr.PeerID.ID
		peerSync, ok := dump.Data[pid]
		if !ok {
			peerSync = make(map[uint64]net.SyncStatus)
			dump.Data[pid] = peerSync
		}

		for _, se := range sr.Entries {
			peerSync[se.THash] = net.SyncStatus{
				LastPull: se.LastPull,
				Up:       decodeSyncState(se.UpStatus),
				Down:     decodeSyncState(se.DownStatus),
			}
		}
	}

	return dump, nil
}

func (sb dsSyncBook) RestoreSync(dump core.DumpSyncBook) error {
	if !AllowEmptyRestore && len(dump.Data) == 0 {
		return core.ErrEmptyDump
	}

	txn, err := sb.ds.NewTransaction(false)
	if err != nil {
		return fmt.Errorf("error when creating txn in datastore: %w", err)
	}
	defer txn.Discard()

	for pid, peerSync := range dump.Data {
		var (
			key = dsPeerKey(pid, sbBase)
			se  = pb.SyncBookRecord{
				PeerID:  &pb.ProtoPeerID{ID: pid},
				Entries: nil,
			}
		)

		for tHash, status := range peerSync {
			se.Entries = append(se.Entries, &pb.SyncBookRecord_SyncEntry{
				THash:      tHash,
				UpStatus:   encodeSyncState(status.Up),
				DownStatus: encodeSyncState(status.Down),
				LastPull:   status.LastPull,
			})
		}

		data, err := proto.Marshal(&se)
		if err != nil {
			return fmt.Errorf("error when marshaling syncbookrecord proto for %v: %w", key, err)
		}
		if err = txn.Put(key, data); err != nil {
			return fmt.Errorf("error when saving new syncbook record in datastore for %v: %v", key, err)
		}
	}

	return txn.Commit()
}

func encodeSyncState(s net.SyncState) uint32 {
	switch s {
	case net.Unknown:
		return 0
	case net.InProgress:
		return 1
	case net.Success:
		return 2
	case net.Failure:
		return 3
	default:
		panic("encoding unsupported sync state")
	}
}

func decodeSyncState(s uint32) net.SyncState {
	switch s {
	case 0:
		return net.Unknown
	case 1:
		return net.InProgress
	case 2:
		return net.Success
	case 3:
		return net.Failure
	default:
		panic("decoding unsupported sync state")
	}
}
