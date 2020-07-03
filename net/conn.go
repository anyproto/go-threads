package net

import (
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	stdNet "net"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
)

var (
	// ErrSkipDial indicates system decided to skip establishing peer connection.
	ErrSkipDial = errors.New("skip peer dial")

	// ErrPeerActivityNotFound indicates activity record was not found for requested peer.
	ErrPeerActivityNotFound = errors.New("peer activity not found")

	// Root key for storing peers activity in peerstore.
	psTrackerKey = "/peer/activity"
)

type DialPolicy interface {
	Decide(info PeerActivity, at int64) bool
	InterruptFailed() bool
}

type PeerTracker interface {
	Get(id peer.ID) (PeerActivity, error)
	Update(id peer.ID, info PeerActivity) error
}

type PeerActivity struct {
	LastSeenAt  int64
	LastTriedAt int64
}

func peerActivityNow(seen, tried bool) (pa PeerActivity) {
	var now = time.Now().Unix()
	if seen {
		pa.LastSeenAt = now
	}
	if tried {
		pa.LastTriedAt = now
	}
	return
}

/* Peer tracking */

// Stub to be used with non-tracking dial policies.
func NewStubTracker() *stubTracker { return &stubTracker{} }

var _ PeerTracker = (*stubTracker)(nil)

type stubTracker struct{}

func (t stubTracker) Get(_ peer.ID) (PeerActivity, error) {
	return PeerActivity{}, nil
}

func (t stubTracker) Update(_ peer.ID, _ PeerActivity) error {
	return nil
}

// Gob-serializing implementation, uses libp2p-peerstore's metadata storage.
func NewPeerStoreTracker(ps pstore.Peerstore) *pstoreTracker {
	return &pstoreTracker{ps: ps}
}

var _ PeerTracker = (*pstoreTracker)(nil)

type pstoreTracker struct {
	ps pstore.Peerstore
}

func (t *pstoreTracker) Get(id peer.ID) (PeerActivity, error) {
	var p PeerActivity
	if m, err := t.ps.Get(id, psTrackerKey); err != nil {
		if errors.Is(err, pstore.ErrNotFound) {
			return p, ErrPeerActivityNotFound
		}
		return p, err
	} else {
		p = m.(PeerActivity)
	}
	return p, nil
}

func (t *pstoreTracker) Update(id peer.ID, info PeerActivity) error {
	prev, err := t.Get(id)
	if err != nil && !errors.Is(err, ErrPeerActivityNotFound) {
		return err
	}

	// update times monotonically
	if info.LastTriedAt < prev.LastTriedAt {
		info.LastTriedAt = prev.LastTriedAt
	}
	if info.LastSeenAt < prev.LastSeenAt {
		info.LastSeenAt = prev.LastSeenAt
	}

	return t.ps.Put(id, psTrackerKey, info)
}

// Wrapper for standard network listener able to track incoming connections from libp2p.
func TrackIncomingPeers(ln stdNet.Listener, t PeerTracker) stdNet.Listener {
	return trackingWrapper{ln, t}
}

var _ stdNet.Listener = (*trackingWrapper)(nil)

type trackingWrapper struct {
	stdNet.Listener
	t PeerTracker
}

func (w trackingWrapper) Accept() (stdNet.Conn, error) {
	conn, err := w.Listener.Accept()
	if err != nil {
		return nil, fmt.Errorf("accept underlying: %w", err)
	}

	addr := w.Listener.Addr().String()
	peerID, err := peer.Decode(addr)
	if err != nil {
		return nil, fmt.Errorf("bad peer ID %s: %w", addr, err)
	}

	if err := w.t.Update(peerID, peerActivityNow(true, false)); err != nil {
		return nil, fmt.Errorf("update tracking information: %w", err)
	}

	return conn, nil
}

/* Dial policies */

// Simplest policy, reproduces system behaviour with uncontrolled dialing.
func NewStubPolicy() *stubPolicy { return &stubPolicy{} }

var _ DialPolicy = (*stubPolicy)(nil)

type stubPolicy struct{}

func (s stubPolicy) Decide(_ PeerActivity, _ int64) bool { return true }

func (s stubPolicy) InterruptFailed() bool { return false }

// Randomized policy where dial probability depends on both offline
// duration and time elapsed from the last connection attempt.
func NewProbabilisticPolicy() *probabilisticPolicy {
	return &probabilisticPolicy{}
}

var _ DialPolicy = (*probabilisticPolicy)(nil)

type probabilisticPolicy struct{}

func (s *probabilisticPolicy) Decide(info PeerActivity, at int64) bool {
	const (
		lastSeenThreshold  = 60
		lastTriedThreshold = 60
	)

	// TODO: implement meaningful probabilistic logic here!

	var confidence = 1.0
	if info.LastSeenAt > 0 && at-info.LastSeenAt > lastSeenThreshold {
		confidence -= 0.7
	}
	if at-info.LastTriedAt > lastTriedThreshold {
		confidence += 0.5
	}

	// roll the dice
	return rand.Float64() < confidence
}

func (s *probabilisticPolicy) InterruptFailed() bool {
	return true
}

func init() {
	// required by metastore
	gob.Register(PeerActivity{})
}
