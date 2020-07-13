package net

import (
	"encoding/gob"
	"errors"
	"fmt"
	"math"
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

/* Noop policy */

// Simplest policy, reproduces system behaviour with uncontrolled dialing.
func NewStubPolicy() *stubPolicy { return &stubPolicy{} }

var _ DialPolicy = (*stubPolicy)(nil)

type stubPolicy struct{}

func (s stubPolicy) Decide(_ PeerActivity, _ int64) bool { return true }

func (s stubPolicy) InterruptFailed() bool { return false }

/* Probabilistic policy */

type ProbabilisticPolicyConfig struct {
	// Lowest peer dial probability, must be positive to
	// give a small chance of dial at the random moment
	// and avoid massive coordinated dials.
	ProbabilityLowBound float64 // P_low

	// Interval from the last moment peer was detected,
	// after which we start exponentially slowing down
	// initially intensive re-dials.
	RecentlySeenPhaseStart time.Duration // S1

	// Interval from the last moment peer was detected,
	// after which we stop slowing down dialing probability
	// and it is sustained at the predefined low boundary.
	RecentlySeenPhaseEnd time.Duration // S2

	// Interval from the last moment given peer was
	// unsuccessfully dialed, after which we start
	// linearly increasing probability of re-dial.
	LongTriedPhaseStart time.Duration // T1

	// Interval from the last moment given peer was
	// unsuccessfully dialed, after which we reach
	// and sustain dialing probability of 1.
	LongTriedPhaseEnd time.Duration // T2

	// Minimum span between last-tried and last-seen moments
	// at which we start dampening re-dial probability obtained
	// from the LongTriedPhase by lowering corresponding
	// coefficient from 1 down to DampeningFactor.
	DampeningPhaseStart time.Duration // D1

	// Span between last-tried and last-seen moments at which
	// we stop lowering dampen-coefficient of re-dialing.
	// Coefficient value don't go below the DampeningFactor.
	DampeningPhaseEnd time.Duration // D2

	// Lowest value of reduction factor applied to the re-dialing
	// probability obtained by the LongTriedPhase.
	DampeningFactor float64 // C_d
}

func DefaultProbabilisticPolicyConfig() *ProbabilisticPolicyConfig {
	return &ProbabilisticPolicyConfig{
		ProbabilityLowBound:    0.05,
		RecentlySeenPhaseStart: 10 * time.Second,
		RecentlySeenPhaseEnd:   30 * time.Second,
		LongTriedPhaseStart:    5 * time.Minute,
		LongTriedPhaseEnd:      10 * time.Minute,
		DampeningPhaseStart:    20 * time.Minute,
		DampeningPhaseEnd:      3 * time.Hour,
		DampeningFactor:        0.3,
	}
}

// Randomized policy with dial probability depending on both offline
// period duration and the time elapsed from the last connection attempt.
func NewProbabilisticPolicy(conf *ProbabilisticPolicyConfig) *probabilisticPolicy {
	dampSlope := (1.0 - conf.DampeningFactor) / (conf.DampeningPhaseEnd.Seconds() - conf.DampeningPhaseStart.Seconds())
	rsSlope := math.Log(conf.ProbabilityLowBound / (conf.RecentlySeenPhaseStart.Seconds() - conf.RecentlySeenPhaseEnd.Seconds()))
	ltSlope := 1.0 / (conf.LongTriedPhaseEnd.Seconds() - conf.LongTriedPhaseStart.Seconds())

	return &probabilisticPolicy{
		pLow:           conf.ProbabilityLowBound,
		rsPhaseStart:   int64(conf.RecentlySeenPhaseStart.Seconds()),
		rsPhaseEnd:     int64(conf.RecentlySeenPhaseEnd.Seconds()),
		ltPhaseStart:   int64(conf.LongTriedPhaseStart.Seconds()),
		ltPhaseEnd:     int64(conf.LongTriedPhaseEnd.Seconds()),
		dampPhaseStart: int64(conf.DampeningPhaseStart.Seconds()),
		dampPhaseEnd:   int64(conf.DampeningPhaseEnd.Seconds()),
		dampFactor:     conf.DampeningFactor,
		rsSlope:        rsSlope,
		ltSlope:        ltSlope,
		dampSlope:      dampSlope,
	}
}

var _ DialPolicy = (*probabilisticPolicy)(nil)

type probabilisticPolicy struct {
	pLow           float64
	rsSlope        float64
	ltSlope        float64
	dampSlope      float64
	rsPhaseStart   int64
	rsPhaseEnd     int64
	ltPhaseStart   int64
	ltPhaseEnd     int64
	dampPhaseStart int64
	dampPhaseEnd   int64
	dampFactor     float64
}

func (s probabilisticPolicy) Decide(info PeerActivity, at int64) bool {
	return rand.Float64() < s.confidence(info, at)
}

func (s probabilisticPolicy) InterruptFailed() bool {
	return true
}

func (s probabilisticPolicy) confidence(pa PeerActivity, at int64) float64 {
	return math.Min(s.recentlySeen(pa, at)+s.damp(pa)*s.longTried(pa, at), 1)
}

func (s probabilisticPolicy) recentlySeen(pa PeerActivity, at int64) float64 {
	var (
		notSeen = at - pa.LastSeenAt
		prob    float64
	)

	if pa.LastSeenAt <= 0 || notSeen < s.rsPhaseStart {
		prob = 1
	} else if notSeen > s.rsPhaseEnd {
		prob = s.pLow
	} else {
		prob = math.Exp(s.rsSlope * float64(s.rsPhaseStart-notSeen))
	}

	return prob
}

func (s probabilisticPolicy) longTried(pa PeerActivity, at int64) float64 {
	var (
		notTried = at - pa.LastTriedAt
		prob     float64
	)

	if pa.LastTriedAt <= 0 || notTried < s.ltPhaseStart {
		prob = 0
	} else if notTried > s.ltPhaseEnd {
		prob = 1
	} else {
		prob = s.ltSlope * float64(notTried-s.ltPhaseStart)
	}

	return prob
}

func (s probabilisticPolicy) damp(pa PeerActivity) float64 {
	var (
		span   = pa.LastTriedAt - pa.LastSeenAt
		factor float64
	)

	if pa.LastTriedAt <= 0 || pa.LastSeenAt <= 0 || span <= s.dampPhaseStart {
		factor = 1
	} else if span > s.dampPhaseEnd {
		factor = s.dampFactor
	} else {
		factor = s.dampFactor + s.dampSlope*float64(span-s.dampPhaseStart)
	}

	return factor
}

func init() {
	// required by metastore
	gob.Register(PeerActivity{})
}
