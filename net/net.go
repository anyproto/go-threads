// Package net implements the network layer for go-threads.
// Nodes exchange messages with each other via gRPC, and the format is defined under /pb.
package net

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	bs "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	gostream "github.com/libp2p/go-libp2p-gostream"
	ma "github.com/multiformats/go-multiaddr"
	sym "github.com/textileio/crypto/symmetric"
	"github.com/textileio/go-threads/broadcast"
	"github.com/textileio/go-threads/cbor"
	"github.com/textileio/go-threads/core/app"
	lstore "github.com/textileio/go-threads/core/logstore"
	core "github.com/textileio/go-threads/core/net"
	"github.com/textileio/go-threads/core/thread"
	sym "github.com/textileio/go-threads/crypto/symmetric"
	"github.com/textileio/go-threads/metrics"
	pb "github.com/textileio/go-threads/net/pb"
	"github.com/textileio/go-threads/net/queue"
	"github.com/textileio/go-threads/net/util"
	tu "github.com/textileio/go-threads/util"
	"google.golang.org/grpc"
)

var (
	log = logging.Logger("net")

	// MaxThreadsExchanged is the maximum number of threads for the single edge exchange.
	MaxThreadsExchanged = 10

	// ExchangeCompressionTimeout is the maximum duration of collecting threads for the exchange edges request.
	ExchangeCompressionTimeout = PullTimeout / 2

	// QueuePollInterval is the polling interval for the call queue.
	QueuePollInterval = time.Millisecond * 500

	// EventBusCapacity is the buffer size of local event bus listeners.
	EventBusCapacity = 1

	// notifyTimeout is the duration to wait for a subscriber to read a new record.
	notifyTimeout = time.Second * 5

	// tokenChallengeBytes is the byte length of token challenges.
	tokenChallengeBytes = 32

	// tokenChallengeTimeout is the duration of time given to an identity to complete a token challenge.
	tokenChallengeTimeout = time.Minute

	ErrSyncTrackingDisabled = errors.New("synchronization tracking disabled")
)

const (
	callPriorityLow  = 1
	callPriorityHigh = 3
)

var (
	_ util.SemaphoreKey = (*semaThreadUpdate)(nil)
)

// semaphore protecting thread info updates
type semaThreadUpdate thread.ID

type recordPutOriginKey struct{}

func (t semaThreadUpdate) Key() string {
	return "tu:" + string(t)
}

// net is an implementation of app.Net.
type net struct {
	conf Config

	format.DAGService
	host   host.Host
	bstore bs.Blockstore
	store  lstore.Logstore

	tStat     *threadStatusRegistry
	connTrack *connTracker

	rpc    *grpc.Server
	server *server
	bus    *broadcast.Broadcaster

	metrics               metrics.Metrics
	isPrivateReachability bool

	connectors map[thread.ID]*app.Connector
	connLock   sync.RWMutex

	semaphores      *util.SemaphorePool
	queueGetLogs    queue.CallQueue
	queueGetRecords queue.CallQueue

	ctx    context.Context
	cancel context.CancelFunc
}

// Config is used to specify thread instance options.
type Config struct {
	NetPullingLimit           uint
	NetPullingStartAfter      time.Duration
	NetPullingInitialInterval time.Duration
	NetPullingInterval        time.Duration
	NoNetPulling              bool
	NoExchangeEdgesMigration  bool
	PubSub                    bool
	Debug                     bool
	SyncBook     lstore.SyncBook
	SyncTracking bool
}

func (c Config) Validate() error {
	if c.NetPullingLimit <= 0 {
		return errors.New("NetPullingLimit must be greater than zero")
	}
	if c.NetPullingStartAfter <= 0 {
		return errors.New("NetPullingStartAfter must be greater than zero")
	}
	if c.NetPullingInitialInterval <= 0 {
		return errors.New("NetPullingInitialInterval must be greater than zero")
	}
	if c.NetPullingInterval <= 0 {
		return errors.New("NetPullingInterval must be greater than zero")
	}
	return nil
}

// NewNetwork creates an instance of net from the given host and thread store.
func NewNetwork(
	ctx context.Context,
	h host.Host,
	bstore bs.Blockstore,
	ds format.DAGService,
	ls lstore.Logstore,
	conf Config,
	serverOptions []grpc.ServerOption,
	dialOptions []grpc.DialOption,
) (app.Net, error) {
	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %v", err)
	}

	if err := tu.SetLogLevels(map[string]logging.LogLevel{
		"net":      tu.LevelFromDebugFlag(conf.Debug),
		"logstore": tu.LevelFromDebugFlag(conf.Debug),
	}); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	n := &net{
		conf:            conf,
		DAGService:      ds,
		host:            h,
		bstore:          bstore,
		store:           ls,
		rpc:             grpc.NewServer(serverOptions...),
		bus:             broadcast.NewBroadcaster(EventBusCapacity),
		connectors:      make(map[thread.ID]*app.Connector),
		ctx:             ctx,
		cancel:          cancel,
		semaphores:      util.NewSemaphorePool(1),
		queueGetLogs:    queue.NewFFQueue(ctx, QueuePollInterval, conf.NetPullingInterval),
		queueGetRecords: queue.NewFFQueue(ctx, QueuePollInterval, conf.NetPullingInterval),
	}

	go n.migrateHeadsIfNeeded(ctx, ls)

	n.server, err = newServer(n, dialOptions...)
	if err != nil {
		return nil, err
	}

	if conf.SyncTracking {
		n.connTrack = NewConnTracker(h.Network())
		if n.tStat, err = NewThreadStatusRegistry(conf.SyncBook, t.connTrack.Track); err != nil {
			return nil, fmt.Errorf("thread status registry init failed: %w", err)
		}
	}

	listener, err := gostream.Listen(h, thread.Protocol)
	if err != nil {
		return nil, err
	}
	go func() {
		pb.RegisterServiceServer(n.rpc, n.server)
		if err := n.rpc.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve error: %v", err)
		}
	}()

	n.setMetrics(ctx)
	go n.monitorReachability(ctx)
	go n.startPulling()
	return n, nil
}

func (n *net) setMetrics(ctx context.Context) {
	m, ok := ctx.Value(metrics.ContextKey{}).(metrics.Metrics)
	if !ok {
		n.metrics = &metrics.NoOpMetrics{}
		return
	}
	n.metrics = m
}

func (n *net) monitorReachability(ctx context.Context) {
	subReachability, _ := n.host.EventBus().Subscribe(new(event.EvtLocalReachabilityChanged))
	defer subReachability.Close()
	for {
		select {
		case ev, ok := <-subReachability.Out():
			if !ok {
				return
			}
			evt, ok := ev.(event.EvtLocalReachabilityChanged)
			if !ok {
				return
			}
			isPrivate := false
			if evt.Reachability == network.ReachabilityPrivate {
				isPrivate = true
			}
			n.connLock.Lock()
			n.isPrivateReachability = isPrivate
			n.connLock.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (n *net) countRecords(ctx context.Context, tid thread.ID, rid cid.Cid, offset cid.Cid) (int64, error) {
	var (
		cursor        = rid
		counter int64 = 0
	)
	sk, err := n.store.ServiceKey(tid)
	if err != nil {
		return 0, err
	}

	for !cursor.Equals(offset) {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		r, err := cbor.GetRecord(ctx, n, cursor, sk)
		cancel()
		if err != nil {
			log.With("thread id", tid.String()).
				With("record id", cursor.String).
				Error("failed to find record")
			return 0, err
		}
		cursor = r.PrevID()
		counter += 1
	}
	return counter, nil
}

func (n *net) migrateLog(ctx context.Context, tid thread.ID, lid peer.ID) error {
	heads, err := n.store.Heads(tid, lid)
	if err != nil {
		return err
	}
	if len(heads) == 0 {
		return nil
	}

	// our logs have only one head
	h := heads[0]

	// we have already migrated the log
	if h.Counter != thread.CounterUndef {
		return nil
	}

	isBroken := false
	counter, err := n.countRecords(ctx, tid, h.ID, cid.Undef)
	if err != nil {
		log.With("thread id", tid.String()).
			With("log id", lid.String()).
			With("head id", h.ID.String()).
			Error("failed to count records for thread, marking it as undefined")
		isBroken = true
	} else {
		log.With("thread id", tid.String()).
			With("log id", lid.String()).
			With("head id", h.ID.String()).
			With("counter", counter).
			Debug("counted records for thread")
	}

	ts := n.semaphores.Get(semaThreadUpdate(tid))
	ts.Acquire()
	defer ts.Release()

	if current, err := n.currentHead(tid, lid); err != nil {
		return fmt.Errorf("fetching head failed: %w", err)
	} else if !current.ID.Equals(h.ID) && !isBroken {
		// count additional records
		additionalCounter, err := n.countRecords(ctx, tid, current.ID, h.ID)

		// this should not happen hopefully :-)
		if err != nil {
			log.With("thread id", tid.String()).
				With("log id", lid.String()).
				With("head id", h.ID.String()).
				Error("failed to count additional records, returning error")
			return fmt.Errorf("counting additional records failed: %w", err)
		}
		counter += additionalCounter
		log.With("thread id", tid.String()).
			With("log id", lid.String()).
			With("head id", h.ID.String()).
			With("counter", counter).
			Debug("updated records counter for thread")
		h = current
	}

	err = n.store.SetHead(tid, lid, thread.Head{
		ID:      h.ID,
		Counter: counter,
	})
	if err != nil {
		return fmt.Errorf("setting head failed: %w", err)
	}
	return nil
}

func (n *net) migrateHeadsIfNeeded(ctx context.Context) {
	if n.conf.NoExchangeEdgesMigration {
		return
	}

	threadIds, err := n.store.Threads()
	if err != nil {
		log.Errorf("could not get threads from logstore: %v", err)
		return
	}

	log.Info("checking for heads migration")
	completed, err := n.store.MigrationCompleted(lstore.MigrationVersion1)
	if err != nil {
		log.Errorf("error getting migration state: %v", err)
		return
	}
	if completed {
		log.Info("heads already migrated")
		return
	}

	log.Info("starting migrating heads")

	shouldMarkMigrationCompleted := true
	for _, tid := range threadIds {
		tInfo, err := n.store.GetThread(tid)
		if err != nil {
			log.With("thread", tid.String()).
				Errorf("error getting thread: %v", err)
			shouldMarkMigrationCompleted = false
			continue
		}

		for _, l := range tInfo.Logs {
			err = n.migrateLog(ctx, tid, l.ID)
			if err != nil {
				log.With("thread", tid.String()).
					With("log", l.ID.String()).
					Errorf("error migrating log: %v", err)
				shouldMarkMigrationCompleted = false
			}
		}
	}

	if shouldMarkMigrationCompleted {
		err = n.store.SetMigrationCompleted(lstore.MigrationVersion1)
		if err != nil {
			log.Errorf("migration succeded but failed to save migration state: %v", err)
			return
		}
		log.Info("finished migrating heads")
		// this is a sanity check that we have correct counters in the end of migration
		// use only for testing
		// go n.recountHeads(ctx)
	} else {
		log.Error("there were errors in migration, will try to migrate errored logs next time")
	}
}

func (n *net) Close() (err error) {
	// Wait for all thread pulls to finish
	n.semaphores.Stop()

	// Close all pubsub topics
	if err := n.server.removeAllPubsubTopics(); err != nil {
		log.Errorf("closing pubsub topics: %v", err)
	}

	// Close peer connections and shutdown the server
	n.server.Lock()
	defer n.server.Unlock()
	for _, c := range n.server.conns {
		if err = c.Close(); err != nil {
			log.Errorf("error closing connection: %v", err)
		}
	}
	tu.StopGRPCServer(n.rpc)

	if n.connTrack != nil {
		n.connTrack.Close()
	}

	if n.tStat != nil {
		if err := n.tStat.Close(); err != nil {
			log.Errorf("error closing thread status registry: %v", err)
		}
	}

	var errs []error
	weakClose := func(name string, c interface{}) {
		if cl, ok := c.(io.Closer); ok {
			if err = cl.Close(); err != nil {
				errs = append(errs, fmt.Errorf("%s error: %v", name, err))
			}
		}
	}
	weakClose("DAGService", n.DAGService)
	weakClose("host", n.host)
	weakClose("threadstore", n.store)
	if len(errs) > 0 {
		return fmt.Errorf("failed while closing net; err(s): %q", errs)
	}

	n.bus.Discard()
	n.cancel()
	return nil
}

func (n *net) Host() host.Host {
	return n.host
}

func (n *net) Store() lstore.Logstore {
	return n.store
}

func (n *net) GetHostID(_ context.Context) (peer.ID, error) {
	return n.host.ID(), nil
}

func (n *net) GetToken(ctx context.Context, identity thread.Identity) (tok thread.Token, err error) {
	msg := make([]byte, tokenChallengeBytes)
	if _, err = rand.Read(msg); err != nil {
		return
	}
	sctx, cancel := context.WithTimeout(ctx, tokenChallengeTimeout)
	defer cancel()
	sig, err := identity.Sign(sctx, msg)
	if err != nil {
		return
	}
	key := identity.GetPublic()
	if ok, err := key.Verify(msg, sig); !ok || err != nil {
		return tok, fmt.Errorf("bad signature")
	}
	return thread.NewToken(n.getPrivKey(), key)
}

func (n *net) CreateThread(
	_ context.Context,
	id thread.ID,
	opts ...core.NewThreadOption,
) (info thread.Info, err error) {
	args := &core.NewThreadOptions{}
	for _, opt := range opts {
		opt(args)
	}
	// @todo: Check identity key against ACL.
	identity, err := n.Validate(id, args.Token, false)
	if err != nil {
		return
	}
	if identity != nil {
		log.Debugf("creating thread with identity: %s", identity)
	} else {
		identity = thread.NewLibp2pPubKey(n.getPrivKey().GetPublic())
	}

	if err = n.ensureUniqueLog(id, args.LogKey, identity); err != nil {
		return
	}

	info = thread.Info{
		ID:  id,
		Key: args.ThreadKey,
	}
	if !info.Key.Defined() {
		info.Key = thread.NewRandomKey()
	}
	if err = n.store.AddThread(info); err != nil {
		return
	}
	if !args.NoLog {
		if _, err = n.createLog(id, args.LogKey, identity); err != nil {
			return
		}
	}
	if err = n.server.addPubsubTopic(id); err != nil {
		return
	}

	return n.getThreadWithAddrs(id)
}

func (n *net) AddThread(
	ctx context.Context,
	addr ma.Multiaddr,
	opts ...core.NewThreadOption,
) (info thread.Info, err error) {
	args := &core.NewThreadOptions{}
	for _, opt := range opts {
		opt(args)
	}

	id, err := thread.FromAddr(addr)
	if err != nil {
		return
	}
	identity, err := n.Validate(id, args.Token, false)
	if err != nil {
		return
	}
	if identity != nil {
		log.Debugf("adding thread with identity: %s", identity)
	} else {
		identity = thread.NewLibp2pPubKey(n.getPrivKey().GetPublic())
	}

	if err = n.ensureUniqueLog(id, args.LogKey, identity); err != nil {
		return
	}

	threadComp, err := ma.NewComponent(thread.Name, id.String())
	if err != nil {
		return
	}
	peerAddr := addr.Decapsulate(threadComp)
	addri, err := peer.AddrInfoFromP2pAddr(peerAddr)
	if err != nil {
		return
	}

	// Check if we're trying to dial ourselves (regardless of addr)
	addFromSelf := addri.ID == n.host.ID()
	if addFromSelf {
		// Error if we don't have the thread locally
		if _, err = n.store.GetThread(id); errors.Is(err, lstore.ErrThreadNotFound) {
			err = fmt.Errorf("cannot retrieve thread from self: %v", err)
			return
		}
	}

	// Even if we already have the thread locally, we might still need to add a new log
	if err = n.store.AddThread(thread.Info{
		ID:  id,
		Key: args.ThreadKey,
	}); err != nil {
		return
	}
	if !args.NoLog && (args.ThreadKey.CanRead() || args.LogKey != nil) {
		if _, err = n.createLog(id, args.LogKey, identity); err != nil {
			return
		}
	}

	// Skip if trying to dial ourselves (already have the logs)
	if !addFromSelf {
		if err = n.Host().Connect(ctx, *addri); err != nil {
			return
		}

		if err = n.queueGetLogs.Call(addri.ID, id, func(ctx context.Context, p peer.ID, t thread.ID) error {
			if err := n.updateLogsFromPeer(ctx, p, t); err != nil {
				return err
			}
			return n.server.addPubsubTopic(id)
		}); err != nil {
			return
		}
	}
	return n.getThreadWithAddrs(id)
}

func (n *net) GetThread(_ context.Context, id thread.ID, opts ...core.ThreadOption) (info thread.Info, err error) {
	args := &core.ThreadOptions{}
	for _, opt := range opts {
		opt(args)
	}
	if _, err = n.Validate(id, args.Token, true); err != nil {
		return
	}
	return n.getThreadWithAddrs(id)
}

func (n *net) getThreadWithAddrs(id thread.ID) (info thread.Info, err error) {
	var tinfo thread.Info
	var peerID *ma.Component
	var threadID *ma.Component
	tinfo, err = n.store.GetThread(id)
	if err != nil {
		return
	}
	peerID, err = ma.NewComponent("p2p", n.host.ID().String())
	if err != nil {
		return
	}
	threadID, err = ma.NewComponent("thread", tinfo.ID.String())
	if err != nil {
		return
	}
	addrs := n.host.Addrs()
	res := make([]ma.Multiaddr, len(addrs))
	for i := range addrs {
		res[i] = addrs[i].Encapsulate(peerID).Encapsulate(threadID)
	}
	tinfo.Addrs = res
	return tinfo, nil
}

func (n *net) PullThread(ctx context.Context, id thread.ID, opts ...core.ThreadOption) error {
	args := &core.ThreadOptions{}
	for _, opt := range opts {
		opt(args)
	}
	if _, err := n.Validate(id, args.Token, true); err != nil {
		return err
	}
	return n.pullThread(ctx, id)
}

// pullThread for the new records. This method is thread-safe.
func (n *net) pullThread(ctx context.Context, tid thread.ID) error {
	offsets, peers, err := n.threadOffsets(tid)
	if err != nil {
		return err
	}

	// Pull from peers
	recs, err := n.server.getRecords(peers, tid, offsets, n.conf.NetPullingLimit)
	if err != nil {
		return err
	}

	for lid, rs := range recs {
		if err = n.putRecords(ctx, tid, lid, rs.records, rs.counter); err != nil {
			return err
		}
	}

	return nil
}

func (n *net) DeleteThread(ctx context.Context, id thread.ID, opts ...core.ThreadOption) error {
	args := &core.ThreadOptions{}
	for _, opt := range opts {
		opt(args)
	}
	if _, err := n.Validate(id, args.Token, false); err != nil {
		return err
	}
	if _, ok := n.getConnectorProtected(id, args.APIToken); !ok {
		return fmt.Errorf("cannot delete thread: %w", app.ErrThreadInUse)
	}

	log.Debugf("deleting thread %s...", id)
	ts := n.semaphores.Get(semaThreadUpdate(id))

	// Must block in case the thread is being pulled
	ts.Acquire()
	err := n.deleteThread(ctx, id)
	ts.Release()

	return err
}

// deleteThread cleans up all the persistent and in-memory bits of a thread. This includes:
// - Removing all record and event nodes.
// - Deleting all logstore keys, addresses, and heads.
// - Cancelling the pubsub subscription and topic.
// Local subscriptions will not be cancelled and will simply stop reporting.
// This method is internal and *not* thread-safe. It assumes we currently own the thread-lock.
func (n *net) deleteThread(ctx context.Context, id thread.ID) error {
	if err := n.server.removePubsubTopic(id); err != nil {
		return err
	}

	info, err := n.store.GetThread(id)
	if err != nil {
		return err
	}
	for _, lg := range info.Logs { // Walk logs, removing record and event nodes
		head := lg.Head.ID
		for head.Defined() {
			head, err = n.deleteRecord(ctx, head, info.Key.Service())
			if err != nil {
				return err
			}
		}
	}

	return n.store.DeleteThread(id) // Delete logstore keys, addresses, heads, and metadata
}

func (n *net) AddReplicator(
	ctx context.Context,
	id thread.ID,
	paddr ma.Multiaddr,
	opts ...core.ThreadOption,
) (pid peer.ID, err error) {
	args := &core.ThreadOptions{}
	for _, opt := range opts {
		opt(args)
	}
	if _, err = n.Validate(id, args.Token, true); err != nil {
		return
	}

	info, err := n.store.GetThread(id)
	if err != nil {
		return
	}

	// Extract peer portion
	p2p, err := paddr.ValueForProtocol(ma.P_P2P)
	if err != nil {
		return
	}
	pid, err = peer.Decode(p2p)
	if err != nil {
		return
	}

	// Update local addresses
	addr, err := ma.NewMultiaddr("/" + ma.ProtocolWithCode(ma.P_P2P).Name + "/" + p2p)
	if err != nil {
		return
	}

	containsAddr := func(l thread.LogInfo) bool {
		for _, a := range l.Addrs {
			if a.Equal(addr) {
				return true
			}
		}
		return false
	}

	var unreplicatedLogs []thread.LogInfo
	for _, lg := range info.Logs {
		if !containsAddr(lg) {
			lg.Addrs = append(lg.Addrs, addr)
			unreplicatedLogs = append(unreplicatedLogs, lg)
		}
	}

	// Check if we're dialing ourselves (regardless of addr)
	if pid != n.host.ID() {
		// If not, update peerstore address
		var dialable ma.Multiaddr
		dialable, err = getDialable(paddr)
		if err == nil {
			n.host.Peerstore().AddAddr(pid, dialable, pstore.PermanentAddrTTL)
		}

		// Send all unreplicated logs to the new replicator
		for _, l := range unreplicatedLogs {
			if err = n.server.pushLog(ctx, info.ID, l, pid, info.Key.Service(), nil); err != nil {
				return
			}
			if err = n.store.AddAddr(info.ID, l.ID, addr, pstore.PermanentAddrTTL); err != nil {
				log.Errorf("error adding log to the store: %s", err)
				return
			}
		}
	}

	// Send the updated log(s) to peers
	var addrs []ma.Multiaddr
	for _, l := range unreplicatedLogs {
		addrs = append(addrs, l.Addrs...)
	}
	peers, err := n.uniquePeers(addrs)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(pid peer.ID) {
			defer wg.Done()
			for _, lg := range unreplicatedLogs {
				if err = n.server.pushLog(ctx, info.ID, lg, pid, nil, nil); err != nil {
					log.Errorf("error pushing log %s to %s: %v", lg.ID, pid, err)
				}
			}
		}(p)
	}

	wg.Wait()
	return pid, nil
}

func (n *net) uniquePeers(addrs []ma.Multiaddr) ([]peer.ID, error) {
	var pm = make(map[peer.ID]struct{}, len(addrs))
	for _, addr := range addrs {
		pid, ok, err := n.callablePeer(addr)
		if err != nil {
			return nil, err
		} else if !ok {
			// skip calling itself
			continue
		}
		pm[pid] = struct{}{}
	}
	var ps = make([]peer.ID, 0, len(pm))
	for pid := range pm {
		ps = append(ps, pid)
	}
	return ps, nil
}

// callablePeer attempts to obtain external peer ID from the multiaddress.
func (n *net) callablePeer(addr ma.Multiaddr) (peer.ID, bool, error) {
	p, err := addr.ValueForProtocol(ma.P_P2P)
	if err != nil {
		return "", false, err
	}

	pid, err := peer.Decode(p)
	if err != nil {
		return "", false, err
	}

	if pid.String() == n.host.ID().String() {
		return pid, false, nil
	}

	return pid, true, nil
}

func getDialable(addr ma.Multiaddr) (ma.Multiaddr, error) {
	parts := strings.Split(addr.String(), "/"+ma.ProtocolWithCode(ma.P_P2P).Name)
	return ma.NewMultiaddr(parts[0])
}

func (n *net) CreateRecord(
	ctx context.Context,
	id thread.ID,
	body format.Node,
	opts ...core.ThreadOption,
) (tr core.ThreadRecord, err error) {
	args := &core.ThreadOptions{}
	for _, opt := range opts {
		opt(args)
	}
	identity, err := n.Validate(id, args.Token, false)
	if err != nil {
		return
	}
	if identity == nil {
		identity = thread.NewLibp2pPubKey(n.getPrivKey().GetPublic())
	}
	con, ok := n.getConnectorProtected(id, args.APIToken)
	if !ok {
		return nil, fmt.Errorf("cannot create record: %w", app.ErrThreadInUse)
	} else if con != nil {
		if err = con.ValidateNetRecordBody(ctx, body, identity); err != nil {
			return
		}
	}

	releaseIfNeeded := func() {}
	// check if we need to take semaphore before adding record
	// this can happen during background migration, because we can set an incorrect counter
	if completed, _ := n.store.MigrationCompleted(lstore.MigrationVersion1); !completed {
		ts := n.semaphores.Get(semaThreadUpdate(id))
		ts.Acquire()
		releaseIfNeeded = func() {
			ts.Release()
		}
	}

	lg, err := n.getOrCreateLog(id, identity, args.LogPrivateKey)
	if err != nil {
		return
	}
	r, err := n.newRecord(ctx, id, lg, body, identity)
	if err != nil {
		return
	}
	tr = NewRecord(r, id, lg.ID)
	counter := lg.Head.Counter + 1
	if lg.Head.IsFromBrokenLog() {
		counter = thread.CounterUndef
	}
	head := thread.Head{
		ID:      tr.Value().Cid(),
		Counter: counter,
	}
	if err = n.store.SetHead(id, lg.ID, head); err != nil {
		return
	}

	releaseIfNeeded()

	startTime := time.Now()
	log.Debugf("created record %s (thread=%s, log=%s)", tr.Value().Cid(), id, lg.ID)
	if err = n.bus.SendWithTimeout(tr, notifyTimeout); err != nil {
		return
	}
	busMs := time.Now().Sub(startTime).Milliseconds()
	if err = n.server.pushRecord(ctx, id, lg.ID, tr.Value(), counter); err != nil {
		return
	}
	pushMs := time.Now().Sub(startTime).Milliseconds() - busMs
	n.metrics.CreateRecord(int(busMs), int(pushMs))
	return tr, nil
}

func (n *net) AddRecord(
	ctx context.Context,
	id thread.ID,
	lid peer.ID,
	rec core.Record,
	opts ...core.ThreadOption,
) error {
	args := &core.ThreadOptions{}
	for _, opt := range opts {
		opt(args)
	}
	if _, err := n.Validate(id, args.Token, false); err != nil {
		return err
	}

	logpk, err := n.store.PubKey(id, lid)
	if err != nil {
		return err
	}
	if logpk == nil {
		return lstore.ErrLogNotFound
	}

	if knownRecord, err := n.isKnown(rec.Cid()); err != nil {
		return err
	} else if knownRecord {
		return nil
	}

	if err = rec.Verify(logpk); err != nil {
		return err
	}
	if err = n.putRecords(ctx, id, lid, []core.Record{rec}, thread.CounterUndef); err != nil {
		return err
	}
	return n.server.pushRecord(ctx, id, lid, rec, thread.CounterUndef)
}

func (n *net) GetRecord(
	ctx context.Context,
	id thread.ID,
	rid cid.Cid,
	opts ...core.ThreadOption,
) (core.Record, error) {
	args := &core.ThreadOptions{}
	for _, opt := range opts {
		opt(args)
	}
	if _, err := n.Validate(id, args.Token, true); err != nil {
		return nil, err
	}
	return n.getRecord(ctx, id, rid)
}

func (n *net) getRecord(ctx context.Context, id thread.ID, rid cid.Cid) (core.Record, error) {
	sk, err := n.store.ServiceKey(id)
	if err != nil {
		return nil, err
	}
	if sk == nil {
		return nil, fmt.Errorf("a service-key is required to get records")
	}
	return cbor.GetRecord(ctx, n, rid, sk)
}

// Record implements core.Record. The most basic component of a Log.
type Record struct {
	core.Record
	threadID thread.ID
	logID    peer.ID
}

// NewRecord returns a record with the given values.
func NewRecord(r core.Record, id thread.ID, lid peer.ID) core.ThreadRecord {
	return &Record{Record: r, threadID: id, logID: lid}
}

func (r *Record) Value() core.Record {
	return r
}

func (r *Record) ThreadID() thread.ID {
	return r.threadID
}

func (r *Record) LogID() peer.ID {
	return r.logID
}

func (n *net) Subscribe(ctx context.Context, opts ...core.SubOption) (<-chan core.ThreadRecord, error) {
	args := &core.SubOptions{}
	for _, opt := range opts {
		opt(args)
	}

	filter := make(map[thread.ID]struct{})
	for _, id := range args.ThreadIDs {
		if err := id.Validate(); err != nil {
			return nil, err
		}
		if id.Defined() {
			if _, err := n.Validate(id, args.Token, true); err != nil {
				return nil, err
			}
			filter[id] = struct{}{}
		}
	}
	return n.subscribe(ctx, filter)
}

func (n *net) subscribe(ctx context.Context, filter map[thread.ID]struct{}) (<-chan core.ThreadRecord, error) {
	channel := make(chan core.ThreadRecord)
	go func() {
		defer close(channel)
		listener := n.bus.Listen()
		defer listener.Discard()
		for {
			select {
			case <-ctx.Done():
				return
			case i, ok := <-listener.Channel():
				if !ok {
					return
				}
				if rec, ok := i.(*Record); ok {
					if len(filter) > 0 {
						if _, ok := filter[rec.threadID]; ok {
							channel <- rec
						}
					} else {
						channel <- rec
					}
				} else {
					log.Warn("listener received a non-record value")
				}
			}
		}
	}()
	return channel, nil
}

func (n *net) ConnectApp(a app.App, id thread.ID) (*app.Connector, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	info, err := n.getThreadWithAddrs(id)
	if err != nil {
		return nil, fmt.Errorf("error getting thread %s: %v", id, err)
	}
	con, err := app.NewConnector(a, n, info)
	if err != nil {
		return nil, fmt.Errorf("error making connector %s: %v", id, err)
	}
	n.addConnector(id, con)
	return con, nil
}

// @todo: Handle thread ACL checks against ID and readOnly.
func (n *net) Validate(id thread.ID, token thread.Token, readOnly bool) (thread.PubKey, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return token.Validate(n.getPrivKey())
}

func (n *net) addConnector(id thread.ID, conn *app.Connector) {
	n.connLock.Lock()
	n.connectors[id] = conn
	n.connLock.Unlock()
}

func (n *net) getConnector(id thread.ID) (*app.Connector, bool) {
	n.connLock.RLock()
	defer n.connLock.RUnlock()

	conn, exist := n.connectors[id]
	return conn, exist
}

// getConnectorProtected returns the connector tied to the thread if it exists
// and whether or not the token is valid.
func (n *net) getConnectorProtected(id thread.ID, token core.Token) (*app.Connector, bool) {
	c, exist := n.getConnector(id)
	if !exist {
		return nil, true // thread is not owned by a connector
	}
	if !token.Equal(c.Token()) {
		return nil, false
	}
	return c, true
}

// PutRecord adds an existing record. This method is thread-safe.
func (n *net) PutRecord(ctx context.Context, id thread.ID, lid peer.ID, rec core.Record, counter int64) error {
	if err := id.Validate(); err != nil {
		return err
	}
	return n.putRecords(ctx, id, lid, []core.Record{rec}, counter)
}

// putRecords adds existing records. This method is thread-safe.
func (n *net) putRecords(ctx context.Context, tid thread.ID, lid peer.ID, recs []core.Record, counter int64) error {
	chain, head, err := n.loadRecords(ctx, tid, lid, recs, counter)
	if err != nil {
		return fmt.Errorf("loading records failed: %w", err)
	} else if len(chain) == 0 {
		return nil
	}

	ts := n.semaphores.Get(semaThreadUpdate(tid))
	ts.Acquire()
	defer ts.Release()

	// check the head again, as some other process could change the log concurrently
	if current, err := n.currentHead(tid, lid); err != nil {
		return fmt.Errorf("fetching head failed: %w", err)
	} else if !current.ID.Equals(head.ID) {
		// fast-forward the chain up to the updated head
		var headReached bool
		head = current
		for i := 0; i < len(chain); i++ {
			if chain[i].Value().Cid().Equals(current.ID) {
				chain = chain[i+1:]
				headReached = true
				break
			}
		}
		if !headReached {
			// entire chain already processed
			return nil
		}
	}

	var (
		connector, appConnected = n.getConnector(tid)
		identity                = &thread.Libp2pPubKey{}
		readKey                 *sym.Key
		validate                bool
		// setting new counters for heads
		updatedCounter = head.Counter
	)

	if appConnected {
		var err error
		if readKey, err = n.store.ReadKey(tid); err != nil {
			return err
		} else if readKey != nil {
			validate = true
		}
	}

	for _, record := range chain {
		if validate {
			block, err := record.Value().GetBlock(ctx, n)
			if err != nil {
				return err
			}

			event, ok := block.(*cbor.Event)
			if !ok {
				event, err = cbor.EventFromNode(block)
				if err != nil {
					return fmt.Errorf("invalid event: %w", err)
				}
			}

			dbody, err := event.GetBody(ctx, n, readKey)
			if err != nil {
				return err
			}

			if err = identity.UnmarshalBinary(record.Value().PubKey()); err != nil {
				return err
			}

			if err = connector.ValidateNetRecordBody(ctx, dbody, identity); err != nil {
				userErr := err

				// remove stored internal blocks
				header, err := event.GetHeader(ctx, n, nil)
				if err != nil {
					return err
				}

				body, err := event.GetBody(ctx, n, nil)
				if err != nil {
					return err
				}

				if err := n.RemoveMany(ctx, []cid.Cid{event.Cid(), header.Cid(), body.Cid()}); err != nil {
					return fmt.Errorf("removing invalid blocks: %w", err)
				}

				return userErr
			}
		}

		if !head.IsFromBrokenLog() {
			updatedCounter++
		}

		if err := n.store.SetHead(
			tid,
			lid,
			thread.Head{
				ID:      record.Value().Cid(),
				Counter: updatedCounter,
			}); err != nil {
			return fmt.Errorf("setting log head failed: %w", err)
		}

		if appConnected {
			if err := connector.HandleNetRecord(ctx, record); err != nil {
				// Future improvement notes.
				// If record handling fails there are two options available:
				// 1. Just interrupt and return error (current behaviour). Log head remains moved and some events
				//    from the record possibly won't reach reducers/listeners or even get dispatched.
				// 2. Rollback log head to the previous record. In this case record handling will be retried until
				//    success, but reducers must guarantee its idempotence and there is a chance of getting stuck
				//    with bad event and not making any progress at all.
				return fmt.Errorf("handling record failed: %w", err)
			}
		}

		// add record envelope to the blockstore, indicating it was successfully processed
		if err := n.Add(ctx, record.Value()); err != nil {
			return fmt.Errorf("adding record to the blockstore failed: %w", err)
		}

		// Generally broadcasting should not block for too long, i.e. we have to run it
		// under the semaphore to ensure consistent order seen by the listeners. Record
		// bursts could be overcome by adjusting listener buffers (EventBusCapacity).
		if err = n.bus.SendWithTimeout(record, notifyTimeout); err != nil {
			return err
		}
	}

	return nil
}

// Load, validate and cache all records in log between last provided and currentHead.
func (n *net) loadRecords(
	ctx context.Context,
	tid thread.ID,
	lid peer.ID,
	recs []core.Record,
	counter int64,
) ([]core.ThreadRecord, thread.Head, error) {
	if len(recs) == 0 {
		return nil, thread.HeadUndef, errors.New("cannot load empty record chain")
	}
	head, err := n.currentHead(tid, lid)
	if err != nil {
		return nil, thread.HeadUndef, err
	}

	// check if the last record was already loaded and processed
	var last = recs[len(recs)-1]
	// if we don't have the counter (but have some recs) then we were communicating with old version peer
	if counter == thread.CounterUndef || head.IsFromBrokenLog() {
		if exist, err := n.isKnown(last.Cid()); err != nil {
			return nil, thread.HeadUndef, err
		} else if exist || !last.Cid().Defined() {
			return nil, thread.HeadUndef, nil
		}
	} else if counter <= head.Counter {
		return nil, head, nil
	}

	var (
		chain    = make([]core.Record, 0, len(recs))
		complete bool
	)

	for i := len(recs) - 1; i >= 0; i-- {
		// adding metrics here because this is the only loop when we go through all the records
		// excluding the ones we get from bitswap
		recordType, ok := ctx.Value(recordPutOriginKey{}).(metrics.RecordType)
		if ok {
			n.metrics.AcceptRecord(recordType, n.isPrivateReachability)
		}

		var next = recs[i]
		if c := next.Cid(); !c.Defined() || c.Equals(head.ID) {
			complete = true
			break
		}
		chain = append(chain, next)
	}

	if !complete {
		// bridge the gap between the last provided record and current head
		var c = chain[len(chain)-1].PrevID()
		for c.Defined() {
			if c.Equals(head.ID) {
				break
			}

			r, err := n.getRecord(ctx, tid, c)
			if err != nil {
				return nil, head, err
			}

			chain = append(chain, r)
			c = r.PrevID()
		}
	}

	if len(chain) == 0 {
		// fast path
		return nil, head, nil
	}

	tRecords := make([]core.ThreadRecord, 0, len(chain))
	for i := len(chain) - 1; i >= 0; i-- {
		var r = chain[i]
		block, err := r.GetBlock(ctx, n)
		if err != nil {
			return nil, head, err
		}

		event, ok := block.(*cbor.Event)
		if !ok {
			event, err = cbor.EventFromNode(block)
			if err != nil {
				return nil, head, fmt.Errorf("invalid event: %w", err)
			}
		}

		header, err := event.GetHeader(ctx, n, nil)
		if err != nil {
			return nil, head, err
		}

		body, err := event.GetBody(ctx, n, nil)
		if err != nil {
			return nil, head, err
		}

		// store internal blocks locally, record envelope will be added by the caller after successful processing
		if err = n.AddMany(ctx, []format.Node{event, header, body}); err != nil {
			return nil, head, err
		}

		tRecords = append(tRecords, NewRecord(r, tid, lid))
	}

	return tRecords, head, nil
}

func (n *net) isKnown(rec cid.Cid) (bool, error) {
	return n.bstore.Has(rec)
}

func (n *net) currentHead(tid thread.ID, lid peer.ID) (thread.Head, error) {
	var head thread.Head
	heads, err := n.store.Heads(tid, lid)
	if err != nil {
		return head, err
	}

	if len(heads) > 0 {
		head = heads[0]
	} else {
		head = thread.HeadUndef
	}

	return head, nil
}

// newRecord creates a new record with the given body as a new event body.
func (n *net) newRecord(
	ctx context.Context,
	id thread.ID,
	lg thread.LogInfo,
	body format.Node,
	pk thread.PubKey,
) (core.Record, error) {
	if lg.PrivKey == nil {
		return nil, fmt.Errorf("a private-key is required to create records")
	}
	sk, err := n.store.ServiceKey(id)
	if err != nil {
		return nil, err
	}
	if sk == nil {
		return nil, fmt.Errorf("a service-key is required to create records")
	}
	rk, err := n.store.ReadKey(id)
	if err != nil {
		return nil, err
	}
	if rk == nil {
		return nil, fmt.Errorf("a read-key is required to create records")
	}
	event, err := cbor.CreateEvent(ctx, n, body, rk)
	if err != nil {
		return nil, err
	}
	return cbor.CreateRecord(ctx, n, cbor.CreateRecordConfig{
		Block:      event,
		Prev:       lg.Head.ID,
		Key:        lg.PrivKey,
		PubKey:     pk,
		ServiceKey: sk,
	})
}

// getPrivKey returns the host's private key.
func (n *net) getPrivKey() crypto.PrivKey {
	return n.host.Peerstore().PrivKey(n.host.ID())
}

// getLocalRecords returns local records from the given thread that are ahead of
// offset but not farther than limit.
// It is possible to reach limit before offset, meaning that the caller
// will be responsible for the remaining traversal.
func (n *net) getLocalRecords(
	ctx context.Context,
	id thread.ID,
	lid peer.ID,
	offset cid.Cid,
	limit int,
	counter int64,
) ([]core.Record, error) {
	lg, err := n.store.GetLog(id, lid)
	if err != nil {
		return nil, err
	}
	// reverting to old logic if the new one is not supported
	if counter == thread.CounterUndef && offset != cid.Undef || lg.Head.IsFromBrokenLog() {
		if offset.Defined() {
			// ensure that we know about requested offset
			if knownRecord, err := n.isKnown(offset); err != nil {
				return nil, err
			} else if !knownRecord {
				return nil, nil
			}
		}
		// if we have less or equal records
	} else if lg.Head.Counter <= counter {
		return []core.Record{}, nil
	}
	sk, err := n.store.ServiceKey(id)
	if err != nil {
		return nil, err
	}
	if sk == nil {
		return nil, fmt.Errorf("a service-key is required to get records")
	}

	var (
		cursor = lg.Head.ID
		recs   []core.Record
	)

	for len(recs) < limit {
		if !cursor.Defined() || cursor.String() == offset.String() {
			break
		}
		r, err := cbor.GetRecord(ctx, n, cursor, sk) // Important invariant: heads are always in blockstore
		if err != nil {
			// return records fetched so far
			return recs, err
		}
		recs = append([]core.Record{r}, recs...)
		cursor = r.PrevID()
	}

	return recs, nil
}

// deleteRecord remove a record from the dag service.
func (n *net) deleteRecord(ctx context.Context, rid cid.Cid, sk *sym.Key) (prev cid.Cid, err error) {
	rec, err := cbor.GetRecord(ctx, n, rid, sk)
	if err != nil {
		return
	}
	if err = cbor.RemoveRecord(ctx, n, rec); err != nil {
		return
	}
	event, err := cbor.EventFromRecord(ctx, n, rec)
	if err != nil {
		return
	}
	if err = cbor.RemoveEvent(ctx, n, event); err != nil {
		return
	}
	return rec.PrevID(), nil
}

// startPulling periodically pulls on all threads.
func (n *net) startPulling() {
	if n.conf.NoNetPulling {
		return
	}
	select {
	case <-time.After(n.conf.NetPullingStartAfter):
	case <-n.ctx.Done():
		return
	}

	// set pull cycle interval into initial value,
	// it will be redefined on the next iteration
	var interval = n.conf.NetPullingInitialInterval

	// group threads by peers and exchange edges efficiently
	var compressor = queue.NewThreadPacker(n.ctx, MaxThreadsExchanged, ExchangeCompressionTimeout)
	go n.startExchange(compressor)

PullCycle:
	for {
		ts, err := n.store.Threads()
		if err != nil {
			log.Errorf("error listing threads: %s", err)
			return
		}
		log.Infof("pulling %d threads", len(ts))

		if len(ts) == 0 {
			// if there are no threads served, just wait and retry
			select {
			case <-time.After(interval):
				interval = n.conf.NetPullingInterval
				continue PullCycle
			case <-n.ctx.Done():
				return
			}
		}

		var (
			period = interval / time.Duration(len(ts))
			ticker = time.NewTicker(period)
			idx    = 0
		)

		for {
			select {
			case <-ticker.C:
				var tid = ts[idx]
				if _, peers, err := n.threadOffsets(tid); err != nil {
					log.Errorf("error getting thread info %s: %s", tid, err)
					return
				} else {
					for _, pid := range peers {
						compressor.Add(pid, tid)
					}
				}

				idx++
				if idx >= len(ts) {
					ticker.Stop()
					interval = n.conf.NetPullingInterval
					continue PullCycle
				}

			case <-n.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}
}

func (n *net) startExchange(compressor queue.ThreadPacker) {
	for pack := range compressor.Run() {
		go func(p queue.ThreadPack) {
			if err := n.server.exchangeEdges(n.ctx, p.Peer, p.Threads); err != nil {
				log.Debugf("exchangeEdges with %s failed: %v", p.Peer, err)
			}
		}(pack)
	}
}

// createLog creates a new log with the given peer as host.
func (n *net) createLog(id thread.ID, key crypto.Key, identity thread.PubKey) (info thread.LogInfo, err error) {
	var ok bool
	if key == nil {
		info.PrivKey, info.PubKey, err = crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return
		}
	} else if info.PrivKey, ok = key.(crypto.PrivKey); ok {
		info.PubKey = info.PrivKey.GetPublic()
	} else if info.PubKey, ok = key.(crypto.PubKey); !ok {
		return info, fmt.Errorf("invalid log-key")
	}
	info.ID, err = peer.IDFromPublicKey(info.PubKey)
	if err != nil {
		return
	}
	addr, err := ma.NewMultiaddr("/" + ma.ProtocolWithCode(ma.P_P2P).Name + "/" + n.host.ID().String())
	if err != nil {
		return
	}
	info.Addrs = []ma.Multiaddr{addr}
	// If we're creating the log, we're 'managing' it
	info.Managed = true

	// Add to thread
	if err = n.store.AddLog(id, info); err != nil {
		return info, err
	}
	lidb, err := info.ID.MarshalBinary()
	if err != nil {
		return info, err
	}
	if err = n.store.PutBytes(id, identity.String(), lidb); err != nil {
		return info, err
	}
	return info, nil
}

// getOrCreateLog returns a log for identity under the given thread.
// If no log exists, a new one is created.
func (n *net) getOrCreateLog(id thread.ID, identity thread.PubKey, key crypto.Key) (info thread.LogInfo, err error) {
	if identity == nil {
		identity = thread.NewLibp2pPubKey(n.getPrivKey().GetPublic())
	}
	lidb, err := n.store.GetBytes(id, identity.String())
	if err != nil {
		return info, err
	}
	// Check if we have an old-style "own" (unindexed) log
	if lidb == nil && identity.Equals(thread.NewLibp2pPubKey(n.getPrivKey().GetPublic())) {
		thrd, err := n.store.GetThread(id)
		if err != nil {
			return info, err
		}
		ownLog := thrd.GetFirstPrivKeyLog()
		if ownLog != nil {
			return *ownLog, nil
		}
	} else if lidb != nil {
		lid, err := peer.IDFromBytes(*lidb)
		if err != nil {
			return info, err
		}
		return n.store.GetLog(id, lid)
	}
	return n.createLog(id, key, identity)
}

// createExternalLogsIfNotExist creates an external logs if doesn't exists. The created
// logs will have cid.Undef as the current head. Is thread-safe.
func (n *net) createExternalLogsIfNotExist(
	tid thread.ID,
	lis []thread.LogInfo,
) error {
	ts := n.semaphores.Get(semaThreadUpdate(tid))
	ts.Acquire()
	defer ts.Release()

	for _, li := range lis {
		if currHeads, err := n.Store().Heads(tid, li.ID); err != nil {
			return err
		} else if len(currHeads) == 0 {
			li.Head = thread.HeadUndef
			if err = n.Store().AddLog(tid, li); err != nil {
				return err
			}
		} else {
			// update log addresses
			if err = n.Store().AddAddrs(tid, li.ID, li.Addrs, pstore.PermanentAddrTTL); err != nil {
				return err
			}
		}
	}
	return nil
}

// ensureUniqueLog returns a non-nil error if a log with key already exists,
// or if a log for identity already exists for the given thread.
func (n *net) ensureUniqueLog(id thread.ID, key crypto.Key, identity thread.PubKey) (err error) {
	thrd, err := n.store.GetThread(id)
	if errors.Is(err, lstore.ErrThreadNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var lid peer.ID
	if key != nil {
		switch key.(type) {
		case crypto.PubKey:
			lid, err = peer.IDFromPublicKey(key.(crypto.PubKey))
			if err != nil {
				return err
			}
		case crypto.PrivKey:
			lid, err = peer.IDFromPrivateKey(key.(crypto.PrivKey))
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid log key")
		}
	} else {
		lidb, err := n.store.GetBytes(id, identity.String())
		if err != nil {
			return err
		}
		if lidb == nil {
			// Check if we have an old-style "own" (unindexed) log
			if identity.Equals(thread.NewLibp2pPubKey(n.getPrivKey().GetPublic())) {
				li := thrd.GetFirstPrivKeyLog()
				if li != nil && li.PrivKey != nil {
					return lstore.ErrThreadExists
				}
			}
			return nil
		}
		lid, err = peer.IDFromBytes(*lidb)
		if err != nil {
			return err
		}
	}
	_, err = n.store.GetLog(id, lid)
	if err == nil {
		return lstore.ErrLogExists
	}
	if !errors.Is(err, lstore.ErrLogNotFound) {
		return err
	}
	return nil
}

// updateRecordsFromPeer fetches new logs & records from the peer and adds them in the local peer store.
func (n *net) updateRecordsFromPeer(ctx context.Context, pid peer.ID, tid thread.ID) error {
	log.With("thread", tid.String()).With("peer", pid.String()).Debugf("update records from peer")
	offsets, _, err := n.threadOffsets(tid)
	if err != nil {
		return fmt.Errorf("getting offsets for thread %s failed: %w", tid, err)
	}
	req, sk, err := n.server.buildGetRecordsRequest(tid, offsets, n.conf.NetPullingLimit)
	if err != nil {
		return fmt.Errorf("building GetRecords request for thread %s failed: %w", tid, err)
	}
	recs, err := n.server.getRecordsFromPeer(ctx, tid, pid, req, sk)
	if err != nil {
		return fmt.Errorf("getting records for thread %s from %s failed: %w", tid, pid, err)
	}
	ctx = context.WithValue(ctx, recordPutOriginKey{}, metrics.RecordTypeGet)
	for lid, rs := range recs {
		if err = n.putRecords(ctx, tid, lid, rs.records, rs.counter); err != nil {
			return fmt.Errorf("putting records from log %s (thread %s) failed: %w", lid, tid, err)
		}
	}
	return nil
}

// updateLogsFromPeer gets new logs information from the peer and adds it in the local peer store.
func (n *net) updateLogsFromPeer(ctx context.Context, pid peer.ID, tid thread.ID) error {
	log.With("thread", tid.String()).With("peer", pid.String()).Debugf("update logs from peer")
	lgs, err := n.server.getLogs(ctx, tid, pid)
	if err != nil {
		return err
	}
	return n.createExternalLogsIfNotExist(tid, lgs)
}

// returns offsets and involved peers for all known thread's logs.
func (n *net) threadOffsets(tid thread.ID) (map[peer.ID]thread.Head, []peer.ID, error) {
	info, err := n.store.GetThread(tid)
	if err != nil {
		return nil, nil, err
	}
	var (
		offsets = make(map[peer.ID]thread.Head, len(info.Logs))
		addrs   []ma.Multiaddr
	)
	for _, lg := range info.Logs {
		var has bool
		if lg.Head.ID.Defined() {
			has, err = n.isKnown(lg.Head.ID)
			if err != nil {
				return nil, nil, err
			}
		}
		if has {
			offsets[lg.ID] = lg.Head
		} else {
			offsets[lg.ID] = thread.HeadUndef
		}
		addrs = append(addrs, lg.Addrs...)
	}
	peers, err := n.uniquePeers(addrs)
	if err != nil {
		return nil, nil, err
	}
	return offsets, peers, nil
}

func (n *net) Connectivity() (<-chan core.ConnectionStatus, error) {
	if n.connTrack == nil {
		return nil, ErrSyncTrackingDisabled
	}
	return n.connTrack.Notify(), nil
}

func (n *net) Status(tid thread.ID, pid peer.ID) (core.SyncStatus, error) {
	if n.tStat == nil {
		return core.SyncStatus{}, ErrSyncTrackingDisabled
	}
	return n.tStat.Status(tid, pid), nil
}

func (n *net) View(tid thread.ID) (map[peer.ID]core.SyncStatus, error) {
	if n.tStat == nil {
		return nil, ErrSyncTrackingDisabled
	}
	return n.tStat.View(tid), nil
}

func (n *net) PeerSummary(pid peer.ID) (core.SyncSummary, error) {
	if n.tStat == nil {
		return core.SyncSummary{}, ErrSyncTrackingDisabled
	}
	return n.tStat.PeerSummary(pid), nil
}

func (n *net) ThreadSummary(tid thread.ID) (core.SyncSummary, error) {
	if n.tStat == nil {
		return core.SyncSummary{}, ErrSyncTrackingDisabled
	}
	return n.tStat.ThreadSummary(tid), nil
}
