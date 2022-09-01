package net

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/status"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/textileio/go-threads/cbor"
	lstore "github.com/textileio/go-threads/core/logstore"
	core "github.com/textileio/go-threads/core/net"
	"github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/logstore/lstoreds"
	"github.com/textileio/go-threads/metrics"
	pb "github.com/textileio/go-threads/net/pb"
	"github.com/textileio/go-threads/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcpeer "google.golang.org/grpc/peer"
)

var (
	errNoAddrsEdge = errors.New("no addresses to compute edge")
	errNoHeadsEdge = errors.New("no heads to compute edge")
)

// server implements the net gRPC server.
type server struct {
	sync.Mutex
	net   *net
	ps    *PubSub
	opts  []grpc.DialOption
	conns map[peer.ID]*grpc.ClientConn
}

// newServer creates a new network server.
func newServer(n *net, enablePubSub bool, opts ...grpc.DialOption) (*server, error) {
	var (
		s = &server{
			net:   n,
			conns: make(map[peer.ID]*grpc.ClientConn),
		}

		defaultOpts = []grpc.DialOption{
			s.getLibp2pDialer(),
			grpc.WithInsecure(),
		}
	)

	s.opts = append(defaultOpts, opts...)

	if enablePubSub {
		ps, err := pubsub.NewGossipSub(
			n.ctx,
			n.host,
			pubsub.WithMessageSigning(false),
			pubsub.WithStrictSignatureVerification(false))
		if err != nil {
			return nil, err
		}
		s.ps = NewPubSub(n.ctx, n.host.ID(), ps, s.pubsubHandler)

		ts, err := n.store.Threads()
		if err != nil {
			return nil, err
		}
		for _, id := range ts {
			if err := s.ps.Add(id); err != nil {
				return nil, err
			}
		}
	}

	return s, nil
}

// pubsubHandler receives records over pubsub.
func (s *server) pubsubHandler(ctx context.Context, req *pb.PushRecordRequest) {
	ctx = context.WithValue(ctx, recordPutOriginKey{}, metrics.RecordTypePubsub)
	if _, err := s.PushRecord(ctx, req); err != nil {
		// This error will be "log not found" if the record sent over pubsub
		// beat the log, which has to be sent directly via the normal API.
		// In this case, the record will arrive directly after the log via
		// the normal API.
		log.With("thread", req.Body.ThreadID.ID.String()).Errorf("error handling pubsub record: %s", err)
	}
}

// GetLogs receives a get logs request.
func (s *server) GetLogs(ctx context.Context, req *pb.GetLogsRequest) (*pb.GetLogsReply, error) {
	pid, err := peerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	log.With("thread", req.Body.ThreadID.ID.String()).With("peer", pid.String()).Debugf("received get logs request from peer")

	pblgs := &pb.GetLogsReply{}
	if err := s.checkServiceKey(req.Body.ThreadID.ID, req.Body.ServiceKey); err != nil {
		return pblgs, err
	}

	info, err := s.net.store.GetThread(req.Body.ThreadID.ID) // Safe since putRecords will change head when fully-available
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	pblgs.Logs = make([]*pb.Log, len(info.Logs))
	for i, l := range info.Logs {
		pblgs.Logs[i] = logToProto(l)
	}

	log.With("thread", req.Body.ThreadID.ID.String()).With("peer", pid.String()).Debugf("sending %d logs to peer", len(info.Logs))

	return pblgs, nil
}

// PushLog receives a push log request.
// @todo: Don't overwrite info from non-owners
func (s *server) PushLog(ctx context.Context, req *pb.PushLogRequest) (*pb.PushLogReply, error) {
	pid, err := peerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	log.With("thread", req.Body.ThreadID.ID.String()).With("peer", pid.String()).With("log", req.Body.Log.ID.ID.String()).With("counter", req.Body.Log.Counter).Debugf("received push log request from peer")

	// Pick up missing keys
	info, err := s.net.store.GetThread(req.Body.ThreadID.ID)
	if err != nil && !errors.Is(err, lstore.ErrThreadNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !info.Key.Defined() {
		if req.Body.ServiceKey != nil && req.Body.ServiceKey.Key != nil {
			if err = s.net.store.AddServiceKey(req.Body.ThreadID.ID, req.Body.ServiceKey.Key); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		} else {
			return nil, status.Error(codes.NotFound, lstore.ErrThreadNotFound.Error())
		}
	} else if !info.Key.CanRead() {
		if req.Body.ReadKey != nil && req.Body.ReadKey.Key != nil {
			if err = s.net.store.AddReadKey(req.Body.ThreadID.ID, req.Body.ReadKey.Key); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		}
	}

	lg := logFromProto(req.Body.Log)
	if err = s.net.createExternalLogsIfNotExist(req.Body.ThreadID.ID, []thread.LogInfo{lg}); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if s.net.queueGetRecords.Schedule(pid, req.Body.ThreadID.ID, callPriorityLow, s.net.updateRecordsFromPeer) {
		log.With("thread", req.Body.ThreadID.ID.String()).With("peer", pid.String()).Debugf("record update for thread from peer scheduled")
	}
	return &pb.PushLogReply{}, nil
}

// GetRecords receives a get records request.
func (s *server) GetRecords(ctx context.Context, req *pb.GetRecordsRequest) (*pb.GetRecordsReply, error) {
	pid, err := peerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	log.With("thread", req.Body.ThreadID.ID.String()).With("request", fmt.Sprintf("%p", req)).With("peer", pid.String()).With("logs", len(req.Body.Logs)).Debugf("received get records request from peer")

	var pbrecs = &pb.GetRecordsReply{}
	if err := s.checkServiceKey(req.Body.ThreadID.ID, req.Body.ServiceKey); err != nil {
		return pbrecs, err
	}

	// fast check if requested offsets are equal with thread heads
	started := time.Now()
	if changed, err := s.headsChanged(req); err != nil {
		return nil, err
	} else if !changed {
		return pbrecs, nil
	}
	s.net.metrics.GetRecordsHeadsChangedDuration(time.Since(started))

	reqd := make(map[peer.ID]*pb.GetRecordsRequest_Body_LogEntry)
	for _, l := range req.Body.Logs {
		reqd[l.LogID.ID] = l
	}
	started = time.Now()
	info, err := s.net.store.GetThread(req.Body.ThreadID.ID)
	s.net.metrics.GetRecordsGetThreadDuration(time.Since(started))

	if err != nil {
		return nil, err
	} else if len(info.Logs) == 0 {
		return pbrecs, nil
	}
	pbrecs.Logs = make([]*pb.GetRecordsReply_LogEntry, 0, len(info.Logs))

	logRecordLimit := MaxPullLimit
	if !s.net.useMaxPullLimit {
		logRecordLimit = MaxPullLimit / len(info.Logs)
	}

	var (
		failures     int32
		totalRecords int
		mx           sync.Mutex
		wg           sync.WaitGroup
	)

	for _, lg := range info.Logs {
		var (
			offset  cid.Cid
			limit   int
			counter int64
			pblg    = logToProto(lg)
		)
		if opts, ok := reqd[lg.ID]; ok {
			offset = opts.Offset.Cid
			counter = opts.Counter
			limit = minInt(int(opts.Limit), logRecordLimit)
		} else {
			offset = cid.Undef
			limit = logRecordLimit
			counter = thread.CounterUndef
		}

		wg.Add(1)
		go func(tid thread.ID, lid peer.ID, off cid.Cid, head cid.Cid, lim int) {
			defer wg.Done()
			// if we don't have records in the log then skipping it
			if pblg.Head.Cid == cid.Undef {
				return
			}

			startedLog := time.Now()
			recs, err := s.net.getLocalRecords(ctx, tid, lid, off, lim, counter)
			if err != nil {
				atomic.AddInt32(&failures, 1)
				log.With("thread", tid.String()).With("request", fmt.Sprintf("%p", req)).With("log", lid.String()).Errorf("getting local records failed: %v", err)
			}

			var prs = make([]*pb.Log_Record, 0, len(recs))
			for _, r := range recs {
				pr, err := cbor.RecordToProto(ctx, s.net, r)
				if err != nil {
					atomic.AddInt32(&failures, 1)
					log.Errorf("constructing proto-record %s (thread %s, log %s): %v", r.Cid(), tid, lid, err)
					break
				}
				prs = append(prs, pr)
			}

			if len(prs) == 0 {
				// do not include logs with no records in reply
				return
			}

			mx.Lock()
			pbrecs.Logs = append(pbrecs.Logs, &pb.GetRecordsReply_LogEntry{
				LogID:   &pb.ProtoPeerID{ID: lid},
				Records: prs,
				Log:     pblg,
			})
			s.net.metrics.NumberOfRecordsSentForLog(len(prs))
			totalRecords += len(prs)
			mx.Unlock()

			log.With("thread", tid.String()).With("peer", pid.String()).With("request", fmt.Sprintf("%p", req)).With("spent", time.Since(startedLog).Milliseconds()).With("offset", off.String()).With("counter", counter).With("log", lid.String()).With("head", head).With("records", len(recs)).Debugf("sending records in log to remote peer")
		}(req.Body.ThreadID.ID, lg.ID, offset, lg.Head.ID, limit)
	}

	wg.Wait()

	if totalRecords > 100 {
		log.With("thread", req.Body.ThreadID.ID.String()).With("spent", time.Since(started).Milliseconds()).With("request", fmt.Sprintf("%p", req)).With("total", totalRecords).With("peer", pid.String()).With("logs", len(req.Body.Logs)).Debugf("get records request from peer finished")
	}

	s.net.metrics.NumberOfRecordsSentTotal(totalRecords)
	if registry := s.net.tStat; registry != nil && failures == 0 {
		// if requester was able to receive our latest records its
		// equivalent to successful push in the reverse direction
		registry.Apply(pid, req.Body.ThreadID.ID, threadStatusUploadDone)
	}
	return pbrecs, nil
}

// PushRecord receives a push record request.
func (s *server) PushRecord(ctx context.Context, req *pb.PushRecordRequest) (*pb.PushRecordReply, error) {
	pid, err := peerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// A log is required to accept new records
	logpk, err := s.net.store.PubKey(req.Body.ThreadID.ID, req.Body.LogID.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if logpk == nil {
		return nil, status.Error(codes.NotFound, "log not found")
	}

	key, err := s.net.store.ServiceKey(req.Body.ThreadID.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	rec, err := cbor.RecordFromProto(req.Body.Record, key)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err = rec.Verify(logpk); err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	counter := req.Body.Counter
	// checking req.Counter for backwards compatibility
	if counter == thread.CounterUndef {
		counter = req.Counter
	}
	log.With("peer", pid.String()).
		With("rid", rec.String()).
		With("rcnt", counter).
		With("log", req.Body.LogID.String()).
		With("thread", req.Body.ThreadID.String()).
		Debugf("received push record request from peer")

	var final = threadStatusDownloadFailed
	if registry := s.net.tStat; registry != nil {
		// receiving and successful processing records is equivalent to pulling from the peer
		registry.Apply(pid, req.Body.ThreadID.ID, threadStatusDownloadStarted)
		defer func() { registry.Apply(pid, req.Body.ThreadID.ID, final) }()
	}

	// we may still have no counter if the version is very old :-)
	if counter != thread.CounterUndef {
		h, err := s.net.currentHead(req.Body.ThreadID.ID, req.Body.LogID.ID)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		// already have everything
		if h.Counter >= counter {
			final = threadStatusDownloadDone
			return &pb.PushRecordReply{
				Payload: &pb.PushRecordReply_PushRecordPayload{MissingCounter: thread.CounterUndef},
			}, nil
		}
		if h.Counter+1 != counter {
			if req.Body.Counter != thread.CounterUndef {
				// we have a client using new format (it has counter in body) and thus it has extended protocol
				return &pb.PushRecordReply{
					Payload: &pb.PushRecordReply_PushRecordPayload{MissingCounter: h.Counter + 1},
				}, nil
			} else {
				// backwards compatibility logic
				s.net.queueGetRecords.Schedule(req.Body.LogID.ID, req.Body.ThreadID.ID, callPriorityHigh, s.net.updateRecordsFromPeer)
				errString := fmt.Sprintf("can't push record because counters are different, have %d, need %d", h.Counter, req.Counter-1)
				log.With("peer", pid.String()).
					With("log", req.Body.LogID.String()).
					With("thread", req.Body.ThreadID.String()).Errorf(errString)
				return nil, status.Error(codes.Internal, errString)
			}
		}
	}

	// if it is not a pubsub then it is a push
	if ctx.Value(recordPutOriginKey{}) == nil {
		ctx = context.WithValue(ctx, recordPutOriginKey{}, metrics.RecordTypePush)
	}
	if err = s.net.PutRecord(ctx, req.Body.ThreadID.ID, req.Body.LogID.ID, rec, counter); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	final = threadStatusDownloadDone

	return &pb.PushRecordReply{
		Payload: &pb.PushRecordReply_PushRecordPayload{MissingCounter: thread.CounterUndef},
	}, nil
}

func (s *server) PushRecords(ctx context.Context, req *pb.PushRecordsRequest) (*pb.PushRecordsReply, error) {
	pid, err := peerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// A log is required to accept new records
	logpk, err := s.net.store.PubKey(req.Body.ThreadID.ID, req.Body.LogID.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if logpk == nil {
		return nil, status.Error(codes.NotFound, "log not found")
	}

	key, err := s.net.store.ServiceKey(req.Body.ThreadID.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var records []core.Record
	for _, r := range req.Body.Records {
		rec, err := cbor.RecordFromProto(r, key)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if err = rec.Verify(logpk); err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
		records = append(records, rec)
	}
	var final = threadStatusDownloadFailed
	if registry := s.net.tStat; registry != nil {
		// receiving and successful processing records is equivalent to pulling from the peer
		registry.Apply(pid, req.Body.ThreadID.ID, threadStatusDownloadStarted)
		defer func() { registry.Apply(pid, req.Body.ThreadID.ID, final) }()
	}

	log.With("peer", pid.String()).
		With("totalrecords", len(records)).
		With("rcnt", req.Body.Counter).
		With("log", req.Body.LogID.String()).
		With("thread", req.Body.ThreadID.String()).
		Debugf("received push records request from peer")

	h, err := s.net.currentHead(req.Body.ThreadID.ID, req.Body.LogID.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// already have everything
	if h.Counter >= req.Body.Counter+int64(len(records))-1 {
		final = threadStatusDownloadDone
		return &pb.PushRecordsReply{
			Payload: &pb.PushRecordsReply_PushRecordsPayload{MissingCounter: thread.CounterUndef},
		}, nil
	}
	if h.Counter+1 != req.Body.Counter {
		return &pb.PushRecordsReply{
			Payload: &pb.PushRecordsReply_PushRecordsPayload{MissingCounter: h.Counter + 1},
		}, nil
	}

	// if it is not a pubsub then it is a push
	if ctx.Value(recordPutOriginKey{}) == nil {
		ctx = context.WithValue(ctx, recordPutOriginKey{}, metrics.RecordTypePush)
	}
	if err = s.net.putRecords(ctx, req.Body.ThreadID.ID, req.Body.LogID.ID, records, req.Body.Counter); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	final = threadStatusDownloadDone

	return &pb.PushRecordsReply{
		Payload: &pb.PushRecordsReply_PushRecordsPayload{MissingCounter: thread.CounterUndef},
	}, nil
}

// ExchangeEdges receives an exchange edges request.
func (s *server) ExchangeEdges(ctx context.Context, req *pb.ExchangeEdgesRequest) (*pb.ExchangeEdgesReply, error) {
	pid, err := peerIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	log.With("peer", pid.String()).Debugf("received exchange edges request from peer")
	started := time.Now()

	var reply pb.ExchangeEdgesReply
	for _, entry := range req.Body.Threads {
		var tid = entry.ThreadID.ID
		switch addrsEdgeLocal, headsEdgeLocal, err := s.localEdges(tid); err {
		case errNoAddrsEdge, errNoHeadsEdge, nil:
			var (
				addrsEdgeRemote = entry.AddressEdge
				headsEdgeRemote = entry.HeadsEdge
			)

			// need to get new logs only if we have non empty addresses on remote and the hashes are different
			if addrsEdgeRemote != lstoreds.EmptyEdgeValue && addrsEdgeLocal != addrsEdgeRemote {
				prt := callPriorityLow
				updateLogs := s.net.updateLogsFromPeer
				// if we don't have the thread locally
				if addrsEdgeLocal == lstoreds.EmptyEdgeValue {
					prt = callPriorityHigh // we have to add thread in pubsub, not just update its logs
					updateLogs = func(ctx context.Context, p peer.ID, t thread.ID) error {
						if err := s.net.updateLogsFromPeer(ctx, p, t); err != nil {
							return err
						}
						if s.net.server.ps != nil {
							return s.net.server.ps.Add(t)
						}
						return nil
					}
				}
				if s.net.queueGetLogs.Schedule(pid, tid, prt, updateLogs) {
					log.With("peer", pid.String()).With("thread", tid.String()).Debugf("log information update for thread %s from %s scheduled", tid, pid)
				}
			}

			// need to get new records only if we have non empty heads on remote and the hashes are different
			if headsEdgeRemote != lstoreds.EmptyEdgeValue {
				if headsEdgeLocal != headsEdgeRemote {
					updateRecordsFromPeer := func(ctx context.Context, p peer.ID, t thread.ID) error {
						s.net.metrics.UpdateRecordsDelayAfterExchangeEdges(time.Since(started))
						return s.net.updateRecordsFromPeer(ctx, p, t)
					}
					if s.net.queueGetRecords.Schedule(pid, tid, callPriorityLow, updateRecordsFromPeer) {
						log.With("peer", pid.String()).With("thread", tid.String()).Debugf("record update for thread %s from %s scheduled", tid, pid)
					}
				} else if registry := s.net.tStat; registry != nil {
					// equal heads could be interpreted as successful upload/download
					registry.Apply(pid, tid, threadStatusDownloadDone)
					registry.Apply(pid, tid, threadStatusUploadDone)
				}
			}

			// setting "exists" for backwards compatibility with older versions
			// to get exactly same behaviour as was before
			exists := true
			if addrsEdgeLocal == lstoreds.EmptyEdgeValue || headsEdgeLocal == lstoreds.EmptyEdgeValue {
				exists = false
			}

			reply.Edges = append(reply.Edges, &pb.ExchangeEdgesReply_ThreadEdges{
				ThreadID:    &pb.ProtoThreadID{ID: tid},
				Exists:      exists,
				AddressEdge: addrsEdgeLocal,
				HeadsEdge:   headsEdgeLocal,
			})

		default:
			return nil, fmt.Errorf("getting edges for %s: %w", tid, err)
		}
	}

	return &reply, nil
}

// checkServiceKey compares a key with the one stored under thread.
func (s *server) checkServiceKey(id thread.ID, k *pb.ProtoKey) error {
	if k == nil || k.Key == nil {
		return status.Error(codes.Unauthenticated, "a service-key is required to get logs")
	}
	sk, err := s.net.store.ServiceKey(id)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if sk == nil {
		return status.Error(codes.NotFound, lstore.ErrThreadNotFound.Error())
	}
	if !bytes.Equal(k.Key.Bytes(), sk.Bytes()) {
		return status.Error(codes.Unauthenticated, "invalid service-key")
	}
	return nil
}

// headsChanged determines if thread heads are different from the requested offsets.
func (s *server) headsChanged(req *pb.GetRecordsRequest) (bool, error) {
	var reqHeads = make([]util.LogHead, len(req.Body.Logs))
	for i, l := range req.Body.GetLogs() {
		reqHeads[i] = util.LogHead{
			Head: thread.Head{
				ID:      l.Offset.Cid,
				Counter: l.Counter,
			},
			LogID: l.LogID.ID,
		}
	}
	var currEdge, err = s.net.store.HeadsEdge(req.Body.ThreadID.ID)
	switch {
	case err == nil:
		return util.ComputeHeadsEdge(reqHeads) != currEdge, nil
	case errors.Is(err, lstore.ErrThreadNotFound):
		// no local heads, but there could be missing logs info in reply
		return true, nil
	default:
		return false, err
	}
}

// localEdges returns values of local addresses/heads edges for the thread.
func (s *server) localEdges(tid thread.ID) (addrsEdge, headsEdge uint64, err error) {
	headsEdge = lstoreds.EmptyEdgeValue
	addrsEdge, err = s.net.store.AddrsEdge(tid)
	if err != nil {
		if errors.Is(err, lstore.ErrThreadNotFound) {
			err = errNoAddrsEdge
		} else {
			err = fmt.Errorf("address edge: %w", err)
		}
		return
	}
	headsEdge, err = s.net.store.HeadsEdge(tid)
	if err != nil {
		if errors.Is(err, lstore.ErrThreadNotFound) {
			err = errNoHeadsEdge
		} else {
			err = fmt.Errorf("heads edge: %w", err)
		}
	}
	return
}

// peerIDFromContext returns peer ID from the GRPC context
func peerIDFromContext(ctx context.Context) (peer.ID, error) {
	ctxPeer, ok := grpcpeer.FromContext(ctx)
	if !ok {
		return "", errors.New("unable to identify stream peer")
	}
	pid, err := peer.Decode(ctxPeer.Addr.String())
	if err != nil {
		return "", fmt.Errorf("parsing stream peer id: %v", err)
	}
	return pid, nil
}

// logToProto returns a proto log from a thread log.
func logToProto(l thread.LogInfo) *pb.Log {
	return &pb.Log{
		ID:      &pb.ProtoPeerID{ID: l.ID},
		PubKey:  &pb.ProtoPubKey{PubKey: l.PubKey},
		Addrs:   addrsToProto(l.Addrs),
		Head:    &pb.ProtoCid{Cid: l.Head.ID},
		Counter: l.Head.Counter,
	}
}

// logFromProto returns a thread log from a proto log.
func logFromProto(l *pb.Log) thread.LogInfo {
	return thread.LogInfo{
		ID:     l.ID.ID,
		PubKey: l.PubKey.PubKey,
		Addrs:  addrsFromProto(l.Addrs),
		Head: thread.Head{
			ID:      l.Head.Cid,
			Counter: l.Counter,
		},
	}
}

func addrsToProto(mas []ma.Multiaddr) []pb.ProtoAddr {
	pas := make([]pb.ProtoAddr, len(mas))
	for i, a := range mas {
		pas[i] = pb.ProtoAddr{Multiaddr: a}
	}
	return pas
}

func addrsFromProto(pa []pb.ProtoAddr) []ma.Multiaddr {
	mas := make([]ma.Multiaddr, len(pa))
	for i, a := range pa {
		mas[i] = a.Multiaddr
	}
	return mas
}

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}
