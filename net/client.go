package net

import (
	"context"
	"errors"
	"fmt"
	nnet "net"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	gostream "github.com/libp2p/go-libp2p-gostream"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/textileio/go-threads/cbor"
	core "github.com/textileio/go-threads/core/net"
	"github.com/textileio/go-threads/core/thread"
	sym "github.com/textileio/go-threads/crypto/symmetric"
	"github.com/textileio/go-threads/logstore/lstoreds"
	pb "github.com/textileio/go-threads/net/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
)

var (
	// DialTimeout is the max time duration to wait when dialing a peer.
	DialTimeout = time.Second * 10
	PushTimeout = time.Second * 10
	PullTimeout = time.Second * 10
)

// getLogs in a thread.
func (s *server) getLogs(ctx context.Context, id thread.ID, pid peer.ID) ([]thread.LogInfo, error) {
	sk, err := s.net.store.ServiceKey(id)
	if err != nil {
		return nil, err
	}
	if sk == nil {
		return nil, fmt.Errorf("a service-key is required to request logs")
	}

	body := &pb.GetLogsRequest_Body{
		ThreadID:   &pb.ProtoThreadID{ID: id},
		ServiceKey: &pb.ProtoKey{Key: sk},
	}
	// TODO: remove signing when enough users update
	sig, key, err := s.signRequestBody(body)
	if err != nil {
		return nil, err
	}
	req := &pb.GetLogsRequest{
		Header: &pb.Header{
			PubKey:    &pb.ProtoPubKey{PubKey: key},
			Signature: sig,
		},
		Body: body,
	}

	log.With("thread", id.String()).With("peer", pid.String()).Debugf("getting %s logs from %s...", id, pid)

	client, err := s.dial(pid)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, PullTimeout)
	defer cancel()
	reply, err := client.GetLogs(cctx, req)
	if err != nil {
		log.Warnf("get logs from %s failed: %s", pid, err)
		return nil, err
	}

	log.With("thread", id.String()).With("peer", pid.String()).Debugf("received %d logs", len(reply.Logs))

	lgs := make([]thread.LogInfo, len(reply.Logs))
	for i, l := range reply.Logs {
		lgs[i] = logFromProto(l)
	}

	return lgs, nil
}

// pushLog to a peer.
func (s *server) pushLog(ctx context.Context, id thread.ID, lg thread.LogInfo, pid peer.ID, sk *sym.Key, rk *sym.Key) error {
	body := &pb.PushLogRequest_Body{
		ThreadID: &pb.ProtoThreadID{ID: id},
		Log:      logToProto(lg),
	}
	if sk != nil {
		body.ServiceKey = &pb.ProtoKey{Key: sk}
	}
	if rk != nil {
		body.ReadKey = &pb.ProtoKey{Key: rk}
	}

	counter := body.Log.Counter
	body.Log.Counter = 0 // to omit counter
	// TODO: remove signing and counter magic when enough users update
	sig, key, err := s.signRequestBody(body)
	body.Log.Counter = counter

	if err != nil {
		return err
	}
	lreq := &pb.PushLogRequest{
		Header: &pb.Header{
			PubKey:    &pb.ProtoPubKey{PubKey: key},
			Signature: sig,
		},
		Body: body,
	}

	log.With("thread", id.String()).With("peer", pid.String()).Debugf("pushing log to peer...")

	client, err := s.dial(pid)
	if err != nil {
		return fmt.Errorf("dial %s failed: %w", pid, err)
	}
	cctx, cancel := context.WithTimeout(ctx, PushTimeout)
	defer cancel()
	_, err = client.PushLog(cctx, lreq)
	if err != nil {
		return fmt.Errorf("push log to %s failed: %w", pid, err)
	}
	return err
}

// getRecords from specified peers.
func (s *server) getRecords(
	peers []peer.ID,
	tid thread.ID,
	offsets map[peer.ID]thread.Head,
	limit int,
) (map[peer.ID]peerRecords, error) {
	req, sk, err := s.buildGetRecordsRequest(tid, offsets, limit)
	if err != nil {
		return nil, err
	}

	var (
		rc = newRecordCollector()
		wg sync.WaitGroup
	)

	// Pull from every peer
	for _, p := range peers {
		wg.Add(1)

		go withErrLog(p, func(pid peer.ID) error {
			defer wg.Done()

			return s.net.queueGetRecords.Call(pid, tid, func(ctx context.Context, pid peer.ID, tid thread.ID) error {
				recs, err := s.getRecordsFromPeer(ctx, tid, pid, req, sk)
				if err != nil {
					return err
				}
				for lid, rs := range recs {
					rc.UpdateHeadCounter(lid, rs.counter)
					for _, rec := range rs.records {
						rc.Store(lid, rec)
					}
				}
				return nil
			})
		})
	}
	wg.Wait()

	return rc.List()
}

func (s *server) buildGetRecordsRequest(
	tid thread.ID,
	offsets map[peer.ID]thread.Head,
	limit int,
) (req *pb.GetRecordsRequest, serviceKey *sym.Key, err error) {
	serviceKey, err = s.net.store.ServiceKey(tid)
	if err != nil {
		err = fmt.Errorf("obtaining service key: %w", err)
		return
	} else if serviceKey == nil {
		err = errors.New("a service-key is required to request records")
		return
	}

	var pblgs = make([]*pb.GetRecordsRequest_Body_LogEntry, 0, len(offsets))
	var counters = make([]int64, 0, len(offsets))
	for lid, offset := range offsets {
		pblgs = append(pblgs, &pb.GetRecordsRequest_Body_LogEntry{
			LogID:   &pb.ProtoPeerID{ID: lid},
			Offset:  &pb.ProtoCid{Cid: offset.ID},
			Limit:   int32(limit),
			Counter: 0,
		})
		counters = append(counters, offset.Counter)
	}

	body := &pb.GetRecordsRequest_Body{
		ThreadID:   &pb.ProtoThreadID{ID: tid},
		ServiceKey: &pb.ProtoKey{Key: serviceKey},
		Logs:       pblgs,
	}
	// TODO: remove signing and counter magic when enough users update
	sig, key, err := s.signRequestBody(body)
	if err != nil {
		err = fmt.Errorf("signing GetRecords request: %w", err)
		return
	}
	for i := range counters {
		body.Logs[i].Counter = counters[i]
	}

	req = &pb.GetRecordsRequest{
		Header: &pb.Header{
			PubKey:    &pb.ProtoPubKey{PubKey: key},
			Signature: sig,
		},
		Body: body,
	}
	return
}

type peerRecords struct {
	records []core.Record
	counter int64
}

// Send GetRecords request to a certain peer.
func (s *server) getRecordsFromPeer(
	ctx context.Context,
	tid thread.ID,
	pid peer.ID,
	req *pb.GetRecordsRequest,
	serviceKey *sym.Key,
) (map[peer.ID]peerRecords, error) {
	log.With("thread", tid.String()).With("peer", pid.String()).Debugf("getting records from peer...")

	var final = threadStatusDownloadFailed
	if registry := s.net.tStat; registry != nil {
		registry.Apply(pid, tid, threadStatusDownloadStarted)
		defer func() { registry.Apply(pid, tid, final) }()
	}

	client, err := s.dial(pid)
	if err != nil {
		return nil, fmt.Errorf("dial %s failed: %w", pid, err)
	}

	recs := make(map[peer.ID]peerRecords)
	cctx, cancel := context.WithTimeout(ctx, PullTimeout)
	defer cancel()
	reply, err := client.GetRecords(cctx, req)
	if err != nil {
		log.Warnf("get records from %s failed: %s", pid, err)
		return recs, nil
	}

	for _, l := range reply.Logs {
		var logID = l.LogID.ID
		log.With("thread", tid.String()).With("peer", pid.String()).With("log", logID.String()).Debugf("received %d records in log from peer", len(l.Records))

		if l.Log != nil && len(l.Log.Addrs) > 0 {
			if err = s.net.store.AddAddrs(tid, logID, addrsFromProto(l.Log.Addrs), pstore.PermanentAddrTTL); err != nil {
				return nil, err
			}
		}

		pk, err := s.net.store.PubKey(tid, logID)
		if err != nil {
			return nil, err
		}

		if pk == nil {
			if l.Log == nil || l.Log.PubKey == nil {
				// cannot verify received records
				continue
			}
			if err := s.net.store.AddPubKey(tid, logID, l.Log.PubKey); err != nil {
				return nil, err
			}
			pk = l.Log.PubKey
		}
		var records []core.Record
		for _, r := range l.Records {
			rec, err := cbor.RecordFromProto(r, serviceKey)
			if err != nil {
				return nil, err
			}
			if err = rec.Verify(pk); err != nil {
				return nil, err
			}
			records = append(records, rec)
		}
		counter := thread.CounterUndef
		if l.Log != nil {
			counter = l.Log.Counter
		}
		recs[logID] = peerRecords{
			records: records,
			counter: counter,
		}
	}

	final = threadStatusDownloadDone
	return recs, nil
}

// pushRecord to log addresses and thread topic.
func (s *server) pushRecord(ctx context.Context, tid thread.ID, lid peer.ID, rec core.Record, counter int64) error {
	// Collect known writers
	addrs := make([]ma.Multiaddr, 0)
	info, err := s.net.store.GetThread(tid)
	if err != nil {
		return err
	}
	for _, l := range info.Logs {
		addrs = append(addrs, l.Addrs...)
	}
	peers, err := s.net.uniquePeers(addrs)
	if err != nil {
		return err
	}

	pbrec, err := cbor.RecordToProto(ctx, s.net, rec)
	if err != nil {
		return err
	}
	body := &pb.PushRecordRequest_Body{
		ThreadID: &pb.ProtoThreadID{ID: tid},
		LogID:    &pb.ProtoPeerID{ID: lid},
		Record:   pbrec,
		Counter:  counter,
	}
	// TODO: remove signing when enough users update
	sig, key, err := s.signRequestBody(body)
	if err != nil {
		return err
	}
	req := &pb.PushRecordRequest{
		Header: &pb.Header{
			PubKey:    &pb.ProtoPubKey{PubKey: key},
			Signature: sig,
		},
		Body:    body,
		Counter: counter,
	}

	// Push to each address
	for _, p := range peers {
		// we use special sync queue to make sure that push records come in correct order
		s.net.queuePushRecords.PushBack(p, tid, func(ctx context.Context, queuePeer peer.ID, _ thread.ID) error {
			if err := s.pushRecordToPeer(req, queuePeer, tid, lid); err != nil {
				return fmt.Errorf("pushing record to peer failed: %w", err)
			}
			return nil
		})
	}

	// Finally, publish to the thread's topic
	if s.ps != nil {
		if err = s.ps.Publish(ctx, tid, req); err != nil {
			log.With("thread", tid.String()).Errorf("error publishing record: %s", err)
		}
	}

	return nil
}

func (s *server) pushRecordsToPeer(
	req *pb.PushRecordsRequest,
	pid peer.ID,
	tid thread.ID) error {
	var final = threadStatusUploadFailed
	if registry := s.net.tStat; registry != nil {
		registry.Apply(pid, tid, threadStatusUploadStarted)
		defer func() { registry.Apply(pid, tid, final) }()
	}

	client, err := s.dial(pid)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	rctx, cancel := context.WithTimeout(context.Background(), PushTimeout)
	defer cancel()

	_, err = client.PushRecords(rctx, req)
	return err
}

func (s *server) enqueuePushRecordsRequestToPeer(
	ctx context.Context,
	pid peer.ID,
	tid thread.ID,
	lid peer.ID,
	minCounter int64,
) error {
	recs, err := s.net.getLocalRecordsAfterCounter(ctx, tid, lid, minCounter)
	if err != nil {
		return err
	}
	var prs = make([]*pb.Log_Record, 0, len(recs))
	for _, r := range recs {
		pr, err := cbor.RecordToProto(ctx, s.net, r)
		if err != nil {
			log.Errorf("constructing proto-record %s (thread %s, log %s): %v", r.Cid(), tid, lid, err)
			break
		}
		prs = append(prs, pr)
	}

	replaceReq := &pb.PushRecordsRequest{
		Body: &pb.PushRecordsRequest_Body{
			ThreadID: &pb.ProtoThreadID{ID: tid},
			LogID:    &pb.ProtoPeerID{ID: lid},
			Records:  prs,
			Counter:  minCounter + 1,
		},
	}
	s.net.queuePushRecords.ReplaceQueue(pid, tid, func(ctx context.Context, _ peer.ID, _ thread.ID) error {
		if err := s.pushRecordsToPeer(replaceReq, pid, tid); err != nil {
			return fmt.Errorf("pushing records to peer failed: %w", err)
		}
		return nil
	})
	return nil
}

func (s *server) pushRecordToPeer(
	req *pb.PushRecordRequest,
	pid peer.ID,
	tid thread.ID,
	lid peer.ID,
) error {
	var final = threadStatusUploadFailed
	if registry := s.net.tStat; registry != nil {
		registry.Apply(pid, tid, threadStatusUploadStarted)
		defer func() { registry.Apply(pid, tid, final) }()
	}

	client, err := s.dial(pid)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	rctx, cancel := context.WithTimeout(context.Background(), PushTimeout)
	defer cancel()
	res, err := client.PushRecord(rctx, req)
	if err == nil {
		if res.GetPayload().GetMissingCounter() != thread.CounterUndef {
			enqCtx, cancel := context.WithTimeout(context.Background(), PushTimeout)
			defer cancel()
			return s.enqueuePushRecordsRequestToPeer(enqCtx, pid, tid, lid, res.Payload.MissingCounter-1)
		} else {
			final = threadStatusUploadDone
			return nil
		}
	}

	switch status.Convert(err).Code() {
	case codes.Unavailable:
		log.With("peer", pid.String()).With("thread", tid.String()).Debugf("peer unavailable, skip pushing the record")
		return nil

	case codes.NotFound:
		// send the missing log
		log.With("peer", pid.String()).With("thread", tid.String()).Warnf("push record to remote peer failed, not found. Resend the log and try again...")
		lctx, cancel := context.WithTimeout(s.net.ctx, PushTimeout)
		defer cancel()
		lg, err := s.net.store.GetLog(tid, lid)
		if err != nil {
			return fmt.Errorf("getting log information: %w", err)
		}
		body := &pb.PushLogRequest_Body{
			ThreadID: &pb.ProtoThreadID{ID: tid},
			Log:      logToProto(lg),
		}

		// TODO: remove signing and counter magic when enough users update
		counter := body.Log.Counter
		body.Log.Counter = 0
		sig, key, err := s.signRequestBody(body)
		body.Log.Counter = counter

		if err != nil {
			return fmt.Errorf("signing PushLog request: %w", err)
		}
		lreq := &pb.PushLogRequest{
			Header: &pb.Header{
				PubKey:    &pb.ProtoPubKey{PubKey: key},
				Signature: sig,
			},
			Body: body,
		}
		if _, err = client.PushLog(lctx, lreq); err != nil {
			return fmt.Errorf("pushing missing log: %w", err)
		}

		enqCtx, cancel := context.WithTimeout(context.Background(), PushTimeout)
		defer cancel()
		err = s.enqueuePushRecordsRequestToPeer(enqCtx, pid, tid, lid, thread.CounterUndef)
		if err != nil {
			return err
		}

		final = threadStatusUploadDone

		return nil

	default:
		return err
	}
}

// exchangeEdges of specified threads with a peer.
func (s *server) exchangeEdges(ctx context.Context, pid peer.ID, tids []thread.ID) error {
	var tidsStr []string
	for _, tid := range tids {
		tidsStr = append(tidsStr, tid.String())
	}
	log.With("peer", pid.String()).Debugf("exchanging edges of %d threads: %v", len(tids), tidsStr)
	var body = &pb.ExchangeEdgesRequest_Body{}

	// fill local edges
	for _, tid := range tids {
		switch addrsEdge, headsEdge, err := s.localEdges(tid); err {
		// we have lstoreds.EmptyEdgeValue for headsEdge and addrsEdge if we get errors below
		case errNoAddrsEdge, errNoHeadsEdge, nil:
			body.Threads = append(body.Threads, &pb.ExchangeEdgesRequest_Body_ThreadEntry{
				ThreadID:    &pb.ProtoThreadID{ID: tid},
				HeadsEdge:   headsEdge,
				AddressEdge: addrsEdge,
			})
		default:
			log.With("thread", tid.String()).Errorf("getting local edges failed: %v", err)
		}
	}
	if len(body.Threads) == 0 {
		return nil
	}

	// TODO: remove signing when enough users update
	sig, key, err := s.signRequestBody(body)
	if err != nil {
		return fmt.Errorf("signing request body: %w", err)
	}
	req := &pb.ExchangeEdgesRequest{
		Header: &pb.Header{
			PubKey:    &pb.ProtoPubKey{PubKey: key},
			Signature: sig,
		},
		Body: body,
	}

	// send request
	client, err := s.dial(pid)
	if err != nil {
		return fmt.Errorf("dial %s failed: %w", pid, err)
	}
	cctx, cancel := context.WithTimeout(ctx, PullTimeout)
	defer cancel()
	reply, err := client.ExchangeEdges(cctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.Unimplemented:
				log.With("peer", pid.String()).Debugf("peer doesn't support edge exchange, falling back to direct record pulling")
				for _, tid := range tids {
					if s.net.queueGetRecords.Schedule(pid, tid, callPriorityLow, s.net.updateRecordsFromPeer) {
						log.With("thread", tid.String()).With("peer", pid.String()).Debugf("record update scheduled")
					}
				}
				return nil
			case codes.Unavailable:
				log.With("peer", pid.String()).Debugf("peer unavailable, skip edge exchange: %s", err.Error())
				return nil
			}
		}
		return err
	}

	for _, e := range reply.GetEdges() {
		tid := e.ThreadID.ID

		// get local edges potentially updated by another process
		addrsEdgeLocal, headsEdgeLocal, err := s.localEdges(tid)
		// we allow local edges to be empty, because the other peer can still have more information
		if err != nil && err != errNoHeadsEdge && err != errNoAddrsEdge {
			log.With("thread", tid.String()).With("peer", pid.String()).Errorf("second retrieval of local edges failed: %v", err)
			continue
		}

		responseEdge := e.GetAddressEdge()
		// We only update the logs if we got non empty values and different hashes for addresses
		// Note that previous versions also sent 0 (aka EmptyEdgeValue) values when the addresses
		// were non-existent, so it shouldn't break backwards compatibility
		if responseEdge != lstoreds.EmptyEdgeValue && responseEdge != addrsEdgeLocal {
			s.net.metrics.DifferentAddressEdges(addrsEdgeLocal, responseEdge, pid.String(), tid.String())
			if s.net.queueGetLogs.Schedule(pid, tid, callPriorityLow, s.net.updateLogsFromPeer) {
				log.With("thread", tid.String()).With("peer", pid.String()).Debugf("log information update for thread scheduled")
			}
		}

		responseEdge = e.GetHeadsEdge()
		// We only update the records if we got non empty values and different hashes for heads
		if responseEdge != lstoreds.EmptyEdgeValue {
			if responseEdge != headsEdgeLocal {
				s.net.metrics.DifferentHeadEdges(headsEdgeLocal, responseEdge, pid.String(), tid.String())
				if s.net.queueGetRecords.Schedule(pid, tid, callPriorityLow, s.net.updateRecordsFromPeer) {
					log.Debugf("record update for thread %s from %s scheduled", tid, pid)
				}
			} else if registry := s.net.tStat; registry != nil {
				// equal heads could be interpreted as successful upload/download
				registry.Apply(pid, tid, threadStatusDownloadDone)
				registry.Apply(pid, tid, threadStatusUploadDone)
			}
		}
	}

	return nil
}

// dial attempts to open a gRPC connection over libp2p to a peer.
func (s *server) dial(peerID peer.ID) (pb.ServiceClient, error) {
	s.Lock()
	defer s.Unlock()
	conn, ok := s.conns[peerID]
	if ok {
		if conn.GetState() == connectivity.Shutdown {
			if err := conn.Close(); err != nil {
				log.With("peer", peerID.String()).Errorf("error closing connection: %v", err)
			}
		} else {
			return pb.NewServiceClient(conn), nil
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), DialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, peerID.Pretty(), s.opts...)
	if err != nil {
		return nil, err
	}
	s.conns[peerID] = conn
	return pb.NewServiceClient(conn), nil
}

// getLibp2pDialer returns a WithContextDialer option for libp2p dialing.
func (s *server) getLibp2pDialer() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, peerIDStr string) (nnet.Conn, error) {
		id, err := peer.Decode(peerIDStr)
		if err != nil {
			return nil, fmt.Errorf("grpc tried to dial non peerID: %w", err)
		}

		conn, err := gostream.Dial(ctx, s.net.host, id, thread.Protocol)
		if err != nil {
			return nil, fmt.Errorf("gostream dial failed: %w", err)
		}

		return conn, nil
	})
}

func withErrLog(pid peer.ID, f func(pid peer.ID) error) {
	if err := f(pid); err != nil {
		log.With("peer", pid.String()).Error(err.Error())
	}
}

// signRequestBody signs an outbound request body with the hosts's private key.
func (s *server) signRequestBody(msg proto.Marshaler) (sig []byte, pk crypto.PubKey, err error) {
	payload, err := msg.Marshal()
	if err != nil {
		return
	}
	sk := s.net.getPrivKey()
	if sk == nil {
		err = fmt.Errorf("private key for host not found")
		return
	}
	sig, err = sk.Sign(payload)
	if err != nil {
		return
	}
	return sig, sk.GetPublic(), nil
}
