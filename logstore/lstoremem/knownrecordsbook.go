package lstoremem

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	core "github.com/textileio/go-threads/core/logstore"
	"github.com/textileio/go-threads/core/thread"
)

type knownRecordsBook struct {
	knownRecords map[thread.ID]map[peer.ID][]core.RecordsRange
	rmu          sync.RWMutex
}

// TODO: the algorithm here is very unoptimal, we must benchmark if we need to use different solution
// This can be linked list with O(1) insertion but O(n) search, Deque (if we mostly append either to the beggining or an end)
// or a RB tree with O(log N) insertions and searches
func (k *knownRecordsBook) AddRange(threadId thread.ID, peerId peer.ID, recordsRange core.RecordsRange) error {
	k.rmu.Lock()
	defer k.rmu.Unlock()

	logs, ok := k.knownRecords[threadId]
	if !ok {
		k.knownRecords[threadId] = make(map[peer.ID][]core.RecordsRange)
		logs = k.knownRecords[threadId]
	}

	ranges, ok := logs[peerId]
	if !ok {
		logs[peerId] = make([]core.RecordsRange, 0, 10)
		ranges = logs[peerId]
	}

	defer func() {
		logs[peerId] = ranges
	}()

	idx := 0
	for idx < len(ranges) && recordsRange.Start.Counter > ranges[idx].End.Counter {
		idx++
	}

	// if every range is less than the added range
	if idx == len(ranges) {
		ranges = append(ranges, recordsRange)
		return nil
	}

	// if we don't have any intersection with ranges then adding it in the middle
	if recordsRange.End.Counter < ranges[idx].Start.Counter {
		if idx == 0 {
			ranges = append([]core.RecordsRange{recordsRange}, ranges...)
		} else {
			ranges = append(ranges[:idx+1], ranges[:idx]...)
			ranges[idx] = recordsRange
		}

		return nil
	}

	startIdx := idx
	for idx < len(ranges) && recordsRange.End.Counter >= ranges[idx].End.Counter {
		idx++
	}

	endIdx := idx
	if idx == len(ranges) || recordsRange.End.Counter < ranges[idx].End.Counter {
		endIdx = idx - 1
	}

	joinedRange := core.RecordsRange{
		Start: minRangeStart(recordsRange, ranges[startIdx]),
		End:   maxRangeEnd(recordsRange, ranges[endIdx]),
	}

	ranges[startIdx] = joinedRange
	if endIdx == startIdx {
		return nil
	}

	if endIdx < len(ranges)-1 {
		copy(ranges[startIdx+1:], ranges[endIdx+1:])
	}
	ranges = ranges[:len(ranges)-(endIdx-startIdx)]
	return nil
}

func (k *knownRecordsBook) RemoveRange(threadId thread.ID, peerId peer.ID, recordsRange core.RecordsRange) error {
	// This should be implemented when we will add something like garbage collection etc
	panic("implement me")
}

func (k *knownRecordsBook) DumpKnownRecords() (core.DumpKnownRecordsBook, error) {
	return core.DumpKnownRecordsBook{Data: k.knownRecords}, nil
}

func (k *knownRecordsBook) RestoreKnownRecords(knownRecords core.DumpKnownRecordsBook) error {
	k.knownRecords = knownRecords.Data
	return nil
}

func (k *knownRecordsBook) RangeForCounter(threadId thread.ID, peerId peer.ID, counter int64) (core.RecordsRange, error) {
	k.rmu.RLock()
	defer k.rmu.RUnlock()

	emptyRng := core.RecordsRange{}

	logs, ok := k.knownRecords[threadId]
	if !ok {
		return emptyRng, core.ErrRangeNotFound
	}

	ranges, ok := logs[peerId]
	if !ok {
		return emptyRng, core.ErrRangeNotFound
	}

	for _, r := range ranges {
		if r.Start.Counter <= counter && r.End.Counter >= counter {
			return r, nil
		}
	}

	return emptyRng, core.ErrRangeNotFound
}

func minRangeStart(x, y core.RecordsRange) thread.Head {
	if x.Start.Counter < y.Start.Counter {
		return x.Start
	}
	return y.Start
}

func maxRangeEnd(x, y core.RecordsRange) thread.Head {
	if x.End.Counter > y.End.Counter {
		return x.End
	}
	return y.End
}
