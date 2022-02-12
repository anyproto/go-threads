package metrics

import "time"

type RecordType int

const (
	RecordTypePush RecordType = iota
	RecordTypeGet
	RecordTypePubsub
)

type Metrics interface {
	AcceptRecord(tp RecordType, isNAT bool)
	CreateRecord(threadId string, prepareMs int64, newRecordMs int64, localEventBusMs int64, pushRecordMs int64)
	DifferentAddressEdges(localEdgeHash uint64, remoteEdgeHash uint64, peerId string, threadId string)
	DifferentHeadEdges(localEdgeHash uint64, remoteEdgeHash uint64, peerId string, threadId string)

	NumberOfRecordsSentForLog(num int)
	NumberOfRecordsSentTotal(num int)

	GetRecordsGetThreadDuration(duration time.Duration)
	GetRecordsHeadsChangedDuration(duration time.Duration)
	GetLocalRecordsGetLogDuration(duration time.Duration)
	GetLocalRecordsCborGetRecordsDuration(duration time.Duration)

	SemaphoreAcquireDuration(duration time.Duration)
	SemaphoreHoldDuration(duration time.Duration)

	SemaphoreAcquire()

	ThreadServed()
	ThreadPulled()
	ThreadPullDuration(duration time.Duration)

	UpdateRecordsDelayAfterExchangeEdges(duration time.Duration)
}

type NoOpMetrics struct{}

func (n *NoOpMetrics) NumberOfRecordsSentForLog(num int) {}

func (n *NoOpMetrics) NumberOfRecordsSentTotal(num int) {}

func (n *NoOpMetrics) GetRecordsGetThreadDuration(duration time.Duration) {}

func (n *NoOpMetrics) GetRecordsHeadsChangedDuration(duration time.Duration) {}

func (n *NoOpMetrics) GetLocalRecordsGetLogDuration(duration time.Duration) {}

func (n *NoOpMetrics) GetLocalRecordsCborGetRecordsDuration(duration time.Duration) {}

func (n *NoOpMetrics) SemaphoreAcquireDuration(duration time.Duration) {}

func (n *NoOpMetrics) SemaphoreHoldDuration(duration time.Duration) {}

func (n *NoOpMetrics) SemaphoreAcquire() {}

func (n *NoOpMetrics) ThreadServed() {}

func (n *NoOpMetrics) ThreadPulled() {}

func (n *NoOpMetrics) ThreadPullDuration(duration time.Duration) {}

func (n *NoOpMetrics) DifferentAddressEdges(
	localEdgeHash uint64,
	remoteEdgeHash uint64,
	peerId string,
	threadId string) {
}

func (n *NoOpMetrics) DifferentHeadEdges(
	localEdgeHash uint64,
	remoteEdgeHash uint64,
	peerId string,
	threadId string) {
}

func (n *NoOpMetrics) CreateRecord(
	threadId string,
	prepareMs int64,
	newRecordMs int64,
	localEventBusMs int64,
	pushRecordMs int64) {
}

func (n *NoOpMetrics) AcceptRecord(tp RecordType, isNat bool) {}

func (n *NoOpMetrics) UpdateRecordsDelayAfterExchangeEdges(duration time.Duration) {}
