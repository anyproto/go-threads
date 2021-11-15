package metrics

type ContextKey struct{}

type RecordType int

const (
	RecordTypePush RecordType = iota
	RecordTypeGet
	RecordTypePubsub
)

type Metrics interface {
	AcceptRecord(tp RecordType, isNAT bool)
	CreateRecord(threadId string, prepareMs int64, newRecordMs int64, localEventBusMs int64, pushRecordMs int64)
}

type NoOpMetrics struct{}

func (n *NoOpMetrics) CreateRecord(threadId string, prepareMs int64, newRecordMs int64, localEventBusMs int64, pushRecordMs int64) {}

func (n *NoOpMetrics) AcceptRecord(tp RecordType, isNat bool) {}
