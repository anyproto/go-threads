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
	CreateRecord(beforeMs int, localEventBusMs int, pushRecordMs int)
}

type NoOpMetrics struct{}

func (n *NoOpMetrics) CreateRecord(beforeMs int, localEventBusMs int, pushRecordMs int) {}

func (n *NoOpMetrics) AcceptRecord(tp RecordType, isNat bool) {}
