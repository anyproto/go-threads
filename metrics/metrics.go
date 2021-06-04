package metrics

type ContextKey struct {}

type RecordType int

const (
	RecordTypePush RecordType = iota
	RecordTypeGet
)

type Metrics interface {
	AcceptRecord(tp RecordType, isNAT bool)
}

type NoOpMetrics struct {}

func (n *NoOpMetrics) AcceptRecord(tp RecordType, isNat bool) {}
