package metrics

type ContextKey struct{}

type Metrics interface {
	NumberOfRecordsSentForLog(num int)
	NumberOfRecordsSentTotal(num int)
}

type NoOpMetrics struct{}

func (n NoOpMetrics) NumberOfRecordsSentForLog(num int) {}

func (n NoOpMetrics) NumberOfRecordsSentTotal(num int) {}
