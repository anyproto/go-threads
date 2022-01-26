package metrics

import prom "github.com/prometheus/client_golang/prometheus"

type ContextKey struct{}

type Metrics interface {
	NumberOfRecordsSentForLog(num int)
	NumberOfRecordsSentTotal(num int)
}

type NoOpMetrics struct{}

func (n NoOpMetrics) NumberOfRecordsSentForLog(num int) {}

func (n NoOpMetrics) NumberOfRecordsSentTotal(num int) {}

var (
	GetRecordsGetThread = prom.NewSummary(prom.SummaryOpts{
		Namespace: "threads",
		Subsystem: "net",
		Name:      "get_records_get_thread_ms",
		Help:      "The time in ms of getting the thread from logstore",
	})

	GetRecordsHeadsChanged = prom.NewSummary(prom.SummaryOpts{
		Namespace: "threads",
		Subsystem: "net",
		Name:      "get_records_heads_changed_ms",
		Help:      "The time in ms of getting heads from logstore",
	})

	GetLocalRecordsGetLog = prom.NewSummary(prom.SummaryOpts{
		Namespace: "threads",
		Subsystem: "net",
		Name:      "get_local_records_get_log_ms",
		Help:      "The time in ms of getting the log from logstore",
	})

	GetLocalRecordsCborGetRecords = prom.NewSummary(prom.SummaryOpts{
		Namespace: "threads",
		Subsystem: "net",
		Name:      "get_local_records_cbor_get_records_ms",
		Help:      "The time in ms of getting all the records from blockstore",
	})
)

func init() {
	prom.MustRegister(GetRecordsHeadsChanged)
	prom.MustRegister(GetRecordsGetThread)
	prom.MustRegister(GetLocalRecordsGetLog)
	prom.MustRegister(GetLocalRecordsCborGetRecords)
}
