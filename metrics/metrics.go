package metrics

import (
	"time"

	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/textileio/go-threads/net/util"
)

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

	UpdateRecordsDelayAfterExchangeEdges = prom.NewHistogram(prom.HistogramOpts{
		Namespace: "threads",
		Subsystem: "net",
		Name:      "update_records_delay_after_exchange_edges",
		Help:      "Delay between the server receiving exchange edges and the server sending update records",
		Buckets: util.MetricTimeBuckets([]time.Duration{
			256 * time.Millisecond,
			512 * time.Millisecond,
			1024 * time.Millisecond,
			2 * time.Second,
			4 * time.Second,
			8 * time.Second,
			16 * time.Second,
			30 * time.Second,
			45 * time.Second,
			60 * time.Second,
			90 * time.Second,
			120 * time.Second,
			180 * time.Second,
			240 * time.Second,
		}),
	})
)

func init() {
	prom.MustRegister(GetRecordsHeadsChanged)
	prom.MustRegister(GetRecordsGetThread)
	prom.MustRegister(GetLocalRecordsGetLog)
	prom.MustRegister(GetLocalRecordsCborGetRecords)
}
