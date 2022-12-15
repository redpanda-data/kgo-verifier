package worker

import (
	"fmt"
	"os"
	"strings"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

func SummarizeHistogram(h *metrics.Histogram) HistogramSummary {
	return HistogramSummary{
		P50: (*h).Percentile(0.5),
		P90: (*h).Percentile(0.90),
		P99: (*h).Percentile(0.99),
	}
}

type MessageStats struct {
	Produced int64
	Consumed int64
	Errors   int64

	// Note: the underlying metrics.Sample structure uses a
	// mutex on each insert :-/

	// Latency from calling produce to ack handler call
	Ack_latency metrics.Histogram

	// Latency from calling produce to receiving message in consumer
	E2e_latency metrics.Histogram

	// Server errors that caused us to tear down and rebuild the client
	// to proceed: these are potential redpanda bugs.
	Rebuilds int64
}

func NewMessageStats() MessageStats {
	s_ack := metrics.NewExpDecaySample(1024, 0.015)
	s_e2e := metrics.NewExpDecaySample(1024, 0.015)
	return MessageStats{
		Ack_latency: metrics.NewHistogram(s_ack),
		E2e_latency: metrics.NewHistogram(s_e2e),
		Errors:      0,
	}
}

type Result struct {
	Stats MessageStats
}

func (r *Result) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "Ack: 50/90/99 (us) : %.2f/%.2f/%.2fus",
		r.Stats.Ack_latency.Percentile(0.5),
		r.Stats.Ack_latency.Percentile(0.90),
		r.Stats.Ack_latency.Percentile(0.99),
	)
	fmt.Fprintf(&b, "E2e: 50/90/99 (us) : %.2f/%.2f/%.2fus",
		r.Stats.E2e_latency.Percentile(0.5),
		r.Stats.E2e_latency.Percentile(0.90),
		r.Stats.E2e_latency.Percentile(0.99),
	)

	return b.String()
}

type Outage struct {
	start time.Time
	end   time.Time
}

func (ms *MessageStats) Reset() {
	ms.Errors = 0
	ms.Ack_latency.Clear()
	ms.E2e_latency.Clear()
}

type Worker interface {
	GetStatus() interface{}
	ResetStats()
}

type KeySpace struct {
	UniqueCount uint64
}

type ValueGenerator struct {
	PayloadSize uint64
}

type WorkerConfig struct {
	Name               string
	Brokers            string
	Trace              bool
	Topic              string
	Linger             time.Duration
	MaxBufferedRecords uint
	BatchMaxbytes      uint
	SaslUser           string
	SaslPass           string
	Transactions       bool
}

func (wc *WorkerConfig) MakeKgoOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(wc.Brokers, ",")...),

		// Producer properties
		kgo.DefaultProduceTopic(wc.Topic),

		kgo.ProducerBatchMaxBytes(int32(wc.BatchMaxbytes)),
		kgo.MaxBufferedRecords(int(wc.MaxBufferedRecords)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	if wc.Name != "" {
		opts = append(opts, kgo.ClientID(wc.Name))

	}

	if wc.Transactions {
		opts = append(opts, []kgo.Opt{
			// By default kgo reads uncommited records and unstable offsets
			kgo.RequireStableFetchOffsets(),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		}...)
	}

	// Disable auth if username not given
	if len(wc.SaslUser) > 0 {
		auth_mech := scram.Auth{
			User: wc.SaslUser,
			Pass: wc.SaslPass,
		}
		auth := auth_mech.AsSha256Mechanism()
		opts = append(opts,
			kgo.SASL(auth))
	}

	if wc.Trace {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, func() string {
			return fmt.Sprintf("time=\"%s\" name=%s", time.Now().UTC().Format(time.RFC3339), wc.Name)
		})))
	}

	return opts
}

func NewWorkerConfig(name string, brokers string, trace bool, topic string, linger time.Duration, maxBufferedRecords uint, transactions bool) WorkerConfig {
	return WorkerConfig{
		Name:               name,
		Brokers:            brokers,
		Trace:              trace,
		Topic:              topic,
		Linger:             linger,
		MaxBufferedRecords: maxBufferedRecords,
		SaslUser:           "",
		SaslPass:           "",
		Transactions:       transactions,
	}
}
