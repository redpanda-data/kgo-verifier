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
	Start()
	Stop()
	Wait() Result
}

type WorkerReport struct {
}

type KeySpace struct {
	UniqueCount uint64
}

type ValueGenerator struct {
	PayloadSize uint64
}

type WorkerConfig struct {
	Brokers            string
	Trace              bool
	Topic              string
	Linger             time.Duration
	MaxBufferedRecords uint
	BatchMaxbytes      uint
	SaslUser           string
	SaslPass           string
}

func (wc *WorkerConfig) MakeKgoOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(wc.Brokers, ",")...),

		// Consumer properties
		kgo.ConsumeTopics(wc.Topic),

		// Producer properties
		kgo.DefaultProduceTopic(wc.Topic),

		kgo.ProducerBatchMaxBytes(int32(wc.BatchMaxbytes)),
		kgo.MaxBufferedRecords(int(wc.MaxBufferedRecords)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
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
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}

	return opts
}

func NewWorkerConfig(brokers string, trace bool, topic string, linger time.Duration, maxBufferedRecords uint) WorkerConfig {
	return WorkerConfig{
		Brokers:            brokers,
		Trace:              trace,
		Topic:              topic,
		Linger:             linger,
		MaxBufferedRecords: maxBufferedRecords,
		SaslUser:           "",
		SaslPass:           "",
	}
}
