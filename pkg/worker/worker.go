package verifier

import (
	"fmt"
	"strings"
	"time"

	metrics "github.com/rcrowley/go-metrics"
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
	Group              string
	Partitions         []int32
	KeySpace           KeySpace
	ValueGenerator     ValueGenerator
	DataInFlight       uint64
}

func NewWorkerConfig(brokers string, trace bool, topic string, linger time.Duration, maxBufferedRecords uint, group string, partitions []int32, keys uint64, payloadSize uint64, dataInFlight uint64) WorkerConfig {
	return WorkerConfig{
		Brokers:            brokers,
		Trace:              trace,
		Topic:              topic,
		Linger:             linger,
		MaxBufferedRecords: maxBufferedRecords,
		Group:              group,
		Partitions:         partitions,
		KeySpace:           KeySpace{UniqueCount: keys},
		ValueGenerator:     ValueGenerator{PayloadSize: payloadSize},
		DataInFlight:       dataInFlight,
	}
}
