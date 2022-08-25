package loop

// Purpose of the 'loop' verifier:
//  - this passes around tokens in a loop from an internal channel -> producer ->consumer ->back into the channel
//  - Tuning the quantity of data in flight is done by inserting or dropping tokens
//    from the channel
//  - The auto-tuning process aims to increase the number of tokens in flight until
//    they start backing up (i.e. the internal channel has >1 token in it).  Thereby
//    populating all buffers at all levels of the stack with some messages, and making
//    the benchmark sensitive to interruptions/delays in any component of the system under
//    test.

// This approach is distinct from:
//   - Very lightly loaded traffic generators that tend not to fill up all the
//     internal buffers of a system under the test.
//   - "Brute force" load generators that try and saturate the system under test,
//     and find it hard to distinguish between real latency/delay issues, and
//     the delays while their excess inputs wait to be accepted by the saturated system.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
	worker "github.com/redpanda-data/kgo-verifier/pkg/worker"
)

func newRecord(producerId int, sequence int64) *kgo.Record {
	var key bytes.Buffer
	fmt.Fprintf(&key, "%06d.%018d", producerId, sequence)

	var r *kgo.Record
	r = kgo.KeySliceRecord(key.Bytes(), key.Bytes())
	return r
}

type MessageBody struct {
	Token   int64     `json:"token"`
	SentAt  time.Time `json:"sent_at"`
	Payload []byte    `json:"payload"`
}

type RepeaterConfig struct {
	workerCfg      worker.WorkerConfig
	Group          string
	Partitions     []int32
	KeySpace       worker.KeySpace
	ValueGenerator worker.ValueGenerator
	DataInFlight   uint64
}

func NewRepeaterConfig(cfg worker.WorkerConfig, group string, partitions []int32, keys uint64, payloadSize uint64, dataInFlight uint64) RepeaterConfig {
	return RepeaterConfig{
		workerCfg:      cfg,
		Group:          group,
		Partitions:     partitions,
		KeySpace:       worker.KeySpace{UniqueCount: keys},
		ValueGenerator: worker.ValueGenerator{PayloadSize: payloadSize},
		DataInFlight:   dataInFlight,
	}
}

/**
 * This Worker is a 'well behaved' client that limits the number
 * of messages in flight and spreads them uniformly across partitions.
 */
type Worker struct {
	lock sync.Mutex

	config RepeaterConfig

	totalProduced int64
	totalConsumed int64

	consumeCtx    context.Context
	produceCtx    context.Context
	cancelConsume func()
	cancelProduce func()

	globalStats worker.MessageStats

	// The max number of tokens this object can hold (buffered channels' capacity)
	capacity int64

	// Tokens not currently in flight
	pending chan int64

	// For adding tokens, next ID to allocate
	nextTokenId int64

	// How many tokens have we created on this particular Worker?
	tokenIssueCount int

	// How many tokens have we revoked after creating them (may not have
	// been created on this Worker)?
	tokenRevokeCount int

	// Junk data used to bulk out the message body
	payload []byte

	produceWait sync.WaitGroup
	consumeWait sync.WaitGroup

	client *kgo.Client
}

type LatencyReport struct {
	Ack worker.HistogramSummary `json:"ack"`
	E2e worker.HistogramSummary `json:"e2e"`
}

type WorkerStatus struct {
	Produced int64         `json:"produced"`
	Consumed int64         `json:"consumed"`
	Enqueued int           `json:"enqueued"`
	Errors   int64         `json:"errors"`
	Latency  LatencyReport `json:"latency"`
}

/**
 * Return a json-serializable object describing the
 * worker's progress and performance.
 */
func (v *Worker) Status() WorkerStatus {
	return WorkerStatus{
		Produced: v.totalProduced,
		Consumed: v.totalConsumed,
		Enqueued: len(v.pending),
		Errors:   v.globalStats.Errors,
		Latency: LatencyReport{
			Ack: worker.SummarizeHistogram(&v.globalStats.Ack_latency),
			E2e: worker.SummarizeHistogram(&v.globalStats.E2e_latency),
		},
	}
}

/**
 * Reset all counters
 */
func (v *Worker) Reset() {
	v.globalStats = worker.NewMessageStats()
	v.totalConsumed = 0
	v.totalProduced = 0
}

func (v *Worker) TokenBacklog() int {
	return len(v.pending)
}

func (v *Worker) TokenIssueCount() int {
	return v.tokenIssueCount
}

func (v *Worker) TokenRevokeCount() int {
	return v.tokenRevokeCount
}

func (v *Worker) TokenCapacity() int {
	return int(v.capacity)
}

func (v *Worker) TokenIssue() {
	v.pending <- v.nextTokenId
	v.nextTokenId += 1
	v.tokenIssueCount += 1
}

func (v *Worker) TokenRevoke() {
	// Drop token on the floor
	<-v.pending
	v.tokenRevokeCount += 1
}

func (v *Worker) TokenSize() int {
	return len(v.payload)
}

func (v *Worker) ResetStats() {
	v.globalStats.Reset()
}

func NewWorker(config RepeaterConfig) Worker {
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	produceCtx, cancelProduce := context.WithCancel(context.Background())

	payload := make([]byte, config.ValueGenerator.PayloadSize)

	total_initial_tokens := config.DataInFlight / config.ValueGenerator.PayloadSize

	log.Debugf("Constructing worker with initial tokens %d (%dMB)",
		total_initial_tokens, config.DataInFlight)

	var max_size int64 = 128000
	v := Worker{
		config:        config,
		consumeCtx:    consumeCtx,
		produceCtx:    produceCtx,
		cancelConsume: cancelConsume,
		cancelProduce: cancelProduce,
		payload:       payload,
		globalStats:   worker.NewMessageStats(),
		capacity:      max_size,
		pending:       make(chan int64, max_size),
	}

	var i int64
	for i = 0; i < int64(total_initial_tokens); i++ {
		v.TokenIssue()
	}

	return v
}

func (v *Worker) ConsumeRecord(r *kgo.Record) {
	v.lock.Lock()
	defer v.lock.Unlock()

	v.totalConsumed += 1

	log.Debugf("Consume %s got record on partition %d...", v.config.workerCfg.Name, r.Partition)

	message := MessageBody{}
	err := json.Unmarshal(r.Value, &message)
	if err != nil {
		// This typically happens if the topic was used by other
		// traffic generators at the same time.
		log.Errorf("Consume %s parse error on body from %s.%d offset %d: %s",
			v.config.workerCfg.Name, r.Topic, r.Partition, r.Offset,
			r.Value[0:64])
		v.globalStats.Errors += 1
		return
	}

	token := message.Token

	now := time.Now()
	e2e_latency := now.Sub(message.SentAt)

	v.globalStats.E2e_latency.Update(e2e_latency.Microseconds())

	log.Debugf("Consume %s token %06d, total latency %s", v.config.workerCfg.Name, token, e2e_latency)
	v.pending <- int64(token)
}

func (v *Worker) Init() {
	opts := v.config.workerCfg.MakeKgoOpts()

	opts = append(opts, []kgo.Opt{
		kgo.ConsumeTopics(v.config.workerCfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.ProducerBatchMaxBytes(1024 * 1024),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ConsumerGroup(v.config.Group),
	}...)

	if v.config.Group != "" {
		opts = append(opts, kgo.ConsumerGroup(v.config.Group))
	}

	client, err := kgo.NewClient(opts...)
	util.Chk(err, "unable to initialize client: %v", err)
	v.client = client
}

func (v *Worker) Shutdown() {
	v.client.Close()
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func (v *Worker) Produce() {
	defer v.produceWait.Done()

	var ackWait sync.WaitGroup

loop:
	for {
		// Drop out if signalled to stop
		log.Debugf("Produce %s checking for token...", v.config.workerCfg.Name)

		var token int64
		select {
		case <-v.produceCtx.Done():
			log.Infof("Produce %s got Done signal", v.config.workerCfg.Name)
			break loop
		case token = <-v.pending:
			log.Debugf("Produce %s sending token %d", v.config.workerCfg.Name, token)
		}

		var maxKey uint64
		if v.config.KeySpace.UniqueCount > 0 {
			maxKey = v.config.KeySpace.UniqueCount
		} else {
			maxKey = ^uint64(0)
		}
		var key bytes.Buffer
		fmt.Fprintf(&key, "%d", rand.Uint64()%maxKey)

		var r *kgo.Record

		sentAt := time.Now()
		message := MessageBody{
			Token:   token,
			SentAt:  sentAt,
			Payload: v.payload,
		}
		message_bytes, err := json.Marshal(message)
		util.Chk(err, "Serializing message")

		r = kgo.KeySliceRecord(key.Bytes(), message_bytes)

		handler := func(r *kgo.Record, err error) {
			// FIXME: error doesn't necessarily mean the write wasn't committed:
			// consumer needs logic to handle the unexpected token
			log.Debugf("Produce %s acked %d on partition %d offset %d", v.config.workerCfg.Name, token, r.Partition, r.Offset)
			if err != nil {
				// On produce error, we drop the token: we rely on producer errors
				// being rare and/or a background Tuner re-injecting fresh tokens
				log.Errorf("Produce %s error, dropped token %d: %v", v.config.workerCfg.Name, token, err)
				v.globalStats.Errors += 1
				v.tokenRevokeCount += 1
			} else {
				ackLatency := time.Now().Sub(sentAt)
				v.globalStats.Ack_latency.Update(ackLatency.Microseconds())
				v.totalProduced += 1
			}
			ackWait.Done()
		}

		ackWait.Add(1)
		v.client.Produce(v.produceCtx, r, handler)
	}

	log.Infof("Produce shutdown: flushing")
	flushCtx, flushCtxCancel := context.WithTimeout(context.Background(), time.Duration(time.Millisecond*1000))
	defer flushCtxCancel()
	v.client.Flush(flushCtx)
	log.Infof("Produce shutdown: waiting")
	waitTimeout(&ackWait, time.Duration(time.Millisecond*1000))
	log.Infof("Produce shutdown: complete")
}

func (v *Worker) Consume() {
	defer v.consumeWait.Done()

loop:
	for {
		// Drop out if signalled to stop
		select {
		case <-v.consumeCtx.Done():
			log.Info("Consumer got Done signal")
			break loop
		default:
			// Do nothing
		}

		log.Debugf("Consume %s fetching...", v.config.workerCfg.Name)
		fetches := v.client.PollFetches(v.consumeCtx)
		log.Debugf("Consume %s fetched %d records", v.config.workerCfg.Name, len(fetches.Records()))

		fetches.EachError(func(t string, p int32, err error) {
			// This is non-fatal because it includes e.g. a topic getting
			// prefix-truncated on retention limits, and thereby getting
			// a "lost records" on the consumer
			log.Errorf("Consume %s topic %s partition %d had error: %v", v.config.workerCfg.Name, t, p, err)
		})

		if len(fetches.Records()) == 0 && len(fetches.Errors()) == 0 {
			log.Warnf("Consumed %s got empty fetch result", v.config.workerCfg.Name)

		}

		fetches.EachRecord(func(r *kgo.Record) {
			v.ConsumeRecord(r)
		})

	}
	log.Debug("Consume %s dropping out", v.config.workerCfg.Name)

	sync_ctx, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)
	v.client.CommitUncommittedOffsets(sync_ctx)
}

func (v *Worker) Prepare() {
	v.Init()

	// Consumer must start first, to initialize read offset
	// below where the producer will produce
	v.consumeWait.Add(1)
	go v.Consume()
}

func (v *Worker) Activate() {
	log.Debug("Start")
	v.produceWait.Add(1)
	go v.Produce()
}

func (v *Worker) Stop() {
	log.Info("Signalling produce to stop")
	v.cancelProduce()
}

func (v *Worker) Wait() worker.Result {
	log.Info("Waiting for produce to stop")
	v.produceWait.Wait()

	// Drain phase
	log.Info("Waiting for consume to idle")
	// TODO Wait for drain
	v.cancelConsume()
	v.consumeWait.Wait()

	// Annoyingly, the go-metrics Histogram object doesn't
	// seem to have a way of merging multiple histograms, so
	// instead of summing up our partition stats here, we had
	// to keep this separate global one throughout
	return worker.Result{Stats: v.globalStats}
}
