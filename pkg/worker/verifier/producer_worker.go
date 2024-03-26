package verifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rcrowley/go-metrics"
	"github.com/redpanda-data/kgo-verifier/pkg/util"
	worker "github.com/redpanda-data/kgo-verifier/pkg/worker"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

type ProducerConfig struct {
	workerCfg             worker.WorkerConfig
	name                  string
	nPartitions           int32
	messageSize           int
	messageCount          int
	fakeTimestampMs       int64
	fakeTimestampStepMs   int64
	rateLimitBytes        int
	keySetCardinality     int
	messagesPerProducerId int
	valueGenerator        worker.ValueGenerator
}

func NewProducerConfig(wc worker.WorkerConfig, name string, nPartitions int32,
	messageSize int, messageCount int, fakeTimestampMs int64, fakeTimestampStepMs int64, rateLimitBytes int, keySetCardinality int, messagesPerProducerId int) ProducerConfig {
	return ProducerConfig{
		workerCfg:             wc,
		name:                  name,
		nPartitions:           nPartitions,
		messageCount:          messageCount,
		messageSize:           messageSize,
		fakeTimestampMs:       fakeTimestampMs,
		fakeTimestampStepMs:   fakeTimestampStepMs,
		rateLimitBytes:        rateLimitBytes,
		keySetCardinality:     keySetCardinality,
		messagesPerProducerId: messagesPerProducerId,
		valueGenerator: worker.ValueGenerator{
			PayloadSize:  uint64(messageSize),
			Compressible: wc.CompressiblePayload,
		},
	}
}

type ProducerWorker struct {
	config       ProducerConfig
	Status       ProducerWorkerStatus
	validOffsets TopicOffsetRanges

	payload []byte

	// Used for enabling transactional produces
	transactionsEnabled  bool
	transactionSTMConfig worker.TransactionSTMConfig
	transactionSTM       *worker.TransactionSTM
	churnProducers       bool

	tolerateDataLoss bool
}

func NewProducerWorker(cfg ProducerConfig) ProducerWorker {
	validOffsets := LoadTopicOffsetRanges(cfg.workerCfg.Topic, cfg.nPartitions)
	if cfg.workerCfg.TolerateDataLoss {
		for ix := range validOffsets.PartitionRanges {
			validOffsets.PartitionRanges[ix].TolerateDataLoss = true
		}
	}

	return ProducerWorker{
		config:           cfg,
		Status:           NewProducerWorkerStatus(cfg.workerCfg.Topic),
		validOffsets:     validOffsets,
		payload:          cfg.valueGenerator.Generate(),
		churnProducers:   cfg.messagesPerProducerId > 0,
		tolerateDataLoss: cfg.workerCfg.TolerateDataLoss,
	}
}

func (v *ProducerWorker) EnableTransactions(config worker.TransactionSTMConfig) {
	v.transactionSTMConfig = config
	v.transactionsEnabled = true
}

func (pw *ProducerWorker) newRecord(producerId int, sequence int64) *kgo.Record {

	var header_key bytes.Buffer
	if !pw.transactionsEnabled || !pw.transactionSTM.InAbortedTransaction() {
		fmt.Fprintf(&header_key, "%06d.%018d", producerId, sequence)

	} else {
		// This message ensures that `ValidatorStatus.ValidateRecord`
		// will report it as an invalid read if it's consumed. This is
		// since messages in aborted transactions should never be read.
		fmt.Fprintf(&header_key, "ABORTED MSG: %06d.%018d", producerId, sequence)
		pw.Status.AbortedTransactionMessages += 1
	}

	payload := make([]byte, pw.config.messageSize)
	var r *kgo.Record

	if pw.config.keySetCardinality < 0 {
		// by default use the same value as in record id header key
		r = kgo.KeySliceRecord(header_key.Bytes(), payload)
	} else {
		// generate a random key from a set of requested cardinality
		var key bytes.Buffer
		fmt.Fprintf(&key, "key-%d", rand.Intn(pw.config.keySetCardinality))
		r = kgo.KeySliceRecord(key.Bytes(), payload)

	}

	r.Headers = append(r.Headers, kgo.RecordHeader{Key: "KGO_VERIFIER_RECORD_ID", Value: header_key.Bytes()})

	if pw.config.fakeTimestampMs != -1 {
		r.Timestamp = time.Unix(0, pw.config.fakeTimestampMs*1000000)
		pw.config.fakeTimestampMs += pw.config.fakeTimestampStepMs
	}
	return r
}

type ProducerWorkerStatus struct {
	// Topic being produced to
	Topic string `json:"topic"`
	// How many messages did we try to transmit?
	Sent int64 `json:"sent"`

	// How many messages did we send successfully (were acked
	// by the server at the offset we expected)?
	Acked int64 `json:"acked"`

	// How many messages landed at an unexpected offset?
	// (indicates retries/resends)
	BadOffsets int64 `json:"bad_offsets"`

	MaxOffsetsProduced map[int32]int64 `json:"max_offsets_produced"`

	// How many times did we restart the producer loop?
	Restarts int64 `json:"restarts"`

	// How many failures occured while trying to begin, abort,
	// or commit a transaction.
	FailedTransactions int64 `json:"failed_transactions"`

	// How many messages were sent inside aborted transactions?
	// (this is not the transaction abort count, it's the count of the messages
	//  inside those transactions)
	AbortedTransactionMessages int64 `json:"aborted_transaction_msgs"`

	// Ack latency: a private histogram for the data,
	// and a public summary for JSON output
	latency metrics.Histogram
	Latency worker.HistogramSummary `json:"latency"`

	Active bool `json:"active"`

	lock sync.Mutex

	// For emitting checkpoints on time intervals
	lastCheckpoint time.Time
}

func NewProducerWorkerStatus(topic string) ProducerWorkerStatus {
	return ProducerWorkerStatus{
		Topic:              topic,
		MaxOffsetsProduced: make(map[int32]int64),
		lastCheckpoint:     time.Now(),
		latency:            metrics.NewHistogram(metrics.NewExpDecaySample(1024, 0.015)),
	}
}

func (self *ProducerWorkerStatus) OnAcked(Partition int32, Offset int64) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.Acked += 1

	currentMax, present := self.MaxOffsetsProduced[Partition]
	if present {
		if currentMax < Offset {
			expected := currentMax + 1
			if Offset != expected {
				log.Warnf("Gap detected in produced offsets. Expected %d, but got %d", expected, Offset)
			}

			self.MaxOffsetsProduced[Partition] = Offset
		}
	} else {
		self.MaxOffsetsProduced[Partition] = Offset
	}
}

func (self *ProducerWorkerStatus) OnBadOffset() {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.BadOffsets += 1
}

func (pw *ProducerWorker) produceCheckpoint() {
	err := pw.validOffsets.Store()
	util.Chk(err, "Error writing offset map: %v", err)

	status, lock := pw.GetStatus()

	lock.Lock()
	data, err := json.Marshal(status)
	lock.Unlock()

	util.Chk(err, "Status serialization error")
	log.Infof("Producer status: %s", data)
}

func (pw *ProducerWorker) Wait() error {
	pw.Status.Active = true
	defer func() { pw.Status.Active = false }()

	n := int64(pw.config.messageCount)

	for {
		n_produced, bad_offsets, err := pw.produceInner(n)
		if err != nil {
			return err
		}
		n = n - n_produced

		if len(bad_offsets) > 0 {
			log.Infof("Produce stopped early, %d still to do", n)
		}

		if n <= 0 {
			return nil
		} else {
			// Record that we took another run at produceInner
			pw.Status.Restarts += 1
		}
	}
}

type BadOffset struct {
	P int32
	O int64
}

func (pw *ProducerWorker) produceInner(n int64) (int64, []BadOffset, error) {
	opts := pw.config.workerCfg.MakeKgoOpts()

	opts = append(opts, []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}...)

	if pw.transactionsEnabled {
		randId := uuid.New()
		tid := "p" + randId.String()
		log.Debugf("Configuring transactions with TransactionalID %s", tid)

		opts = append(opts, []kgo.Opt{
			kgo.TransactionalID(tid),
			kgo.TransactionTimeout(2 * time.Minute),
		}...)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Errorf("Error creating Kafka client: %v", err)
		return 0, nil, err
	}

	if pw.transactionsEnabled {
		pw.transactionSTM = worker.NewTransactionSTM(context.Background(), client, pw.transactionSTMConfig)
	}

	nextOffset := GetOffsets(client, pw.config.workerCfg.Topic, pw.config.nPartitions, -1)

	for i, o := range nextOffset {
		log.Infof("Produce start offset %s/%d %d...", pw.config.workerCfg.Topic, i, o)
	}

	var wg sync.WaitGroup

	errored := false
	produced := int64(0)

	// Channel must be >= concurrency
	bad_offsets := make(chan BadOffset, 16384)
	concurrent := semaphore.NewWeighted(4096)

	log.Infof("Producing %d messages (%d bytes), rate limit=%d", n, pw.config.messageSize, pw.config.rateLimitBytes)

	var rlimiter *rate.Limiter
	if pw.config.rateLimitBytes > 0 {
		rlimiter = rate.NewLimiter(rate.Limit(pw.config.rateLimitBytes), pw.config.rateLimitBytes)
	}

	for i := int64(0); i < n && len(bad_offsets) == 0; i = i + 1 {
		concurrent.Acquire(context.Background(), 1)
		produced += 1
		pw.Status.Sent += 1
		var p = rand.Int31n(pw.config.nPartitions)

		if pw.transactionsEnabled {
			addedControlMarkers, err := pw.transactionSTM.BeforeMessageSent()
			if err != nil {
				log.Errorf("Transaction error %v", err)
				errored = true
				pw.Status.FailedTransactions += 1
				break
			}

			if addedControlMarkers > 0 {
				for i, _ := range nextOffset {
					for j := int64(0); j < int64(addedControlMarkers); j = j + 1 {
						pw.validOffsets.Insert(p, nextOffset[i]+j)
					}
					nextOffset[i] += addedControlMarkers
				}
			}
		}

		if pw.churnProducers && pw.Status.Sent > 0 && pw.Status.Sent%int64(pw.config.messagesPerProducerId) == 0 {
			break
		}

		expectOffset := nextOffset[p]
		nextOffset[p] += 1

		r := pw.newRecord(0, expectOffset)
		r.Partition = p
		wg.Add(1)

		if rlimiter != nil {
			rlimiter.WaitN(context.Background(), len(r.Value))
		}

		log.Debugf("Writing partition %d at %d", r.Partition, expectOffset)

		sentAt := time.Now()
		handler := func(r *kgo.Record, err error) {
			concurrent.Release(1)
			util.Chk(err, "Produce failed: %v", err)
			if expectOffset != r.Offset {
				log.Warnf("Produced at unexpected offset %d (expected %d) on partition %d", r.Offset, expectOffset, r.Partition)
				pw.Status.OnBadOffset()
				bad_offsets <- BadOffset{r.Partition, r.Offset}
				errored = true
				log.Debugf("errored = %t", errored)
			} else {
				ackLatency := time.Now().Sub(sentAt)
				pw.Status.OnAcked(r.Partition, r.Offset)
				pw.Status.latency.Update(ackLatency.Microseconds())
				log.Debugf("Wrote partition %d at %d", r.Partition, r.Offset)
				pw.validOffsets.Insert(r.Partition, r.Offset)
			}
			wg.Done()
		}
		client.Produce(context.Background(), r, handler)

		// Not strictly necessary, but useful if a long running producer gets killed
		// before finishing

		if time.Since(pw.Status.lastCheckpoint) > 5*time.Second {
			pw.Status.lastCheckpoint = time.Now()
			pw.produceCheckpoint()
		}
	}

	if pw.transactionsEnabled {
		if err := pw.transactionSTM.TryEndTransaction(); err != nil {
			log.Errorf("unable to end transaction: %v", err)
			errored = true
			pw.Status.FailedTransactions += 1
		}
	}

	log.Info("Waiting...")
	wg.Wait()
	log.Info("Waited.")
	wg.Wait()
	close(bad_offsets)

	log.Info("Closing client...")
	client.Close()
	log.Info("Closed client.")

	pw.produceCheckpoint()

	if errored {
		log.Warnf("%d bad offsets", len(bad_offsets))
		var r []BadOffset
		for o := range bad_offsets {
			r = append(r, o)
		}
		if len(r) == 0 && pw.Status.FailedTransactions == 0 {
			util.Die("No bad offsets or failed transactions but errored?")
		}
		successful_produced := produced - int64(len(r))
		return successful_produced, r, nil
	} else {
		wg.Wait()
		return produced, nil, nil
	}
}

func (pw *ProducerWorker) ResetStats() {
	pw.Status = NewProducerWorkerStatus(pw.config.workerCfg.Topic)
}

func (pw *ProducerWorker) GetStatus() (interface{}, *sync.Mutex) {
	// Update public summary from private statustics
	pw.Status.Latency = worker.SummarizeHistogram(&pw.Status.latency)

	return &pw.Status, &pw.Status.lock
}
