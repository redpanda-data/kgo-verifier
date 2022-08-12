package verifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
	worker "github.com/redpanda-data/kgo-verifier/pkg/worker"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/semaphore"
)

type ProducerConfig struct {
	workerCfg    worker.WorkerConfig
	name         string
	nPartitions  int32
	messageSize  int
	messageCount int
}

func NewProducerConfig(wc worker.WorkerConfig, name string, nPartitions int32,
	messageSize int, messageCount int) ProducerConfig {
	return ProducerConfig{
		workerCfg:    wc,
		name:         name,
		nPartitions:  nPartitions,
		messageCount: messageCount,
		messageSize:  messageSize,
	}
}

type ProducerWorker struct {
	config       ProducerConfig
	Status       ProducerWorkerStatus
	validOffsets TopicOffsetRanges
}

func NewProducerWorker(cfg ProducerConfig) ProducerWorker {
	return ProducerWorker{
		config:       cfg,
		Status:       NewProducerWorkerStatus(),
		validOffsets: LoadTopicOffsetRanges(cfg.workerCfg.Topic, cfg.nPartitions),
	}
}

func (pw *ProducerWorker) newRecord(producerId int, sequence int64) *kgo.Record {
	var key bytes.Buffer
	fmt.Fprintf(&key, "%06d.%018d", producerId, sequence)

	payload := make([]byte, pw.config.messageSize)

	var r *kgo.Record
	r = kgo.KeySliceRecord(key.Bytes(), payload)
	return r
}

type ProducerWorkerStatus struct {
	// How many messages did we try to transmit?
	Sent int64 `json:"sent"`

	// How many messages did we send successfully (were acked
	// by the server at the offset we expected)?
	Acked int64 `json:"acked"`

	// How many messages landed at an unexpected offset?
	// (indicates retries/resends)
	BadOffsets int64 `json:"acked"`

	// How many times did we restart the producer loop?
	Restarts int64 `json:"restarts"`

	lock sync.Mutex

	// For emitting checkpoints on time intervals
	lastCheckpoint time.Time
}

func NewProducerWorkerStatus() ProducerWorkerStatus {
	return ProducerWorkerStatus{
		lastCheckpoint: time.Now(),
	}
}

func (self *ProducerWorkerStatus) OnAcked() {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.Acked += 1
}

func (self *ProducerWorkerStatus) OnBadOffset() {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.BadOffsets += 1
}

func (pw *ProducerWorker) produceCheckpoint() {
	err := pw.validOffsets.Store()
	util.Chk(err, "Error writing offset map: %v", err)

	data, err := json.Marshal(pw.Status)
	util.Chk(err, "Status serialization error")
	os.Stdout.Write(data)
	os.Stdout.Write([]byte("\n"))
}

func (pw *ProducerWorker) Wait() {

	n := int64(pw.config.messageCount)

	for {
		n_produced, bad_offsets := pw.produceInner(n)
		n = n - n_produced

		if len(bad_offsets) > 0 {
			log.Infof("Produce stopped early, %d still to do", n)
		}

		if n <= 0 {
			return
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

func (pw *ProducerWorker) produceInner(n int64) (int64, []BadOffset) {
	opts := pw.config.workerCfg.MakeKgoOpts()

	opts = append(opts, []kgo.Opt{
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}...)
	client, err := kgo.NewClient(opts...)
	util.Chk(err, "Error creating kafka client")

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

	log.Infof("Producing %d messages (%d bytes)", n, pw.config.messageSize)

	for i := int64(0); i < n && len(bad_offsets) == 0; i = i + 1 {
		concurrent.Acquire(context.Background(), 1)
		produced += 1
		pw.Status.Sent += 1
		var p = rand.Int31n(pw.config.nPartitions)

		expect_offset := nextOffset[p]
		nextOffset[p] += 1

		r := pw.newRecord(0, expect_offset)
		r.Partition = p
		wg.Add(1)

		log.Debugf("Writing partition %d at %d", r.Partition, nextOffset[p])
		handler := func(r *kgo.Record, err error) {
			concurrent.Release(1)
			util.Chk(err, "Produce failed: %v", err)
			if expect_offset != r.Offset {
				log.Warnf("Produced at unexpected offset %d (expected %d) on partition %d", r.Offset, expect_offset, r.Partition)
				pw.Status.OnBadOffset()
				bad_offsets <- BadOffset{r.Partition, r.Offset}
				errored = true
				log.Debugf("errored = %b", errored)
			} else {
				pw.Status.OnAcked()
				pw.validOffsets.Insert(r.Partition, r.Offset)
				log.Debugf("Wrote partition %d at %d", r.Partition, r.Offset)
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

	log.Info("Waiting...")
	wg.Wait()
	log.Info("Waited.")
	wg.Wait()
	close(bad_offsets)

	pw.produceCheckpoint()

	if errored {
		log.Warnf("%d bad offsets", len(bad_offsets))
		var r []BadOffset
		for o := range bad_offsets {
			r = append(r, o)
		}
		if len(r) == 0 {
			util.Die("No bad offsets but errored?")
		}
		successful_produced := produced - int64(len(r))
		return successful_produced, r
	} else {
		wg.Wait()
		return produced, nil
	}
}
