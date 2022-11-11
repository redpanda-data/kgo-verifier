package verifier

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	worker "github.com/redpanda-data/kgo-verifier/pkg/worker"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type GroupReadConfig struct {
	workerCfg    worker.WorkerConfig
	name         string
	nPartitions  int32
	nReaders     int
	maxReadCount int
}

func NewGroupReadConfig(
	wc worker.WorkerConfig, name string, nPartitions int32, nReaders int, maxReadCount int) GroupReadConfig {
	return GroupReadConfig{
		workerCfg:    wc,
		name:         name,
		nPartitions:  nPartitions,
		nReaders:     nReaders,
		maxReadCount: maxReadCount,
	}
}

type GroupWorkerStatus struct {
	Validator ValidatorStatus `json:"validator"`
	Active    bool            `json:"active"`
	Errors    int             `json:"errors"`
}

type GroupReadWorker struct {
	config GroupReadConfig
	Status GroupWorkerStatus
}

func NewGroupReadWorker(cfg GroupReadConfig) GroupReadWorker {
	return GroupReadWorker{
		config: cfg,
		Status: GroupWorkerStatus{},
	}
}

type ConsumerGroupOffsets struct {
	// This is called by one of the readers to signal that we have read all
	// offsets that we intended to.
	cancelFunc context.CancelFunc

	lock sync.Mutex
	// Partition id -> offset last seen by readers
	lastSeen []int64
	// Partition id -> max offset that we intend to read (exclusive)
	upTo []int64
	// number of currently consumed messages
	curReadCount int
	// max number of messages to consume
	maxReadCount int
}

func NewConsumerGroupOffsets(hwms []int64, maxReadCount int, cancelFunc context.CancelFunc) ConsumerGroupOffsets {
	lastSeen := make([]int64, len(hwms))
	upTo := make([]int64, len(hwms))
	copy(upTo, hwms)
	return ConsumerGroupOffsets{
		cancelFunc:   cancelFunc,
		lastSeen:     lastSeen,
		upTo:         upTo,
		maxReadCount: maxReadCount,
	}
}

func (cgs *ConsumerGroupOffsets) AddRecord(r *kgo.Record) {
	cgs.lock.Lock()
	defer cgs.lock.Unlock()

	cgs.curReadCount += 1

	if r.Offset > cgs.lastSeen[r.Partition] {
		cgs.lastSeen[r.Partition] = r.Offset
	}

	if cgs.maxReadCount >= 0 && cgs.curReadCount >= cgs.maxReadCount {
		cgs.cancelFunc()
		return
	}

	if cgs.lastSeen[r.Partition] >= cgs.upTo[r.Partition]-1 {
		allComplete := true
		for p, hwm := range cgs.upTo {
			if cgs.lastSeen[p] < hwm-1 {
				allComplete = false
				break
			}
		}
		if allComplete {
			cgs.cancelFunc()
		}
	}
}

func (grw *GroupReadWorker) Wait() error {
	grw.Status.Active = true
	defer func() { grw.Status.Active = false }()

	client, err := kgo.NewClient(grw.config.workerCfg.MakeKgoOpts()...)
	if err != nil {
		log.Errorf("Error constructing client: %v", err)
		return err
	}

	startOffsets := GetOffsets(client, grw.config.workerCfg.Topic, grw.config.nPartitions, -2)
	hwms := GetOffsets(client, grw.config.workerCfg.Topic, grw.config.nPartitions, -1)
	client.Close()

	hasMessages := false
	for p := 0; p < int(grw.config.nPartitions); p++ {
		if startOffsets[p] < hwms[p] {
			hasMessages = true
			break
		}
	}

	if !hasMessages {
		log.Infof("Topic is empty, exiting...")
		return nil
	}

	groupName := fmt.Sprintf("kgo-verifier-%d-%d", time.Now().Unix(), os.Getpid())
	log.Infof("Reading with consumer group %s", groupName)

	status := NewValidatorStatus()
	ctx, cancelFunc := context.WithCancel(context.Background())
	cgOffsets := NewConsumerGroupOffsets(hwms, grw.config.maxReadCount, cancelFunc)

	var wg sync.WaitGroup
	for i := 0; i < int(grw.config.nReaders); i++ {
		wg.Add(1)
		go func(fiberId int) {
			for {
				err := grw.consumerGroupReadInner(
					ctx, fiberId, groupName, &cgOffsets)
				if err != nil {
					log.Warnf(
						"fiber %v: restarting consumer group reader for error %v",
						fiberId, err)
					// Loop around and retry
				} else {
					log.Infof("fiber %v: consumer group reader finished", fiberId)
					break
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	status.Checkpoint()
	return nil
}

func (grw *GroupReadWorker) consumerGroupReadInner(
	ctx context.Context,
	fiberId int, groupName string,
	cgOffsets *ConsumerGroupOffsets) error {

	opts := grw.config.workerCfg.MakeKgoOpts()
	opts = append(opts, []kgo.Opt{
		kgo.ConsumeTopics(grw.config.workerCfg.Topic),
		kgo.ConsumerGroup(groupName),
	}...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		// Our caller can retry us.
		log.Warnf("Error creating kafka client: %v", err)
		return err
	}
	defer client.Close()

	validRanges := LoadTopicOffsetRanges(grw.config.workerCfg.Topic, grw.config.nPartitions)

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() == context.Canceled {
			break
		} else if ctx.Err() != nil {
			return ctx.Err()
		}

		var r_err error
		fetches.EachError(func(t string, p int32, err error) {
			log.Warnf(
				"fiber %v: Consumer group fetch %s/%d e=%v...",
				fiberId, t, p, err)
			r_err = err
		})

		if r_err != nil {
			return r_err
		}

		fetches.EachRecord(func(r *kgo.Record) {
			log.Debugf(
				"fiber %v: Consumer group read %s/%d o=%d...",
				fiberId, grw.config.workerCfg.Topic, r.Partition, r.Offset)
			grw.Status.Validator.ValidateRecord(r, &validRanges)
			// Will cancel the context if we have read everything
			cgOffsets.AddRecord(r)
		})

		// Offsets will be committed on the next PollFetches invocation
	}

	return nil
}

func (grw *GroupReadWorker) ResetStats() {
	grw.Status = GroupWorkerStatus{}
}

func (grw *GroupReadWorker) GetStatus() interface{} {
	return &grw.Status
}
