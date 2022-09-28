package verifier

import (
	"context"

	worker "github.com/redpanda-data/kgo-verifier/pkg/worker"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type SeqReadConfig struct {
	workerCfg   worker.WorkerConfig
	name        string
	nPartitions int32
}

func NewSeqReadConfig(wc worker.WorkerConfig, name string, nPartitions int32) SeqReadConfig {
	return SeqReadConfig{
		workerCfg:   wc,
		name:        name,
		nPartitions: nPartitions,
	}
}

type SeqWorkerStatus struct {
	Validator ValidatorStatus `json:"validator"`
	Active    bool            `json:"active"`
	Errors    int             `json:"errors"`
}

type SeqReadWorker struct {
	config SeqReadConfig
	Status SeqWorkerStatus
}

func NewSeqReadWorker(cfg SeqReadConfig) SeqReadWorker {
	return SeqReadWorker{
		config: cfg,
		Status: SeqWorkerStatus{},
	}
}

func (srw *SeqReadWorker) Wait() error {
	srw.Status.Active = true
	defer func() { srw.Status.Active = false }()

	client, err := kgo.NewClient(srw.config.workerCfg.MakeKgoOpts()...)
	if err != nil {
		log.Errorf("Error constructing client: %v", err)
		return err
	}

	hwm := GetOffsets(client, srw.config.workerCfg.Topic, srw.config.nPartitions, -1)
	lwm := make([]int64, srw.config.nPartitions)

	for {
		var err error
		lwm, err = srw.sequentialReadInner(lwm, hwm)
		if err != nil {
			log.Warnf("Restarting reader for error %v", err)
			// Loop around
		} else {
			return nil
		}
	}
}

func (srw *SeqReadWorker) sequentialReadInner(startAt []int64, upTo []int64) ([]int64, error) {
	log.Infof("Sequential read start offsets: %v", startAt)
	log.Infof("Sequential read end offsets: %v", upTo)

	offsets := make(map[string]map[int32]kgo.Offset)
	partOffsets := make(map[int32]kgo.Offset, srw.config.nPartitions)
	complete := make([]bool, srw.config.nPartitions)
	for i, o := range startAt {
		partOffsets[int32(i)] = kgo.NewOffset().At(o)
		log.Infof("Sequential start offset %s/%d  %#v...", srw.config.workerCfg.Topic, i, partOffsets[int32(i)])
		if o == upTo[i] {
			complete[i] = true
		}
	}
	offsets[srw.config.workerCfg.Topic] = partOffsets

	validRanges := LoadTopicOffsetRanges(srw.config.workerCfg.Topic, srw.config.nPartitions)

	opts := srw.config.workerCfg.MakeKgoOpts()
	opts = append(opts, []kgo.Opt{
		kgo.ConsumePartitions(offsets),
	}...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Errorf("Error creating Kafka client: %v", err)
		return nil, err
	}

	last_read := make([]int64, srw.config.nPartitions)

	for {
		log.Debugf("Calling PollFetches (last_read=%v status %s)", last_read, srw.Status.Validator.String())
		fetches := client.PollFetches(context.Background())
		log.Debugf("PollFetches returned %d fetches", len(fetches))

		var r_err error
		fetches.EachError(func(t string, p int32, err error) {
			log.Warnf("Sequential fetch %s/%d e=%v...", t, p, err)
			r_err = err
		})

		if r_err != nil {
			// This is not fatal: server is allowed to return an error, the loop outside
			// this function will try again, picking up from last_read.
			log.Warnf("Returning on fetch error %v, read up to %v", r_err, last_read)
			return last_read, r_err
		}

		fetches.EachRecord(func(r *kgo.Record) {
			log.Debugf("Sequential read %s/%d o=%d...", srw.config.workerCfg.Topic, r.Partition, r.Offset)
			if r.Offset > last_read[r.Partition] {
				last_read[r.Partition] = r.Offset
			}

			if r.Offset >= upTo[r.Partition]-1 {
				complete[r.Partition] = true
			}

			srw.Status.Validator.ValidateRecord(r, &validRanges)
		})

		any_incomplete := false
		for _, c := range complete {
			if !c {
				any_incomplete = true
			}

		}

		if !any_incomplete {
			break
		}
	}

	log.Infof("Sequential read complete up to %v (validator status %v)", last_read, srw.Status.Validator.String())

	return last_read, nil
}

func (srw *SeqReadWorker) ResetStats() {
	srw.Status = SeqWorkerStatus{}
}

func (srw *SeqReadWorker) GetStatus() interface{} {
	return &srw.Status
}
