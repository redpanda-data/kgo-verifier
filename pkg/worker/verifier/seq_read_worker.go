package verifier

import (
	"context"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
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

func (srw *SeqReadWorker) Wait() {
	client, err := kgo.NewClient(srw.config.workerCfg.MakeKgoOpts()...)
	util.Chk(err, "Error creating kafka client")

	hwm := GetOffsets(client, srw.config.workerCfg.Topic, srw.config.nPartitions, -1)
	lwm := make([]int64, srw.config.nPartitions)

	status := NewValidatorStatus()
	for {
		var err error
		lwm, err = srw.sequentialReadInner(lwm, hwm)
		if err != nil {
			log.Warnf("Restarting reader for error %v", err)
			// Loop around
		} else {
			status.Checkpoint()
			return
		}
	}
}

func (srw *SeqReadWorker) sequentialReadInner(startAt []int64, upTo []int64) ([]int64, error) {
	log.Infof("Sequential read...")

	offsets := make(map[string]map[int32]kgo.Offset)
	partOffsets := make(map[int32]kgo.Offset, srw.config.nPartitions)
	complete := make([]bool, srw.config.nPartitions)
	for i, o := range startAt {
		partOffsets[int32(i)] = kgo.NewOffset().At(o)
		log.Infof("Sequential start offset %s/%d %d...", srw.config.workerCfg.Topic, i, partOffsets[int32(i)])
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
	util.Chk(err, "Error creating kafka client")

	last_read := make([]int64, srw.config.nPartitions)

	for {
		fetches := client.PollFetches(context.Background())

		var r_err error
		fetches.EachError(func(t string, p int32, err error) {
			log.Debugf("Sequential fetch %s/%d e=%v...", t, p, err)
			r_err = err
		})

		if r_err != nil {
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

	return last_read, nil
}

func (srw *SeqReadWorker) ResetStats() {
	srw.Status = SeqWorkerStatus{}
}

func (srw *SeqReadWorker) GetStatus() interface{} {
	return &srw.Status
}
