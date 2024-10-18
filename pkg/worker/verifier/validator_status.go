package verifier

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewValidatorStatus() ValidatorStatus {
	return ValidatorStatus{
		MaxOffsetsConsumed: make(map[int32]int64),
		lastCheckpoint:     time.Now(),
	}
}

type ValidatorStatus struct {
	// To help human beings reading logs, not functionally necessary.
	Name string `json:"name"`

	// How many messages did we try to transmit?
	ValidReads int64 `json:"valid_reads"`

	// How many validation errors (indicating bugs!)
	InvalidReads int64 `json:"invalid_reads"`

	// How many validation errors on extents that are not
	// designated as valid by the producer (indicates
	// offsets where retries happened or where unrelated
	// data was written to the topic)
	OutOfScopeInvalidReads int64 `json:"out_of_scope_invalid_reads"`

	// The highest valid offset consumed throughout the consumer's lifetime
	MaxOffsetsConsumed map[int32]int64 `json:"max_offsets_consumed"`

	LostOffsets map[int32]int64 `json:"lost_offsets"`

	// Concurrent access happens when doing random reads
	// with multiple reader fibers
	lock sync.Mutex

	// For emitting checkpoints on time intervals
	lastCheckpoint time.Time

	// Last consumed offset per partition. Used to assert monotonicity and check for gaps.
	lastOffsetConsumed map[int32]int64

	// Last leader epoch per partition. Used to assert monotonicity.
	lastLeaderEpoch map[int32]int32
}

func (cs *ValidatorStatus) ValidateRecord(r *kgo.Record, validRanges *TopicOffsetRanges) {
	expect_header_value := fmt.Sprintf("%06d.%018d", 0, r.Offset)
	log.Debugf("Consumed %s on p=%d at o=%d leaderEpoch=%d", r.Key, r.Partition, r.Offset, r.LeaderEpoch)
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if cs.lastLeaderEpoch[r.Partition] < r.LeaderEpoch {
		log.Fatalf("Out of order leader epoch on p=%d at o=%d leaderEpoch=%d. Previous leaderEpoch=%d",
			r.Partition, r.Offset, r.LeaderEpoch, cs.lastLeaderEpoch[r.Partition])
	}

	var got_header_value string
	if len(r.Headers) > 0 {
		got_header_value = string(r.Headers[0].Value)
	}

	if expect_header_value != got_header_value {
		shouldBeValid := validRanges.Contains(r.Partition, r.Offset)

		if shouldBeValid {
			cs.InvalidReads += 1
			util.Die("Bad read at offset %d on partition %s/%d.  Expect '%s', found '%s'", r.Offset, r.Topic, r.Partition, expect_header_value, got_header_value)
		} else {
			cs.OutOfScopeInvalidReads += 1
			log.Infof("Ignoring read validation at offset outside valid range %s/%d %d", r.Topic, r.Partition, r.Offset)
		}
	} else {
		currentMax, present := cs.lastOffsetConsumed[r.Partition]
		if present {
			if currentMax < r.Offset {
				expected := currentMax + 1
				if r.Offset != expected {
					log.Warnf("Gap detected in consumed offsets. Expected %d, but got %d", expected, r.Offset)
				}
			} else {
				log.Fatalf("Out of order read. Max consumed offset(partition=%d)=%d; Current record offset=%d", r.Partition, currentMax, r.Offset)
			}
		}
		cs.recordOffset(r)

		cs.ValidReads += 1
		log.Debugf("Read OK (%s) on p=%d at o=%d", r.Headers[0].Value, r.Partition, r.Offset)
	}

	if time.Since(cs.lastCheckpoint) > time.Second*5 {
		cs.Checkpoint()
		cs.lastCheckpoint = time.Now()
	}
}

func (cs *ValidatorStatus) recordOffset(r *kgo.Record) {
	if cs.MaxOffsetsConsumed == nil {
		cs.MaxOffsetsConsumed = make(map[int32]int64)
	}
	if cs.lastOffsetConsumed == nil {
		cs.lastOffsetConsumed = make(map[int32]int64)
	}
	if cs.lastLeaderEpoch == nil {
		cs.lastLeaderEpoch = make(map[int32]int32)
	}

	if r.Offset > cs.MaxOffsetsConsumed[r.Partition] {
		cs.MaxOffsetsConsumed[r.Partition] = r.Offset
	}

	cs.lastOffsetConsumed[r.Partition] = r.Offset
	cs.lastLeaderEpoch[r.Partition] = r.LeaderEpoch
}

func (cs *ValidatorStatus) RecordLostOffsets(p int32, count int64) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if cs.LostOffsets == nil {
		cs.LostOffsets = make(map[int32]int64)
	}

	cs.LostOffsets[p] += count
}

func (cs *ValidatorStatus) ResetMonotonicityTestState() {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	cs.lastOffsetConsumed = make(map[int32]int64)
}

func (cs *ValidatorStatus) SetMonotonicityTestStateForPartition(partition int32, offset int64) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if cs.lastOffsetConsumed == nil {
		cs.lastOffsetConsumed = make(map[int32]int64)
	}

	cs.lastOffsetConsumed[partition] = offset
}

func (cs *ValidatorStatus) Checkpoint() {
	log.Infof("Validator status: %s", cs.String())
}

func (cs *ValidatorStatus) String() string {
	data, err := json.Marshal(cs)
	util.Chk(err, "Status serialization error")
	return string(data)
}
