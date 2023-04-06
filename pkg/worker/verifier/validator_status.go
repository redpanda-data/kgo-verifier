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
		lastCheckpoint: time.Now(),
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

	// Concurrent access happens when doing random reads
	// with multiple reader fibers
	lock sync.Mutex

	// For emitting checkpoints on time intervals
	lastCheckpoint time.Time
}

func (cs *ValidatorStatus) ValidateRecord(r *kgo.Record, validRanges *TopicOffsetRanges) {
	expect_header_key := fmt.Sprintf("%06d.%018d", 0, r.Offset)
	log.Debugf("Consumed %s on p=%d at o=%d", r.Key, r.Partition, r.Offset)
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if expect_header_key != string(r.Headers[0].Value) {
		shouldBeValid := validRanges.Contains(r.Partition, r.Offset)

		if shouldBeValid {
			cs.InvalidReads += 1
			util.Die("Bad read at offset %d on partition %s/%d.  Expect '%s', found '%s'", r.Offset, r.Topic, r.Partition, expect_header_key, r.Headers[0].Value)
		} else {
			cs.OutOfScopeInvalidReads += 1
			log.Infof("Ignoring read validation at offset outside valid range %s/%d %d", r.Topic, r.Partition, r.Offset)
		}
	} else {
		cs.ValidReads += 1
		log.Debugf("Read OK (%s) on p=%d at o=%d", r.Headers[0].Value, r.Partition, r.Offset)
	}

	if time.Since(cs.lastCheckpoint) > time.Second*5 {
		cs.Checkpoint()
		cs.lastCheckpoint = time.Now()
	}
}

func (cs *ValidatorStatus) Checkpoint() {
	log.Infof("Validator status: %s", cs.String())
}

func (cs *ValidatorStatus) String() string {
	data, err := json.Marshal(cs)
	util.Chk(err, "Status serialization error")
	return string(data)
}
