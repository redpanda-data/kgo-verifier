package verifier

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
	log "github.com/sirupsen/logrus"
)

func LoadTopicOffsetRanges(topic string, nPartitions int32) TopicOffsetRanges {
	data, err := ioutil.ReadFile(topicOffsetRangeFile(topic))
	if err != nil {
		// Pass, assume it's not existing yet
		return NewTopicOffsetRanges(topic, nPartitions)
	} else {
		var tors TopicOffsetRanges
		if len(data) > 0 {
			err = json.Unmarshal(data, &tors)
			util.Chk(err, "Bad JSON %v", err)
		}

		if int32(len(tors.PartitionRanges)) > nPartitions {
			util.Die("More partitions in valid_offsets file than in topic!")
		} else if len(tors.PartitionRanges) < int(nPartitions) {
			// Creating new partitions is allowed
			blanks := make([]OffsetRanges, nPartitions-int32(len(tors.PartitionRanges)))
			tors.PartitionRanges = append(tors.PartitionRanges, blanks...)
		}

		return tors
	}
}

type OffsetRange struct {
	Lower int64 // Inclusive
	Upper int64 // Exclusive
}

type OffsetRanges struct {
	Ranges []OffsetRange
}

func (ors *OffsetRanges) Insert(o int64) {
	// Normal case: this is the next offset after the current range in flight

	if len(ors.Ranges) == 0 {
		ors.Ranges = append(ors.Ranges, OffsetRange{Lower: o, Upper: o + 1})
		return
	}

	last := &ors.Ranges[len(ors.Ranges)-1]
	if o >= last.Lower && o == last.Upper {
		last.Upper += 1
		return
	} else {
		if o < last.Upper {
			// TODO: more flexible structure for out of order inserts, at the moment
			// we rely on franz-go callbacks being invoked in order.
			util.Die("Out of order offset %d (vs %d %d)", o, last.Lower, last.Upper)
		} else {
			ors.Ranges = append(ors.Ranges, OffsetRange{Lower: o, Upper: o + 1})
		}
	}
}

func (ors *OffsetRanges) Contains(o int64) bool {
	for _, r := range ors.Ranges {
		if o >= r.Lower && o < r.Upper {
			return true
		}
	}

	return false
}

type TopicOffsetRanges struct {
	topic           string
	PartitionRanges []OffsetRanges
}

func (tors *TopicOffsetRanges) Insert(p int32, o int64) {
	tors.PartitionRanges[p].Insert(o)
}

func (tors *TopicOffsetRanges) Contains(p int32, o int64) bool {
	return tors.PartitionRanges[p].Contains(o)
}

func topicOffsetRangeFile(topic string) string {
	return fmt.Sprintf("valid_offsets_%s.json", topic)
}

func (tors *TopicOffsetRanges) Store() error {
	log.Infof("TopicOffsetRanges::Storing %s...", topicOffsetRangeFile(tors.topic))
	data, err := json.Marshal(tors)
	if err != nil {
		return err
	}

	tmp_file, err := ioutil.TempFile("./", "valid_offsets_*.tmp")
	if err != nil {
		return err
	}

	_, err = tmp_file.Write(data)
	if err != nil {
		return err
	}

	err = os.Rename(tmp_file.Name(), topicOffsetRangeFile(tors.topic))
	if err != nil {
		return err
	}

	for p, or := range tors.PartitionRanges {
		log.Debugf("TopicOffsetRanges::Store: %d %d", p, len(or.Ranges))
	}

	return nil
}

func NewTopicOffsetRanges(topic string, nPartitions int32) TopicOffsetRanges {
	prs := make([]OffsetRanges, nPartitions)
	for _, or := range prs {
		or.Ranges = make([]OffsetRange, 0)
	}
	return TopicOffsetRanges{
		topic:           topic,
		PartitionRanges: prs,
	}
}
