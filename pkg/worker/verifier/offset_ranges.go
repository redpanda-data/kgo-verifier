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
		log.Warnf("Can't read topic offset ranges: %v", err)
		return NewTopicOffsetRanges(topic, nPartitions)
	} else {
		tors := TopicOffsetRanges{
			topic: topic,
		}
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

	TolerateDataLoss bool
}

func (ors *OffsetRanges) Insert(o int64) {
	// Normal case: this is the next offset after the current range in flight

	if len(ors.Ranges) == 0 {
		ors.Ranges = append(ors.Ranges, OffsetRange{Lower: o, Upper: o + 1})
		return
	}

	{
		last := ors.Ranges[len(ors.Ranges)-1]

		// Handle out of order inserts.
		if o < last.Upper {
			if ors.TolerateDataLoss {
				// Truncate the ranges to the last offset.
				for i, r := range ors.Ranges {
					if o >= r.Lower && o < r.Upper {
						// If the offset is within the range, truncate the range
						// and remove all subsequent ranges.
						ors.Ranges = ors.Ranges[:i+1]
						ors.Ranges[i].Upper = o
						break
					} else if o < r.Lower {
						// If the offset is before the range, truncate the range and all subsequent ranges.
						ors.Ranges = ors.Ranges[:i]
						break
					}
				}
			} else {
				// TODO: more flexible structure for out of order inserts, at the moment
				// we rely on franz-go callbacks being invoked in order.
				panic(fmt.Sprintf("Out of order offset %d (last range: %d-%d)", o, last.Lower, last.Upper))
			}
		}
	}

	last := &ors.Ranges[len(ors.Ranges)-1]
	if o >= last.Lower && o == last.Upper {
		// Extend the last range if the offset is the next one.
		last.Upper += 1
	} else {
		// Otherwise, create a new range.
		ors.Ranges = append(ors.Ranges, OffsetRange{Lower: o, Upper: o + 1})
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
