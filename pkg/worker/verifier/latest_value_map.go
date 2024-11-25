package verifier

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
	log "github.com/sirupsen/logrus"
)

type LatestValueMap struct {
	topic               string
	LatestKvByPartition []map[string]string
}

func (lvm *LatestValueMap) Get(partition int32, key string) (value string, exists bool) {
	if partition < 0 || partition >= int32(len(lvm.LatestKvByPartition)) {
		log.Panicf("Partition %d out of bounds for latestValueMap of size %d", partition, len(lvm.LatestKvByPartition))
	}
	value, exists = lvm.LatestKvByPartition[partition][key]
	return
}

func (lvm *LatestValueMap) Insert(partition int32, key string, value string) {
	lvm.LatestKvByPartition[partition][key] = value
}

func latestValueMapFile(topic string) string {
	return fmt.Sprintf("latest_value_%s.json", topic)
}

func (lvm *LatestValueMap) Store() error {
	log.Infof("LatestValueMap::Storing %s", latestValueMapFile(lvm.topic))

	data, err := json.Marshal(lvm)
	if err != nil {
		return err
	}

	tmp_file, err := ioutil.TempFile("./", "latest_value_*.tmp")
	if err != nil {
		return err
	}

	_, err = tmp_file.Write(data)
	if err != nil {
		return err
	}

	err = os.Rename(tmp_file.Name(), latestValueMapFile(lvm.topic))
	if err != nil {
		return err
	}

	return nil
}

func LoadLatestValues(topic string, nPartitions int32) LatestValueMap {
	data, err := ioutil.ReadFile(latestValueMapFile(topic))
	if err != nil {
		util.Die("Can't read topic latest value map: %v", err)
	}

	var lvm LatestValueMap
	if len(data) > 0 {
		err = json.Unmarshal(data, &lvm)
		util.Chk(err, "Bad JSON %v", err)
	}

	if int32(len(lvm.LatestKvByPartition)) > nPartitions {
		util.Die("More partitions in latest_value_map file than in topic!")
	} else if len(lvm.LatestKvByPartition) < int(nPartitions) {
		// Creating new partitions is allowed
		blanks := make([]map[string]string, nPartitions-int32(len(lvm.LatestKvByPartition)))
		lvm.LatestKvByPartition = append(lvm.LatestKvByPartition, blanks...)
	}
	log.Infof("Successfully read latest value map")
	return lvm
}

func NewLatestValueMap(topic string, nPartitions int32) LatestValueMap {
	maps := make([]map[string]string, nPartitions)
	for i := range maps {
		maps[i] = make(map[string]string)
	}
	return LatestValueMap{
		topic:               topic,
		LatestKvByPartition: maps,
	}
}
