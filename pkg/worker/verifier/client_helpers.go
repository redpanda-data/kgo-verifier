package verifier

import (
	"context"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
)

// Try to get offsets, with a retry loop in case any partitions are not
// in a position to respond.  This is useful to avoid terminating if e.g.
// the cluster is subject to failure injection while workload runs.
func GetOffsets(client *kgo.Client, topic string, nPartitions int32, t int64) []int64 {
	wait_t := 2 * time.Second
	for {
		result, err := getOffsetsInner(client, topic, nPartitions, t)
		if err != nil {
			log.Warnf("Retrying getOffsets in %v", wait_t)
			time.Sleep(wait_t)
		} else {
			return result
		}

	}
}

func getOffsetsInner(client *kgo.Client, topic string, nPartitions int32, t int64) ([]int64, error) {
	log.Infof("Loading offsets for topic %s t=%d...", topic, t)
	pOffsets := make([]int64, nPartitions)

	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	reqTopic := kmsg.NewListOffsetsRequestTopic()
	reqTopic.Topic = topic
	for i := 0; i < int(nPartitions); i++ {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = int32(i)
		part.Timestamp = t
		reqTopic.Partitions = append(reqTopic.Partitions, part)
	}

	req.Topics = append(req.Topics, reqTopic)

	seenPartitions := int32(0)
	shards := client.RequestSharded(context.Background(), req)
	var r_err error
	allFailed := kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		if shard.Err != nil {
			r_err = shard.Err
			return
		}
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, partition := range resp.Topics[0].Partitions {
			if partition.ErrorCode != 0 {
				log.Warnf("error fetching %s/%d metadata: %v", topic, partition.Partition, kerr.ErrorForCode(partition.ErrorCode))
				r_err = kerr.ErrorForCode(partition.ErrorCode)
			}
			pOffsets[partition.Partition] = partition.Offset
			seenPartitions += 1
			log.Debugf("Partition %d offset %d", partition.Partition, pOffsets[partition.Partition])
		}
	})

	if allFailed {
		return nil, errors.New("All offset requests failed")
	}

	if seenPartitions < nPartitions {
		// The results may be partial, simply omitting some partitions while not
		// raising any error.  We transform this into an error to avoid wrongly
		// returning a 0 offset for any missing partitions
		return nil, errors.New("Didn't get data for all partitions")
	}

	return pOffsets, r_err
}
