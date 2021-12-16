package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func Die(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	log.Error(formatted)
	os.Exit(1)
}

func Chk(err error, msg string, args ...interface{}) {
	if err != nil {
		Die(msg, args...)
	}
}

var (
	debug    = flag.Bool("debug", false, "Enable verbose logging")
	brokers  = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic    = flag.String("topic", "", "topic to produce to or consume from")
	username = flag.String("username", "", "SASL username")
	password = flag.String("password", "", "SASL password")
	mSize    = flag.Int("msg_size", 16384, "Size of messages to produce")
	pCount   = flag.Int("produce_msgs", 100000, "Number of messages to produce")
	cCount   = flag.Int("consume_msgs", 100, "Number of validation reads to do")
)

func randomRead(nPartitions int32) {

	// Basic client to read offsets
	client := newClient(make([]kgo.Opt, 0))
	startOffsets := getOffsets(client, nPartitions, -2)
	endOffsets := getOffsets(client, nPartitions, -1)

	// Select a partition and location
	log.Infof("Reading %d random offsets", *cCount)
	for i := 0; i < *cCount; i++ {
		p := rand.Int31n(nPartitions)
		pStart := startOffsets[p]
		pEnd := endOffsets[p]

		if pStart == pEnd {
			log.Warnf("Partition %d is empty, skipping read", p)
			continue
		}
		o := rand.Int63n(pEnd-pStart) + pStart
		offset := kgo.NewOffset().At(o)
		log.Debugf("Read partition %d (%d-%d) at offset %d", p, pStart, pEnd, offset)

		// Fully-baked client for actual consume
		opts := []kgo.Opt{
			kgo.MaxBufferedRecords(4096),
			kgo.ProducerBatchMaxBytes(1024 * 1024),
			kgo.ProducerBatchCompression(kgo.NoCompression()),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.ConsumeResetOffset(offset),
			kgo.ConsumeTopics(*topic),
		}

		client = newClient(opts)

		// Read one record
		fetches := client.PollRecords(context.Background(), 1)
		for _, f := range fetches {
			for _, t := range f.Topics {
				for _, p := range t.Partitions {
					Chk(p.Err, "Consume error on partition")
					for _, r := range p.Records {
						expect_key := fmt.Sprintf("%06d.%018d", 0, r.Offset)
						log.Debugf("Consumed %s on p=%d at o=%d", r.Key, r.Partition, r.Offset)
						if expect_key != string(r.Key) {
							Die("Bad read at offset %d on partition %s/%d.  Expect '%s', found '%s'", r.Offset, *topic, r.Partition, expect_key, r.Key)
						} else {
							log.Infof("Read OK (%s) on p=%d at o=%d", r.Key, r.Partition, r.Offset)

						}
					}

				}
			}
		}
	}

}

func newRecord(producerId int, sequence int64) *kgo.Record {
	var key bytes.Buffer
	fmt.Fprintf(&key, "%06d.%018d", producerId, sequence)

	payload := make([]byte, *mSize)

	var r *kgo.Record
	r = kgo.KeySliceRecord(key.Bytes(), payload)
	return r
}

func getOffsets(client *kgo.Client, nPartitions int32, t int64) []int64 {
	pOffsets := make([]int64, nPartitions)

	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	reqTopic := kmsg.NewListOffsetsRequestTopic()
	reqTopic.Topic = *topic
	for i := 0; i < int(nPartitions); i++ {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = int32(i)
		part.Timestamp = t
		reqTopic.Partitions = append(reqTopic.Partitions, part)
	}

	req.Topics = append(req.Topics, reqTopic)

	resp, err := req.RequestWith(context.Background(), client)
	Chk(err, "unable to request partition metadata: %v", err)
	for _, partition := range resp.Topics[0].Partitions {
		if partition.ErrorCode != 0 {
			Die("Error %s getting partition %d metadata", kerr.ErrorForCode(partition.ErrorCode), partition.Partition)
		}
		pOffsets[partition.Partition] = partition.Offset
		log.Debugf("Partition %d offset %d", partition.Partition, pOffsets[partition.Partition])
	}

	return pOffsets
}

func produce(nPartitions int32) {
	opts := []kgo.Opt{
		kgo.DefaultProduceTopic(*topic),
		kgo.MaxBufferedRecords(4096),
		kgo.ProducerBatchMaxBytes(1024 * 1024),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}
	client := newClient(opts)

	nextOffset := getOffsets(client, nPartitions, -1)

	var wg sync.WaitGroup

	log.Infof("Producing %d messages (%d bytes)", *pCount, *mSize)
	for i := 0; i < *pCount; i = i + 1 {
		var p = rand.Int31n(nPartitions)
		r := newRecord(0, nextOffset[p])
		r.Partition = p
		wg.Add(1)

		log.Debugf("Writing partition %d at %d", r.Partition, nextOffset[p])
		handler := func(r *kgo.Record, err error) {
			wg.Done()
			Chk(err, "Oh no!")
			log.Debugf("Wrote partition %d at %d", r.Partition, r.Offset)
		}
		client.Produce(context.Background(), r, handler)
		nextOffset[p] += 1
	}
	client.Flush(context.Background())

	wg.Wait()
}

func newClient(opts []kgo.Opt) *kgo.Client {
	auth_mech := scram.Auth{
		User: *username,
		Pass: *password,
	}

	auth := auth_mech.AsSha256Mechanism()

	opts = append(opts,
		kgo.SASL(auth))
	opts = append(opts,
		kgo.SeedBrokers(strings.Split(*brokers, ",")...))

	client, err := kgo.NewClient(opts...)
	Chk(err, "Error creating kafka client")
	return client
}

func main() {
	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.Info("Getting topic metadata...")
	client := newClient(make([]kgo.Opt, 0))
	var t kmsg.MetadataResponseTopic
	{
		req := kmsg.NewPtrMetadataRequest()
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(*topic)
		req.Topics = append(req.Topics, reqTopic)

		resp, err := req.RequestWith(context.Background(), client)
		Chk(err, "unable to request topic metadata: %v", err)
		if len(resp.Topics) != 1 {
			Die("metadata response returned %d topics when we asked for 1", len(resp.Topics))
		}
		t = resp.Topics[0]
		if t.ErrorCode != 0 {
			Die("Error %s getting topic metadata", kerr.ErrorForCode(t.ErrorCode))
		}
	}

	nPartitions := int32(len(t.Partitions))
	log.Debugf("Targeting topic %s with %d partitions", *topic, nPartitions)

	// Prepare: write out several segments
	produce(nPartitions)

	// Main stage: continue to write, while running random reader in background
	randomRead(nPartitions)

}
