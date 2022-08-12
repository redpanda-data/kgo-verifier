// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"sync"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/kgo-verifier/pkg/worker"
	"github.com/redpanda-data/kgo-verifier/pkg/worker/verifier"
)

var (
	debug              = flag.Bool("debug", false, "Enable verbose logging")
	trace              = flag.Bool("trace", false, "Enable super-verbose (franz-go internals)")
	brokers            = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic              = flag.String("topic", "", "topic to produce to or consume from")
	username           = flag.String("username", "", "SASL username")
	password           = flag.String("password", "", "SASL password")
	mSize              = flag.Int("msg_size", 16384, "Size of messages to produce")
	pCount             = flag.Int("produce_msgs", 1000, "Number of messages to produce")
	cCount             = flag.Int("rand_read_msgs", 10, "Number of validation reads to do")
	seqRead            = flag.Bool("seq_read", true, "Whether to do sequential read validation")
	parallelRead       = flag.Int("parallel", 1, "How many readers to run in parallel")
	batchMaxBytes      = flag.Int("batch_max_bytes", 1048576, "the maximum batch size to allow per-partition (must be less than Kafka's max.message.bytes, producing)")
	cgReaders          = flag.Int("consumer_group_readers", 0, "Number of parallel readers in the consumer group")
	linger             = flag.Duration("linger", 0, "if non-zero, linger to use when producing")
	maxBufferedRecords = flag.Uint("max-buffered-records", 1024, "Producer buffer size: the default of 1 is makes roughly one event per batch, useful for measurement.  Set to something higher to make it easier to max out bandwidth.")
)

func makeWorkerConfig() worker.WorkerConfig {
	c := worker.WorkerConfig{
		Brokers:            *brokers,
		Trace:              *trace,
		Topic:              *topic,
		Linger:             *linger,
		MaxBufferedRecords: *maxBufferedRecords,
		BatchMaxbytes:      uint(*batchMaxBytes),
		SaslUser:           *username,
		SaslPass:           *password,
	}

	return c
}

func main() {
	flag.Parse()

	if *debug || *trace {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.Info("Getting topic metadata...")
	conf := makeWorkerConfig()
	opts := conf.MakeKgoOpts()
	client, err := kgo.NewClient(opts...)
	util.Chk(err, "Error creating kafka client")

	var t kmsg.MetadataResponseTopic
	{
		req := kmsg.NewPtrMetadataRequest()
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(*topic)
		req.Topics = append(req.Topics, reqTopic)

		resp, err := req.RequestWith(context.Background(), client)
		util.Chk(err, "unable to request topic metadata: %v", err)
		if len(resp.Topics) != 1 {
			util.Die("metadata response returned %d topics when we asked for 1", len(resp.Topics))
		}
		t = resp.Topics[0]
		if t.ErrorCode != 0 {
			util.Die("Error %s getting topic metadata", kerr.ErrorForCode(t.ErrorCode))
		}
	}

	nPartitions := int32(len(t.Partitions))
	log.Debugf("Targeting topic %s with %d partitions", *topic, nPartitions)

	if *pCount > 0 {
		pwc := verifier.NewProducerConfig(makeWorkerConfig(), "producer", nPartitions, *mSize, *cCount)
		pw := verifier.NewProducerWorker(pwc)
		pw.Wait()
	}

	if *parallelRead <= 1 {
		if *seqRead {
			srw := verifier.NewSeqReadWorker(verifier.NewSeqReadConfig(
				makeWorkerConfig(), "sequential", nPartitions,
			))
			srw.Wait()
		}

		if *cCount > 0 {

			workerCfg := verifier.NewRandomReadConfig(
				makeWorkerConfig(), "random", nPartitions,
			)

			worker := verifier.NewRandomReadWorker(workerCfg)
			worker.Wait()
			worker.Status.Validator.Checkpoint()
		}
	} else {
		var wg sync.WaitGroup
		if *seqRead {
			wg.Add(1)
			go func() {
				srw := verifier.NewSeqReadWorker(verifier.NewSeqReadConfig(
					makeWorkerConfig(), "sequential", nPartitions,
				))
				srw.Wait()
				wg.Done()
			}()
		}

		parallelRandoms := *parallelRead
		if *seqRead {
			parallelRandoms -= 1
		}

		if *cCount > 0 {
			for i := 0; i < parallelRandoms; i++ {
				wg.Add(1)
				go func(tag string) {
					workerCfg := verifier.NewRandomReadConfig(
						makeWorkerConfig(), "random", nPartitions,
					)
					worker := verifier.NewRandomReadWorker(workerCfg)
					worker.Wait()
					worker.Status.Validator.Checkpoint()
					wg.Done()
				}(fmt.Sprintf("%03d", i))
			}
		}

		wg.Wait()
	}

	if *cgReaders > 0 {
		grw := verifier.NewGroupReadWorker(verifier.NewGroupReadConfig(makeWorkerConfig(), "groupReader", nPartitions, *cgReaders))
		grw.Wait()
	}
}
