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
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/kgo-verifier/pkg/worker"
	"github.com/redpanda-data/kgo-verifier/pkg/worker/verifier"
)

var (
	debug               = flag.Bool("debug", false, "Enable verbose logging")
	trace               = flag.Bool("trace", false, "Enable super-verbose (franz-go internals)")
	brokers             = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic               = flag.String("topic", "", "topic to produce to or consume from")
	username            = flag.String("username", "", "SASL username")
	password            = flag.String("password", "", "SASL password")
	enableTls           = flag.Bool("enable-tls", false, "Enables use of TLS")
	mSize               = flag.Int("msg_size", 16384, "Size of messages to produce")
	pCount              = flag.Int("produce_msgs", 0, "Number of messages to produce")
	cCount              = flag.Int("rand_read_msgs", 0, "Number of validation reads to do from each random reader")
	seqRead             = flag.Bool("seq_read", false, "Whether to do sequential read validation")
	parallelRead        = flag.Int("parallel", 1, "How many readers to run in parallel")
	seqConsumeCount     = flag.Int("seq_read_msgs", -1, "Seq/group consumer: set max number of records to consume")
	batchMaxBytes       = flag.Int("batch_max_bytes", 1048576, "the maximum batch size to allow per-partition (must be less than Kafka's max.message.bytes, producing)")
	cgReaders           = flag.Int("consumer_group_readers", 0, "Number of parallel readers in the consumer group")
	linger              = flag.Duration("linger", 0, "if non-zero, linger to use when producing")
	maxBufferedRecords  = flag.Uint("max-buffered-records", 1024, "Producer buffer size: the default of 1 is makes roughly one event per batch, useful for measurement.  Set to something higher to make it easier to max out bandwidth.")
	remote              = flag.Bool("remote", false, "Remote control mode, driven by HTTP calls, for use in automated tests")
	remotePort          = flag.Uint("remote-port", 7884, "HTTP listen port for remote control/query")
	loop                = flag.Bool("loop", false, "For readers, run indefinitely until stopped via signal or HTTP call")
	name                = flag.String("client-name", "kgo", "Name of kafka client")
	fakeTimestampMs     = flag.Int64("fake-timestamp-ms", -1, "Producer: set artificial batch timestamps on an incrementing basis, starting from this number")
	consumeTputMb       = flag.Int("consume-throughput-mb", -1, "Seq/group consumer: set max throughput in mb/s")
	produceRateLimitBps = flag.Int("produce-throughput-bps", -1, "Producer: set max throughput in bytes/s")
	keySetCardinality   = flag.Int("key-set-cardinality", -1, "Cardinality of a set of possible record keys (makes data compactible)")

	useTransactions      = flag.Bool("use-transactions", false, "Producer: use a transactional producer")
	transactionAbortRate = flag.Float64("transaction-abort-rate", 0.0, "The probability that any given transaction should abort")
	msgsPerTransaction   = flag.Uint("msgs-per-transaction", 1, "The number of messages that should be in a given transaction")

	compressionType     = flag.String("compression-type", "", "One of none, gzip, snappy, lz4, zstd, or 'mixed' to pick a random codec for each producer")
	compressiblePayload = flag.Bool("compressible-payload", false, "If true, use a highly compressible payload instead of the default random payload")
)

func makeWorkerConfig() worker.WorkerConfig {
	c := worker.WorkerConfig{
		Brokers:             *brokers,
		Trace:               *trace,
		Topic:               *topic,
		Linger:              *linger,
		MaxBufferedRecords:  *maxBufferedRecords,
		BatchMaxbytes:       uint(*batchMaxBytes),
		SaslUser:            *username,
		SaslPass:            *password,
		UseTls:              *enableTls,
		Name:                *name,
		Transactions:        *useTransactions,
		CompressionType:     *compressionType,
		CompressiblePayload: *compressiblePayload,
	}

	return c
}

func main() {
	flag.Parse()

	if *topic == "" {
		util.Die("No topic specified (use -topic)")
	}

	if *debug || *trace {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	signalChan := make(chan os.Signal, 1)
	if *remote {
		signal.Notify(signalChan, os.Interrupt)
	}

	// Once we are done, keep the process alive until this channel is fired
	shutdownChan := make(chan int, 1)

	// For sequential consumers, keep re-reading the topic from start until
	// this channel is fired.
	lastPassChan := make(chan int, 1)

	log.Info("Getting topic metadata...")
	conf := makeWorkerConfig()
	opts := conf.MakeKgoOpts()
	client, err := kgo.NewClient(opts...)
	util.Chk(err, "Error creating kafka client: %v", err)

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

	var workers []worker.Worker

	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		var results []interface{}
		var locks []*sync.Mutex
		for _, v := range workers {
			status, lock := v.GetStatus()
			results = append(results, status)
			locks = append(locks, lock)
		}

		for _, lock := range locks {
			lock.Lock()
		}

		serialized, err := json.MarshalIndent(results, "", "  ")

		for _, lock := range locks {
			lock.Unlock()
		}

		util.Chk(err, "Status serialization error")

		w.WriteHeader(http.StatusOK)
		w.Write(serialized)
	})

	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		log.Info("Remote request /reset")
		for _, v := range workers {
			v.ResetStats()
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		log.Info("Remote request /shutdown")
		shutdownChan <- 1
	})

	mux.HandleFunc("/last_pass", func(w http.ResponseWriter, r *http.Request) {
		log.Info("Remote request /last_pass")
		timeout := r.URL.Query().Get("timeout")
		if len(timeout) > 0 {
			timeoutSec, err := strconv.Atoi(timeout)
			if err == nil {
				log.Infof("Setting a timeout of %v seconds to print the stack trace", timeoutSec)
				go func() {
					time.Sleep(time.Duration(timeoutSec) * time.Second)
					pprof.Lookup("goroutine").WriteTo(os.Stdout, 2)
				}()
			} else {
				log.Warn("unable to parse timeout query param, skipping printing stack trace logs")
			}
		}
		lastPassChan <- 1
	})

	mux.HandleFunc("/print_stack", func(w http.ResponseWriter, r *http.Request) {
		log.Infof("Printing stack on remote request:")
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	})

	go http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *remotePort), mux)

	if *pCount > 0 {
		log.Info("Starting producer...")
		pwc := verifier.NewProducerConfig(makeWorkerConfig(), "producer", nPartitions, *mSize, *pCount, *fakeTimestampMs, (*produceRateLimitBps), *keySetCardinality)
		pw := verifier.NewProducerWorker(pwc)

		if *useTransactions {
			tconfig := worker.NewTransactionSTMConfig(*transactionAbortRate, *msgsPerTransaction)
			pw.EnableTransactions(tconfig)
		}

		workers = append(workers, &pw)
		waitErr := pw.Wait()
		util.Chk(err, "Producer error: %v", waitErr)
		log.Info("Finished producer.")
	}

	if *seqRead {
		srw := verifier.NewSeqReadWorker(verifier.NewSeqReadConfig(
			makeWorkerConfig(), "sequential", nPartitions, *seqConsumeCount,
			(*consumeTputMb)*1024*1024,
		))
		workers = append(workers, &srw)

		firstPass := true
		for firstPass || (len(lastPassChan) == 0 && *loop) {
			log.Info("Starting sequential read pass")
			firstPass = false
			waitErr := srw.Wait()
			if waitErr != nil {
				// Proceed around the loop, to be tolerant of e.g. kafka client
				// construct failures on unavailable cluster
				log.Warnf("Error from sequeqntial read worker: %v", err)
			}
		}
	}

	if *cCount > 0 {
		var wg sync.WaitGroup
		var randomWorkers []*verifier.RandomReadWorker
		for i := 0; i < *parallelRead; i++ {
			workerCfg := verifier.NewRandomReadConfig(
				makeWorkerConfig(), fmt.Sprintf("random-%03d", i), nPartitions, *cCount,
			)
			worker := verifier.NewRandomReadWorker(workerCfg)
			randomWorkers = append(randomWorkers, &worker)
			workers = append(workers, &worker)
		}

		firstPass := true
		for firstPass || (len(lastPassChan) == 0 && *loop) {
			firstPass = false
			for _, w := range randomWorkers {
				wg.Add(1)
				go func(worker *verifier.RandomReadWorker) {
					waitErr := worker.Wait()
					if waitErr != nil {
						// Proceed around the loop, to be tolerant of e.g. kafka client
						// construct failures on unavailable cluster
						log.Warnf("Error from random worker: %v", err)
					}
					worker.Status.Validator.Checkpoint()
					wg.Done()
				}(w)
			}
			wg.Wait()
		}
	}

	if *cgReaders > 0 {
		grw := verifier.NewGroupReadWorker(
			verifier.NewGroupReadConfig(
				makeWorkerConfig(), "groupReader", nPartitions, *cgReaders,
				*seqConsumeCount, (*consumeTputMb)*1024*1024))
		workers = append(workers, &grw)

		firstPass := true
		for firstPass || (len(lastPassChan) == 0 && *loop) {
			log.Info("Starting group read pass")
			firstPass = false
			waitErr := grw.Wait()
			util.Chk(waitErr, "Consumer error: %v", err)
		}
	}

	if *remote {
		log.Info("Waiting for remote shutdown request")
		select {
		case <-signalChan:
			log.Info("Stopping on signal...")
			return
		case <-shutdownChan:
			log.Info("Remote requested shutdown, proceeding")
		}
	}

}
