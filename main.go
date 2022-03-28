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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"golang.org/x/sync/semaphore"
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
	debug        = flag.Bool("debug", false, "Enable verbose logging")
	trace        = flag.Bool("trace", false, "Enable super-verbose (franz-go internals)")
	brokers      = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic        = flag.String("topic", "", "topic to produce to or consume from")
	username     = flag.String("username", "", "SASL username")
	password     = flag.String("password", "", "SASL password")
	mSize        = flag.Int("msg_size", 16384, "Size of messages to produce")
	pCount       = flag.Int("produce_msgs", 1000, "Number of messages to produce")
	cCount       = flag.Int("rand_read_msgs", 10, "Number of validation reads to do")
	seqRead      = flag.Bool("seq_read", true, "Whether to do sequential read validation")
	parallelRead = flag.Int("parallel", 1, "How many readers to run in parallel")
)

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
			Die("Out of order offset %d", o)
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
	PartitionRanges []OffsetRanges
}

func (tors *TopicOffsetRanges) Insert(p int32, o int64) {
	tors.PartitionRanges[p].Insert(o)
}

func (tors *TopicOffsetRanges) Contains(p int32, o int64) bool {
	return tors.PartitionRanges[p].Contains(o)
}

func topicOffsetRangeFile() string {
	return fmt.Sprintf("valid_offsets_%s.json", *topic)
}

func (tors *TopicOffsetRanges) Store() error {
	log.Infof("TopicOffsetRanges::Storing %s...", topicOffsetRangeFile())
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

	err = os.Rename(tmp_file.Name(), topicOffsetRangeFile())
	if err != nil {
		return err
	}

	for p, or := range tors.PartitionRanges {
		log.Debugf("TopicOffsetRanges::Store: %d %d", p, len(or.Ranges))
	}

	return nil
}

func NewTopicOffsetRanges(nPartitions int32) TopicOffsetRanges {
	prs := make([]OffsetRanges, nPartitions)
	for _, or := range prs {
		or.Ranges = make([]OffsetRange, 0)
	}
	return TopicOffsetRanges{
		PartitionRanges: prs,
	}
}

func LoadTopicOffsetRanges(nPartitions int32) TopicOffsetRanges {
	data, err := ioutil.ReadFile(topicOffsetRangeFile())
	if err != nil {
		// Pass, assume it's not existing yet
		return NewTopicOffsetRanges(nPartitions)
	} else {
		var tors TopicOffsetRanges
		if len(data) > 0 {
			err = json.Unmarshal(data, &tors)
			Chk(err, "Bad JSON %v", err)
		}

		if int32(len(tors.PartitionRanges)) > nPartitions {
			Die("More partitions in valid_offsets file than in topic!")
		} else if len(tors.PartitionRanges) < int(nPartitions) {
			// Creating new partitions is allowed
			blanks := make([]OffsetRanges, nPartitions-int32(len(tors.PartitionRanges)))
			tors.PartitionRanges = append(tors.PartitionRanges, blanks...)
		}

		return tors
	}
}

func sequentialRead(nPartitions int32) {
	client := newClient(nil)
	hwm := getOffsets(client, nPartitions, -1)
	lwm := make([]int64, nPartitions)

	status := NewConsumerStatus()
	for {
		var err error
		lwm, err = sequentialReadInner(nPartitions, lwm, hwm, &status)
		if err != nil {
			log.Warnf("Restarting reader for error %v", err)
			// Loop around
		} else {
			status.Checkpoint()
			return
		}
	}
}

type ConsumerStatus struct {
	// How many messages did we try to transmit?
	ValidReads int64

	// How many validation errors (indicating bugs!)
	InvalidReads int64

	// How many validation errors on extents that are not
	// designated as valid by the producer (indicates
	// offsets where retries happened or where unrelated
	// data was written to the topic)
	OutOfScopeInvalidReads int64

	// Concurrent access happens when doing random reads
	// with multiple reader fibers
	lock sync.Mutex

	// For emitting checkpoints on time intervals
	lastCheckpoint time.Time
}

func NewConsumerStatus() ConsumerStatus {
	return ConsumerStatus{
		lastCheckpoint: time.Now(),
	}
}

func sequentialReadInner(nPartitions int32, startAt []int64, upTo []int64, status *ConsumerStatus) ([]int64, error) {
	log.Infof("Sequential read...")

	offsets := make(map[string]map[int32]kgo.Offset)
	partOffsets := make(map[int32]kgo.Offset, nPartitions)
	complete := make([]bool, nPartitions)
	for i, o := range startAt {
		partOffsets[int32(i)] = kgo.NewOffset().At(o)
		log.Infof("Sequential start offset %s/%d %d...", *topic, i, partOffsets[int32(i)])
		if o == upTo[i] {
			complete[i] = true
		}
	}
	offsets[*topic] = partOffsets

	validRanges := LoadTopicOffsetRanges(nPartitions)

	opts := []kgo.Opt{
		kgo.ConsumePartitions(offsets),
	}
	client := newClient(opts)

	last_read := make([]int64, nPartitions)

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
			log.Debugf("Sequential read %s/%d o=%d...", *topic, r.Partition, r.Offset)
			if r.Offset > last_read[r.Partition] {
				last_read[r.Partition] = r.Offset
			}

			if r.Offset >= upTo[r.Partition]-1 {
				complete[r.Partition] = true
			}

			status.ValidateRecord(r, &validRanges)
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

func (cs *ConsumerStatus) ValidateRecord(r *kgo.Record, validRanges *TopicOffsetRanges) {
	expect_key := fmt.Sprintf("%06d.%018d", 0, r.Offset)
	log.Debugf("Consumed %s on p=%d at o=%d", r.Key, r.Partition, r.Offset)
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if expect_key != string(r.Key) {
		shouldBeValid := validRanges.Contains(r.Partition, r.Offset)

		if shouldBeValid {
			cs.InvalidReads += 1
			Die("Bad read at offset %d on partition %s/%d.  Expect '%s', found '%s'", r.Offset, *topic, r.Partition, expect_key, r.Key)
		} else {
			cs.OutOfScopeInvalidReads += 1
			log.Infof("Ignoring read validation at offset outside valid range %s/%d %d", *topic, r.Partition, r.Offset)
		}
	} else {
		cs.ValidReads += 1
		log.Debugf("Read OK (%s) on p=%d at o=%d", r.Key, r.Partition, r.Offset)
	}

	if time.Since(cs.lastCheckpoint) > time.Second*5 {
		cs.Checkpoint()
		cs.lastCheckpoint = time.Now()
	}
}

func (cs *ConsumerStatus) Checkpoint() {
	data, err := json.Marshal(cs)
	Chk(err, "Status serialization error")
	os.Stdout.Write(data)
	os.Stdout.Write([]byte("\n"))
}

func randomRead(tag string, nPartitions int32, status *ConsumerStatus) {
	// Basic client to read offsets
	client := newClient(make([]kgo.Opt, 0))
	endOffsets := getOffsets(client, nPartitions, -1)
	client.Close()
	client = newClient(make([]kgo.Opt, 0))
	startOffsets := getOffsets(client, nPartitions, -2)
	client.Close()
	runtime.GC()

	validRanges := LoadTopicOffsetRanges(nPartitions)

	ctxLog := log.WithFields(log.Fields{"tag": tag})

	readCount := *cCount

	// Select a partition and location
	ctxLog.Infof("Reading %d random offsets", *cCount)

	i := 0
	for i < readCount {
		p := rand.Int31n(nPartitions)
		pStart := startOffsets[p]
		pEnd := endOffsets[p]

		if pEnd-pStart < 2 {
			ctxLog.Warnf("Partition %d is empty, skipping read", p)
			continue
		}
		o := rand.Int63n(pEnd-pStart-1) + pStart
		offset := kgo.NewOffset().At(o)

		// Construct a map of topic->partition->offset to seek our new client to the right place
		offsets := make(map[string]map[int32]kgo.Offset)
		partOffsets := make(map[int32]kgo.Offset, 1)
		partOffsets[p] = offset
		offsets[*topic] = partOffsets

		// Fully-baked client for actual consume
		opts := []kgo.Opt{
			kgo.ConsumePartitions(offsets),
		}

		client = newClient(opts)

		// Read one record
		ctxLog.Debugf("Reading partition %d (%d-%d) at offset %d", p, pStart, pEnd, offset)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		fetches := client.PollRecords(ctx, 1)
		ctxLog.Debugf("Read done for partition %d (%d-%d) at offset %d", p, pStart, pEnd, offset)
		fetches.EachError(func(topic string, partition int32, e error) {
			// In random read mode, we tolerate read errors: if the server is unavailable
			// we will just proceed to read the next random offset.
			ctxLog.Errorf("Error reading from partition %s:%d: %v", topic, partition, e)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Partition != p {
				Die("Wrong partition %d in read at offset %d on partition %s/%d", r.Partition, r.Offset, *topic, p)
			}
			status.ValidateRecord(r, &validRanges)
		})
		if len(fetches.Records()) == 0 {
			ctxLog.Errorf("Empty response reading from partition %d at %d", p, offset)
		} else {
			// Each read on which we get some records counts toward
			// the number of reads we were requested to do.
			i += 1
		}
		fetches = nil

		client.Flush(context.Background())
		client.Close()
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

// Try to get offsets, with a retry loop in case any partitions are not
// in a position to respond.  This is useful to avoid terminating if e.g.
// the cluster is subject to failure injection while workload runs.
func getOffsets(client *kgo.Client, nPartitions int32, t int64) []int64 {
	wait_t := 2 * time.Second
	for {
		result, err := getOffsetsInner(client, nPartitions, t)
		if err != nil {
			log.Warnf("Retrying getOffsets in %v", wait_t)
			time.Sleep(wait_t)
		} else {
			return result
		}

	}
}

func getOffsetsInner(client *kgo.Client, nPartitions int32, t int64) ([]int64, error) {
	log.Infof("Loading offsets for topic %s t=%d...", *topic, t)
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
				log.Warnf("error fetching %s/%d metadata: %v", *topic, partition.Partition, kerr.ErrorForCode(partition.ErrorCode))
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

type ProducerStatus struct {
	// How many messages did we try to transmit?
	Sent int64

	// How many messages did we send successfully (were acked
	// by the server at the offset we expected)?
	Acked int64

	// How many messages landed at an unexpected offset?
	// (indicates retries/resends)
	BadOffsets int64

	// How many times did we restart the producer loop?
	Restarts int64

	lock sync.Mutex

	// For emitting checkpoints on time intervals
	lastCheckpoint time.Time
}

func NewProducerStatus() ProducerStatus {
	return ProducerStatus{
		lastCheckpoint: time.Now(),
	}
}

func (self *ProducerStatus) OnAcked() {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.Acked += 1
}

func (self *ProducerStatus) OnBadOffset() {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.BadOffsets += 1
}

func produceCheckpoint(status *ProducerStatus, validOffsets *TopicOffsetRanges) {
	err := validOffsets.Store()
	Chk(err, "Error writing offset map: %v", err)

	data, err := json.Marshal(status)
	Chk(err, "Status serialization error")
	os.Stdout.Write(data)
	os.Stdout.Write([]byte("\n"))
}

func produce(nPartitions int32) {
	n := int64(*pCount)

	status := NewProducerStatus()
	validOffsets := LoadTopicOffsetRanges(nPartitions)

	for {
		n_produced, bad_offsets := produceInner(n, nPartitions, &validOffsets, &status)
		n = n - n_produced

		if len(bad_offsets) > 0 {
			log.Infof("Produce stopped early, %d still to do", n)
		}

		if n <= 0 {
			return
		} else {
			// Record that we took another run at produceInner
			status.Restarts += 1
		}
	}
}

type BadOffset struct {
	P int32
	O int64
}

func produceInner(n int64, nPartitions int32, validOffsets *TopicOffsetRanges, status *ProducerStatus) (int64, []BadOffset) {
	opts := []kgo.Opt{
		kgo.DefaultProduceTopic(*topic),
		kgo.MaxBufferedRecords(1024),
		kgo.ProducerBatchMaxBytes(1024 * 1024),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}
	client := newClient(opts)

	nextOffset := getOffsets(client, nPartitions, -1)

	for i, o := range nextOffset {
		log.Infof("Produce start offset %s/%d %d...", *topic, i, o)
	}

	var wg sync.WaitGroup

	errored := false
	produced := int64(0)

	// Channel must be >= concurrency
	bad_offsets := make(chan BadOffset, 16384)
	concurrent := semaphore.NewWeighted(4096)

	log.Infof("Producing %d messages (%d bytes)", n, *mSize)

	for i := int64(0); i < n && len(bad_offsets) == 0; i = i + 1 {
		concurrent.Acquire(context.Background(), 1)
		produced += 1
		status.Sent += 1
		var p = rand.Int31n(nPartitions)

		expect_offset := nextOffset[p]
		nextOffset[p] += 1

		r := newRecord(0, expect_offset)
		r.Partition = p
		wg.Add(1)

		log.Debugf("Writing partition %d at %d", r.Partition, nextOffset[p])
		handler := func(r *kgo.Record, err error) {
			concurrent.Release(1)
			Chk(err, "Produce failed!")
			if expect_offset != r.Offset {
				log.Warnf("Produced at unexpected offset %d (expected %d) on partition %d", r.Offset, expect_offset, r.Partition)
				status.OnBadOffset()
				bad_offsets <- BadOffset{r.Partition, r.Offset}
				errored = true
				log.Debugf("errored = %b", errored)
			} else {
				status.OnAcked()
				validOffsets.Insert(r.Partition, r.Offset)
				log.Debugf("Wrote partition %d at %d", r.Partition, r.Offset)
			}
			wg.Done()
		}
		client.Produce(context.Background(), r, handler)

		// Not strictly necessary, but useful if a long running producer gets killed
		// before finishing

		if time.Since(status.lastCheckpoint) > 5*time.Second {
			status.lastCheckpoint = time.Now()
			produceCheckpoint(status, validOffsets)
		}
	}

	log.Info("Waiting...")
	wg.Wait()
	log.Info("Waited.")
	wg.Wait()
	close(bad_offsets)

	produceCheckpoint(status, validOffsets)

	if errored {
		log.Warnf("%d bad offsets", len(bad_offsets))
		var r []BadOffset
		for o := range bad_offsets {
			r = append(r, o)
		}
		if len(r) == 0 {
			Die("No bad offsets but errored?")
		}
		successful_produced := produced - int64(len(r))
		return successful_produced, r
	} else {
		wg.Wait()
		return produced, nil
	}
}

func newClient(opts []kgo.Opt) *kgo.Client {
	// Disable auth if username not given
	if len(*username) > 0 {
		auth_mech := scram.Auth{
			User: *username,
			Pass: *password,
		}
		auth := auth_mech.AsSha256Mechanism()
		opts = append(opts,
			kgo.SASL(auth))
	}

	opts = append(opts,
		kgo.SeedBrokers(strings.Split(*brokers, ",")...))

	if *trace {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}

	client, err := kgo.NewClient(opts...)
	Chk(err, "Error creating kafka client")
	return client
}

func main() {
	flag.Parse()

	if *debug || *trace {
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

	if *pCount > 0 {
		produce(nPartitions)
	}

	if *parallelRead <= 1 {
		if *seqRead {
			sequentialRead(nPartitions)
		}

		if *cCount > 0 {
			status := NewConsumerStatus()
			randomRead("", nPartitions, &status)
			status.Checkpoint()

		}
	} else {
		var wg sync.WaitGroup
		if *seqRead {
			wg.Add(1)
			go func() {
				sequentialRead(nPartitions)
				wg.Done()
			}()
		}

		parallelRandoms := *parallelRead
		if *seqRead {
			parallelRandoms -= 1
		}

		status := NewConsumerStatus()
		if *cCount > 0 {
			for i := 0; i < parallelRandoms; i++ {
				wg.Add(1)
				go func(tag string) {
					randomRead(tag, nPartitions, &status)
					wg.Done()
				}(fmt.Sprintf("%03d", i))
			}
		}

		wg.Wait()
		status.Checkpoint()
	}
}
