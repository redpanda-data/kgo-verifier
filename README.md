


# Purpose

Stress test redpanda with concurrent random reads, a particularly important
case for validating tiered storage (shadow indexing) where random reads
tend to lead to lots of cache promotion/demotion.

The tool is meant to be chaos-tolerant, i.e. it should not drop out when brokers become
unavailable or cannot respond to requests, and it should continue to be able to validate
its own output if that output was written in bad conditions (e.g. with producer retries).

Additionally verify content of reads, to check for offset translation issues:
the key of each message includes the offset where the producer expects it to land.
The producer keeps track of which messages were successfully committed at the expected
offset, and writes it out to a file.  Consumers then consult this file while reading,
to check whether a particular offset is expected to contain a valid message (i.e. key
matches offset) or not.

# Usage

- Brokers must not use TLS (in BYOC that means run this script inside your k8s cluster
  and refer to brokers by pod IP)

### 1. Quick produce+consume smoke test: produce and then consume in the same process

    si-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 10000 --rand_read_msgs 10 --seq_read=1


### 2. A long running producer

Run exactly one of these at a time, it writes out
a valid_offsets_{topic}.json file, so multiple concurrent producers would 
interfere with one another

    si-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 10000 --rand_read_msgs 0 --seq_read=0

### 3. A sequential consumer.

Run one of these inside a while loop to continuously stream
the whole content of the topic.

    si-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 0 --rand_read_msgs 0 --seq_read=1 


### 4. A parallel random consumer
The --parallel flag says how many read fibers to run concurently

    si-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 0 --rand_read_msgs 10 --seq_read=0 --parallel 4

### 5. A *very* parallel random consumer
aims to emit so many concurrent reads
that the shadow index cache may violate its size bounds (e.g. do 64 concurrent
reads of 1GB segments, when the cache size limit is only 50GB).
Keep rand_read_msgs at 1 to constrain memory usage.

    si-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 0 --rand_read_msgs 1 --seq_read=0 --parallel 64

``` 