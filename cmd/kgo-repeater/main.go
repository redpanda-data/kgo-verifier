package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
	worker "github.com/redpanda-data/kgo-verifier/pkg/worker"
	repeater "github.com/redpanda-data/kgo-verifier/pkg/worker/repeater"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	debug              = flag.Bool("debug", false, "Enable verbose logging")
	trace              = flag.Bool("trace", false, "Enable ultra-verbose client logging")
	username           = flag.String("username", "", "SASL username")
	password           = flag.String("password", "", "SASL password")
	enableTls          = flag.Bool("enable-tls", false, "Enable use of TLS")
	brokers            = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic              = flag.String("topic", "", "topic to produce to or consume from")
	linger             = flag.Duration("linger", 0, "if non-zero, linger to use when producing")
	maxBufferedRecords = flag.Uint("max-buffered-records", 1, "Producer buffer size: the default of 1 is makes roughly one event per batch, useful for measurement.  Set to something higher to make it easier to max out bandwidth.")
	group              = flag.String("group", "", "consumer group")
	workers            = flag.Uint("workers", 1, "How many to run in this process")
	keys               = flag.Uint64("keys", 0, "How many unique keys to use, or 0 for full 64 bit space")
	payloadSize        = flag.Uint64("payload-size", 16384, "Message payload size in bytes")
	initialDataMb      = flag.Uint64("initial-data-mb", 4, "Initial target data in flight in megabytes")
	remote             = flag.Bool("remote", false, "Operate in remote-controlled mode")
	remotePort         = flag.Uint("remote-port", 7884, "Port for report control HTTP listener")
	profile            = flag.String("profile", "", "Enable CPU profiling")
	rateLimitBps       = flag.Int("rate-limit-bps", -1, "Bytes/second throttle (global, will be split equally between workers)")

	useTransactions      = flag.Bool("use-transactions", false, "Producer: use a transactional producer")
	transactionAbortRate = flag.Float64("transaction-abort-rate", 0.0, "The probability that any given transaction should abort")
	msgsPerTransaction   = flag.Uint("msgs-per-transaction", 1, "The number of messages that should be in a given transaction")

	compressionType     = flag.String("compression-type", "", "One of gzip, snappy, lz4, zstd, or 'mixed' to pick a random codec for each producer")
	compressiblePayload = flag.Bool("compressible-payload", false, "If true, use a highly compressible payload instead of the default random payload")
)

// NewAdmin returns a franz-go admin client.
func NewAdmin() (*kadm.Client, error) {

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
	}

	if len(*username) > 0 {
		auth_mech := scram.Auth{
			User: *username,
			Pass: *password,
		}
		auth := auth_mech.AsSha256Mechanism()
		opts = append(opts, kgo.SASL(auth))
	}

	if *enableTls {
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}
	kgoClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	adm := kadm.NewClient(kgoClient)
	adm.SetTimeoutMillis(5000) // 5s timeout default for any timeout based request
	return adm, nil
}

func main() {
	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	wConfig := worker.NewWorkerConfig(
		"kgo", *brokers, *trace, *topic, *linger, *maxBufferedRecords, *useTransactions, *compressionType, *compressiblePayload, *username, *password, *enableTls)
	opts := wConfig.MakeKgoOpts()
	opts = append(opts, []kgo.Opt{
		kgo.ProducerBatchMaxBytes(1024 * 1024),
	}...)

	dataInFlightPerWorker := (*initialDataMb * 1024 * 1024) / uint64(*workers)

	if dataInFlightPerWorker / *payloadSize <= 0 {
		panic("-initial-data-mb is too small for the configured payload size & worker count")
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	var verifiers []*repeater.Worker

	hostName, err := os.Hostname()
	util.Chk(err, "Error getting hostname %v", err)

	pid := os.Getpid()

	var rateLimitPerWorker int = -1
	if *rateLimitBps > 0 {
		rateLimitPerWorker = *rateLimitBps / int(*workers)
	}

	log.Infof("Preparing %d workers...", *workers)
	for i := uint(0); i < *workers; i++ {
		name := fmt.Sprintf("%s_%d_w_%d", hostName, pid, i)
		log.Debugf("Preparing worker %s...", name)
		wConfig := worker.NewWorkerConfig(
			name, *brokers, *trace, *topic, *linger, *maxBufferedRecords, *useTransactions, *compressionType, *compressiblePayload, *username, *password, *enableTls)
		config := repeater.NewRepeaterConfig(wConfig, *group, *keys, *payloadSize, dataInFlightPerWorker, rateLimitPerWorker)
		lv := repeater.NewWorker(config)
		if *useTransactions {
			tconfig := worker.NewTransactionSTMConfig(*transactionAbortRate, *msgsPerTransaction)
			lv.EnableTransactions(tconfig)
		}
		lv.Prepare()
		verifiers = append(verifiers, &lv)
	}

	do_shutdown := func() {
		for _, v := range verifiers {
			(*v).Stop()
		}
		for i, v := range verifiers {
			log.Infof("Waiting for worker %d...", i)
			result := (*v).Wait()
			log.Infof("Waiting for worker %d complete", i)
			log.Infof("Verifier %d result: %s", i, result.String())
		}
	}

	// Even if we're not in remote mode, start the HTTP listener so
	// that it's convenient to e.g. fetch status
	activate_c := make(chan int, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/activate", func(w http.ResponseWriter, r *http.Request) {
		activate_c <- 1

		w.WriteHeader(http.StatusOK)
		w.Write(make([]byte, 0))
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		var results []repeater.WorkerStatus
		for _, v := range verifiers {
			results = append(results, v.Status())
		}

		serialized, err := json.MarshalIndent(results, "", "  ")
		util.Chk(err, "Status serialization error")

		w.WriteHeader(http.StatusOK)
		w.Write(serialized)
	})

	mux.HandleFunc("/print_stack", func(w http.ResponseWriter, r *http.Request) {
		log.Infof("Printing stack on remote request:")
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 2)
	})

	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		for _, v := range verifiers {
			v.Reset()
		}
		w.WriteHeader(http.StatusOK)
		w.Write(make([]byte, 0))
	})

	go http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *remotePort), mux)

	if !*remote {
		admin, err := NewAdmin()
		util.Chk(err, "Failed to set up admin client: %v", err)
		defer admin.Close()

		// Single process mode: wait for the consumer group to be in a
		// ready state before we start producing.
		retries := 10
		for {
			var groups []string
			groups = append(groups, *group)
			describedGroups, err := admin.DescribeGroups(context.Background(), groups...)
			if err != nil {
				// We retry on describeGroups error because we might be running against
				// a newly started cluster that isn't ready to serve yet.
				if retries <= 0 {
					util.Die("failed to describe consumer group: %v", err)
				} else {
					log.Infof("Retrying on DescribeGroups error %v", err)
					time.Sleep(1000 * time.Millisecond)
					retries -= 1
				}
			}

			described := describedGroups[*group]
			if described.State == "Stable" {
				break
			} else {
				log.Infof("Group not ready yet, state=%s", described.State)
				time.Sleep(5000 * time.Millisecond)
			}

			if len(c) > 0 {
				log.Info("Stopping on signal...")
				do_shutdown()
				return
			}
		}

	} else {
		// External coordinator should wait for all nodes
		// to finish Prepare, wait for consumer group to
		// stabilize (optional) and then kick us to
		// proceed.
		log.Info("Waiting for remote activate request")
		select {
		case <-c:
			log.Info("Stopping on signal...")
			do_shutdown()
			return
		case <-activate_c:
			log.Info("Remote requested activate, proceeding")
		}
	}

	log.Infof("Activating %d workers", len(verifiers))
	for i, v := range verifiers {
		log.Debugf("Activating worker %d...", i)
		v.Activate()
	}

	select {
	case <-c:
		log.Info("Stopping on signal...")
	}
	do_shutdown()
}
