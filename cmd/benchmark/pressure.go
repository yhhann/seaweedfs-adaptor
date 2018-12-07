package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"io"
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
	prom "github.com/prometheus/client_golang/prometheus"

	jinmetrics "jingoal.com/letsgo/metrics"
	jintm "jingoal.com/letsgo/time"
	"jingoal.com/seaweedfs-adaptor/utils"
	. "jingoal.com/seaweedfs-adaptor/weedfs"
)

const (
	logMaxSize uint64 = 1024 * 1024 * 32

	promNamespace = "pressure"
	promSubsys    = "benchmark"
	labelModule   = "storage" // Labels for metrics.
	labelOp       = "upload"
)

type pressEnvConf struct {
	addrs       string
	replication string
	dataCenter  string
	rack        string
}

type pressDegreeConf struct {
	tps              int
	inputFile        string
	numWorkers       int
	startTps         int
	tpsStep          int
	latencyThreshold float64
}

type TPS struct {
	lock  *sync.Mutex
	count int64
	start time.Time
}

func (tps *TPS) IncQueryCount() {
	tps.lock.Lock()
	defer tps.lock.Unlock()
	tps.count++
}

func (tps *TPS) Reset() {
	tps.lock.Lock()
	defer tps.lock.Unlock()
	tps.count = 0
	tps.start = time.Now()
}

func (tps *TPS) GetTPS() float64 {
	tps.lock.Lock()
	defer tps.lock.Unlock()
	return float64(tps.count) / (jintm.MillisecondSince(tps.start) / 1000.0)
}

type Latency struct {
	lock       *sync.Mutex
	count      int64
	avgLatency float64
}

func (la *Latency) AddLatency(latency float64) {
	la.lock.Lock()
	defer la.lock.Unlock()

	la.avgLatency = (la.avgLatency*float64(la.count) + latency) / float64(la.count+1)
	la.count++
}

func (la *Latency) GetLatency() float64 {
	la.lock.Lock()
	defer la.lock.Unlock()
	return la.avgLatency
}

func (la *Latency) Reset() {
	la.lock.Lock()
	defer la.lock.Unlock()
	la.count = 0
	la.avgLatency = 0
}

var (
	addrs         = flag.String("addrs", "localhost:9333", "seaweedfs master addrs")
	replicaPolicy = flag.String("replication", "000", "volume replica copy policy")
	dc            = flag.String("dc", "", "")
	rack          = flag.String("rack", "", "")

	inputFile        = flag.String("file", "", "")
	numWorkers       = flag.Int("num-workers", 100, "Total number of workers")
	startTps         = flag.Int("start-tps", 1000, "Start QPS")
	tpsStep          = flag.Int("tps-step", 100, "QPS ramping up step size")
	latencyThreshold = flag.Float64("latency-threshold", 150, "latency threshold to exit the pressure test")

	scrapePort = flag.Int("debug-scrape-port", 30001, "The Prometheus port (for debugging).")

	envConf    *pressEnvConf
	degreeConf *pressDegreeConf

	reqCh     = make(chan struct{}, 50000)
	la        = Latency{lock: &sync.Mutex{}, count: 0, avgLatency: 0}
	actualTps = TPS{lock: &sync.Mutex{}, count: 0, start: time.Now()}

	transactionCounts = prom.NewCounterVec(
		prom.CounterOpts{
			Namespace: promNamespace,
			Subsystem: promSubsys,
			Name:      "transaction_counts",
			Help:      "mock pressure transaction counts.",
		},
		[]string{labelModule},
	)
	durationHistograms = prom.NewHistogramVec(
		prom.HistogramOpts{
			Namespace: promNamespace,
			Subsystem: promSubsys,
			Name:      "upload_durations",
			Help:      "upload file latency distribution in milliseconds.",
			Buckets:   prom.ExponentialBuckets(200, 1.2, 40),
		},
		[]string{labelModule},
	)
)

func init() {
	prom.MustRegister(transactionCounts)
	prom.MustRegister(durationHistograms)
}

func pressure() {
	flag.Parse()
	glog.MaxSize = logMaxSize
	defer glog.Flush()

	go jinmetrics.StartPrometheusServer(*scrapePort)

	envConf = &pressEnvConf{
		addrs:       *addrs,
		replication: *replicaPolicy,
		dataCenter:  *dc,
		rack:        *rack,
	}
	degreeConf = &pressDegreeConf{
		inputFile:        *inputFile,
		numWorkers:       *numWorkers,
		startTps:         *startTps,
		tpsStep:          *tpsStep,
		latencyThreshold: *latencyThreshold,
	}
	glog.V(2).Infof("storage benchmark env %v degree %v\n", envConf, degreeConf)

	go uploadScreen()

	select {}
}

func reset() {
	actualTps.Reset()
	la.Reset()
}

func uploadScreen() {
	// producer
	go func() {
		testStart := time.Now()
		tps := degreeConf.startTps
		for {
			for range time.Tick(time.Second) {
				latency := la.GetLatency()
				glog.V(2).Infof("TPS: %.f, Latency: %dms, qLen: %v\n",
					actualTps.GetTPS(),
					int(latency),
					len(reqCh))

				if latency > degreeConf.latencyThreshold {
					glog.V(2).Info("latency quit.")
					os.Exit(3)
				}

				for i := 0; i < tps; i++ {
					reqCh <- struct{}{}
				}

				if jintm.MillisecondSince(testStart) > float64(5*60*1000) {
					tps += degreeConf.tpsStep
					glog.V(2).Infof("tps step rise up tps %d", tps)
					testStart = time.Now()
					reset()
				}
			}
		}
	}()

	// consumer
	for i := 1; i <= degreeConf.numWorkers; i++ {
		go func() {
			for {
				<-reqCh
				actualTps.IncQueryCount()
				uploadFile()
			}
		}()
	}
}

func uploadFile() {
	start := time.Now()
	// step1: open file
	reader, err := os.Open(degreeConf.inputFile)
	if err != nil {
		glog.V(3).Infof("Failed to open: %v\n", err)
	}
	defer reader.Close()

	// step2: create WeedFile
	cfile, err := Create(reader.Name(), 0, envConf.addrs, envConf.replication, envConf.dataCenter, envConf.rack, 0)
	glog.V(2).Infof("cfile: %v\n", cfile)
	if err != nil {
		glog.Errorf("Failed to create: %v\n", err)
	}
	// step3: upload file to seaweedfs
	md5Buf := make([]byte, MAX_CHUNK_SIZE)
	origMd5, _, err := copyBuffer(cfile, reader, md5Buf)
	if err != nil {
		glog.Errorf("Failed to upload: %v\n", err)
	}
	// step4: close weedfile
	if cfile != nil {
		err = cfile.Close()
		if err != nil {
			glog.Errorf("Failed to close: %v\n", err)
		}
	}
	durationHistograms.WithLabelValues(labelOp).Observe(jintm.MillisecondSince(start))

	// step5: open&read weedfile
	oFile, err := Open(cfile.Fid, 0, envConf.addrs)
	if err != nil {
		glog.Errorf("Failed to open&read: %v\n", err)
	}
	// step6: compare md5
	if oFile != nil {
		defer oFile.Close()

		downMd5, _, err := utils.Md5Reader(oFile)
		if err != nil {
			glog.Errorf("Failed to md5 read: %v\n", err)
		}
		if downMd5 == origMd5 {
			glog.V(2).Info("MD5 equal.\n")
		} else {
			glog.Warningf("MD5 not equal.\n")
		}
	}
	la.AddLatency(jintm.MillisecondSince(start))
	transactionCounts.WithLabelValues(labelOp).Inc()
	glog.V(2).Infof("upload elapse %.f\n", jintm.MillisecondSince(start))
}

func copyBuffer(dst io.Writer, src io.Reader, buf []byte) (fileMd5 string, written int64, err error) {
	md5 := md5.New()
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			md5.Write(buf[0:nr])

			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	fileMd5 = hex.EncodeToString(md5.Sum(nil))
	return fileMd5, written, err
}
