package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"

	"jingoal.com/seaweedfs-adaptor/cmd/instrument"
	"jingoal.com/seaweedfs-adaptor/utils"
	"jingoal.com/seaweedfs-adaptor/weedfs"
)

type BenchmarkOptions struct {
	seeds         string
	replication   string
	dataCenter    string
	rack          string
	concurrency   int
	numberOfFiles int
	domain        int64
	fileSize      int
	idListFile    string
	write         bool
	read          bool
	remove        bool
}

var (
	b BenchmarkOptions

	wait       sync.WaitGroup
	writeStats *instrument.Stats
	readStats  *instrument.Stats
)

func init() {
	flag.StringVar(&b.seeds, "seeds", "localhost:9333", "SeaweedFS master seeds location")
	flag.StringVar(&b.replication, "replication", "000", "replication strategy type")
	flag.StringVar(&b.dataCenter, "dataCenter", "", "current volume server's data center name")
	flag.StringVar(&b.rack, "rack", "", "current volume server's rack name")
	flag.IntVar(&b.concurrency, "c", 16, "number of concurrent write or read processes")
	flag.IntVar(&b.numberOfFiles, "n", 100, "number of files to write")
	flag.Int64Var(&b.domain, "domain", 1, "corpration ID?")
	flag.IntVar(&b.fileSize, "size", 1024, "simulated file size in bytes, with random(0~63) bytes padding")
	flag.StringVar(&b.idListFile, "id-file", os.TempDir()+"/benchmark_list.txt", "list of uploaded file ids")
	flag.BoolVar(&b.write, "write", true, "enable write")
	flag.BoolVar(&b.read, "read", true, "enable read")
	flag.BoolVar(&b.remove, "remove", true, "enable remove")
}

const (
	GLOG_MAXSIZE uint64 = 1024 * 1024 * 32
)

func main() {
	flag.Parse()
	// MaxSize is the maximum size of a log file in bytes.
	glog.MaxSize = GLOG_MAXSIZE
	rand.Seed(time.Now().UnixNano())
	defer glog.Flush()

	if b.write {
		benchWrite()
	}
	if b.read {
		benchRead()
	}

	if b.remove {
		benchRemove()
	}
}

func benchWrite() {
	fileIdLineChan := make(chan string, b.concurrency)
	finishChan := make(chan bool)
	idChan := make(chan int)
	writeStats = instrument.NewStats(b.concurrency)
	writeStats.Start = time.Now()
	writeStats.Total = b.numberOfFiles

	go writeFileIds(b.idListFile, fileIdLineChan, finishChan)
	go writeStats.CheckProgress(finishChan, &wait, "Start writing benchmark...")

	for i := 0; i < b.concurrency; i++ {
		wait.Add(1)
		go writeFiles(idChan, fileIdLineChan, &writeStats.LocalStats[i])
	}
	for i := 0; i < b.numberOfFiles; i++ {
		idChan <- i
	}
	close(idChan)
	wait.Wait() // wait writeFiles

	wait.Add(2)
	finishChan <- true
	finishChan <- true
	wait.Wait()
	close(finishChan)
	close(fileIdLineChan)
	writeStats.End = time.Now()
	writeStats.PrintStats(b.concurrency, "Writed benchmark complete.")
}

func benchRead() {
	fileIdLineChan := make(chan string, b.concurrency)
	finishChan := make(chan bool)
	readStats = instrument.NewStats(b.concurrency)
	readStats.Start = time.Now()
	readStats.Total = b.numberOfFiles

	go readFileIds(b.idListFile, fileIdLineChan)
	go readStats.CheckProgress(finishChan, &wait, "Start reading benchmark...")

	for i := 0; i < b.concurrency; i++ {
		wait.Add(1)
		go readFiles(fileIdLineChan, &readStats.LocalStats[i])
	}
	wait.Wait() // wait readFiles

	wait.Add(1)
	finishChan <- true
	wait.Wait()
	close(finishChan)
	readStats.End = time.Now()
	readStats.PrintStats(b.concurrency, "Readed benchmark complete.")
}

func benchRemove() {
	fileIdLineChan := make(chan string, b.concurrency)
	finishChan := make(chan bool)

	go readFileIds(b.idListFile, fileIdLineChan)
	go instrument.PrintProgress(finishChan, &wait, "Start removeing benchmark...", "Removed benchmark complete.")
	for i := 0; i < b.concurrency; i++ {
		wait.Add(1)
		go removeFiles(fileIdLineChan)
	}
	wait.Wait() // wait removeFiles

	wait.Add(1)
	finishChan <- true
	wait.Wait()
	close(finishChan)

	if err := os.Remove(b.idListFile); err != nil {
		glog.Warningf("Failed to remove fid file: %v", err)
	}
}

func writeFiles(idChan chan int, fileIdLineChan chan string, s *instrument.Stat) {
	defer wait.Done()

	for id := range idChan {
		start := time.Now()
		fileSize := int64(b.fileSize + rand.Intn(64))
		reader := utils.NewFakeReader(uint64(id), 0, fileSize)
		origMd5, _, err := utils.Md5Reader(reader)
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to md5: %v", err)
			continue
		}
		reader.Seek(0, 0)

		cfile, err := weedfs.Create("", b.domain, b.seeds, b.replication, b.dataCenter, b.rack, 0)
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to create: %v", err)
			continue
		}

		written, err := io.Copy(cfile, reader)
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to copy %s: %v", cfile.Fid, err)
			continue
		}

		err = cfile.Close()
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to close %s: %v", cfile.Fid, err)
			continue
		}
		fileIdLineChan <- fmt.Sprintf("%s|%s", cfile.Fid, origMd5)

		s.Completed++
		s.Transferred += written
		writeStats.AddSample(time.Now().Sub(start))

		glog.V(4).Infof("Successed to write file, FID: %s", cfile.Fid)
	}
}

func parseLine(line string) (fid, md5 string, err error) {
	pipeIndex := strings.Index(line, "|")
	if pipeIndex <= 0 {
		return "", "", fmt.Errorf("wrong line format: %s", line)
	}
	return line[:pipeIndex], line[pipeIndex+1:], nil
}

func readFiles(fileIdLineChan chan string, s *instrument.Stat) {
	defer wait.Done()
	for line := range fileIdLineChan {
		if len(line) == 0 {
			continue
		}
		if line[0] == '#' {
			continue
		}
		fid, md5, err := parseLine(line)
		if err != nil {
			glog.Warningf("Failed to parse %s: %v", line, err)
			continue
		}

		glog.V(4).Infof("Reading file, FID: %s", fid)
		start := time.Now()
		oFile, err := weedfs.Open(fid, b.domain, b.seeds)
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to open %s: %v", fid, err)
			continue
		}
		defer oFile.Close()

		downMd5, written, err := utils.Md5Reader(oFile)
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to copy %s: %v", fid, err)
			continue
		}

		if downMd5 != md5 {
			s.Failed++
			glog.Warningf("MD5 not equal, expect md5 %s, but %s", md5, downMd5)
			continue
		}
		s.Completed++
		s.Transferred += written
		readStats.AddSample(time.Now().Sub(start))
	}
}

func removeFiles(fileIdLineChan chan string) {
	defer wait.Done()
	for line := range fileIdLineChan {
		if len(line) == 0 {
			continue
		}
		if line[0] == '#' {
			continue
		}
		fid, _, err := parseLine(line)
		if err != nil {
			glog.Warningf("Failed to parse %s: %v", line, err)
			continue
		}

		delSucc, err := weedfs.Remove(fid, b.domain, b.seeds)
		if err != nil {
			glog.Warningf("Failed to remove %s: %v", fid, err)
			continue
		}

		glog.V(4).Infof("Remove fid %s, success %t", fid, delSucc)
	}
}

func writeFileIds(fileName string, fileIdLineChan chan string, finishChan chan bool) {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		glog.Warningf("Failed to create file %s: %v", fileName, err)
	}
	defer file.Close()

	for {
		select {
		case <-finishChan:
			wait.Done()
			return
		case line := <-fileIdLineChan:
			file.Write([]byte(line))
			file.Write([]byte("\n"))
		}
	}
}

func readFileIds(fileName string, fileIdLineChan chan string) {
	file, err := os.Open(fileName) // For read access.
	if err != nil {
		glog.Warningf("Failed to read file %s: %v", fileName, err)
	}
	defer file.Close()

	r := bufio.NewReader(file)
	for {
		if line, err := utils.Readln(r); err == nil {
			fileIdLineChan <- string(line)
		} else {
			break
		}
	}
	close(fileIdLineChan)
}
