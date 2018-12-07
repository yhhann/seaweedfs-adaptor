package main

import (
	"bufio"
	"flag"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"

	"jingoal.com/seaweedfs-adaptor/cmd/instrument"
	"jingoal.com/seaweedfs-adaptor/utils"
)

type BenchmarkOptions struct {
	concurrency      int
	numberOfFiles    int
	domain           int64
	fileSize         int
	idListFile       string
	write            bool
	read             bool
	maxCpu           int
	baseDir          string
	deletePercentage int
}

var (
	b BenchmarkOptions

	wait       sync.WaitGroup
	writeStats *instrument.Stats
	readStats  *instrument.Stats
)

func init() {
	flag.IntVar(&b.concurrency, "c", 16, "number of concurrent write or read processes")
	flag.IntVar(&b.numberOfFiles, "n", 1024, "number of files to write")
	flag.Int64Var(&b.domain, "domain", 1, "corpration ID?")
	flag.IntVar(&b.fileSize, "size", 1024, "simulated file size in bytes, with random(0~63) bytes padding")
	flag.StringVar(&b.idListFile, "list", os.TempDir()+"/tmp/local_list.txt", "list of write file ids")
	flag.BoolVar(&b.write, "write", true, "enable write")
	flag.BoolVar(&b.read, "read", true, "enable read")
	flag.StringVar(&b.baseDir, "base-dir", os.TempDir()+"/tmp", "local file store base dir")
	flag.IntVar(&b.deletePercentage, "delete-percent", 0, "the percent of writes that are deletes")
}

const (
	GLOG_MAXSIZE uint64 = 1024 * 1024 * 32
)

func main() {
	flag.Parse()
	glog.MaxSize = GLOG_MAXSIZE
	rand.Seed(time.Now().UnixNano())
	defer glog.Flush()

	if b.write {
		benchWrite()
	}
	if b.read {
		benchRead()
	}
	benchRemove()
}

func benchWrite() {
	fileIdLineChan := make(chan string, b.concurrency)
	finishChan := make(chan bool)
	idChan := make(chan int)
	writeStats = instrument.NewStats(b.concurrency)
	writeStats.Start = time.Now()
	writeStats.Total = b.numberOfFiles

	go writeFileIds(b.idListFile, fileIdLineChan, finishChan)
	go writeStats.CheckProgress(finishChan, &wait, "Start writing local...")

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
	writeStats.PrintStats(b.concurrency, "Writed local complete.")
}

func benchRead() {
	fileIdLineChan := make(chan string, b.concurrency)
	finishChan := make(chan bool)
	readStats = instrument.NewStats(b.concurrency)
	readStats.Start = time.Now()
	readStats.Total = b.numberOfFiles

	go readFileIds(b.idListFile, fileIdLineChan, readStats.Total)
	go readStats.CheckProgress(finishChan, &wait, "Start reading local...")

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
	readStats.PrintStats(b.concurrency, "Readed local complete.")
}
func benchRemove() {
	fileIdLineChan := make(chan string, b.concurrency)
	finishChan := make(chan bool)

	go removeFileIds(b.idListFile, fileIdLineChan)
	go instrument.PrintProgress(finishChan, &wait, "Start removing local...", "Removed local complete.")

	for i := 0; i < b.concurrency; i++ {
		wait.Add(1)
		go removeFiles(fileIdLineChan)
	}
	wait.Wait() // wait removeFiles

	wait.Add(1)
	finishChan <- true
	wait.Wait()
	close(finishChan)

	os.Remove(b.idListFile)
	os.RemoveAll(b.baseDir)
}

func writeFiles(idChan chan int, fileIdLineChan chan string, s *instrument.Stat) {
	defer wait.Done()
	delayedDeleteChan := make(chan *DelayedFile, 100)
	var waitForDeletions sync.WaitGroup

	for i := 0; i < 7; i++ {
		waitForDeletions.Add(1)
		go func() {
			defer waitForDeletions.Done()
			for df := range delayedDeleteChan {
				if df.EnterTime.After(time.Now()) {
					time.Sleep(df.EnterTime.Sub(time.Now()))
				}

				if err := os.Remove(instrument.GetFilePath(b.baseDir, b.domain, df.Fid, 3, 2)); err != nil {
					glog.Warningf("Failed to remove %s: %v", df.Fid, err)
					continue
				}
			}
		}()
	}

	for id := range idChan {
		start := time.Now()
		fileSize := int64(b.fileSize + rand.Intn(64))
		reader := utils.NewFakeReader(uint64(id), 0, fileSize)

		var filepath string
		filepath = instrument.GetFilePath(b.baseDir, b.domain, strconv.Itoa(id), 3, 2)

		err := os.MkdirAll(instrument.GetParentDirectory(filepath), 0777)
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to make dir %s: %v\n", filepath, err)
			continue
		}
		cfile, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to create: %v", err)
			continue
		}

		written, err := io.Copy(cfile, reader)
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to copy: %v", err)
			continue
		}

		err = cfile.Close()
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to close: %v", err)
			continue
		}
		fid := strconv.Itoa(id)
		if rand.Intn(100) < b.deletePercentage {
			delayedDeleteChan <- &DelayedFile{time.Now().Add(time.Second), fid}
		} else {
			fileIdLineChan <- fid
		}

		s.Completed++
		s.Transferred += written
		writeStats.AddSample(time.Now().Sub(start))

		glog.V(4).Infof("writing %d file %s", id, cfile.Name())
	}

	close(delayedDeleteChan)
	waitForDeletions.Wait()
}

func readFiles(fileIdLineChan chan string, s *instrument.Stat) {
	defer wait.Done()
	for fid := range fileIdLineChan {
		if len(fid) == 0 {
			continue
		}
		if fid[0] == '#' {
			continue
		}

		glog.V(4).Infof("reading file %s", fid)
		start := time.Now()
		oFile, err := os.Open(instrument.GetFilePath(b.baseDir, b.domain, fid, 3, 2))
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to open %s: %v", fid, err)
			continue
		}

		written, err := io.Copy(ioutil.Discard, oFile)
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to copy %s: %v", fid, err)
			continue
		}

		err = oFile.Close()
		if err != nil {
			s.Failed++
			glog.Warningf("Failed to close %s: %v", fid, err)
			continue
		}
		s.Completed++
		s.Transferred += written
		readStats.AddSample(time.Now().Sub(start))
	}
}

func removeFiles(fileIdLineChan chan string) {
	defer wait.Done()
	for fid := range fileIdLineChan {
		if len(fid) == 0 {
			continue
		}
		if fid[0] == '#' {
			continue
		}

		glog.V(4).Infof("Remove file %s", fid)
		err := os.Remove(instrument.GetFilePath(b.baseDir, b.domain, fid, 3, 2))
		if err != nil {
			glog.Warningf("Failed to remove %s: %v", fid, err)
			continue
		}
	}
}

func writeFileIds(fileName string, fileIdLineChan chan string, finishChan chan bool) {
	err := os.MkdirAll(instrument.GetParentDirectory(fileName), 0777)
	if err != nil {
		glog.Warningf("Failed to make dir %s: %v\n", fileName, err)
	}

	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		glog.Warningf("Failed to create file %s: %v\n", fileName, err)
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

func readFileIds(fileName string, fileIdLineChan chan string, total int) {
	file, err := os.Open(fileName) // For read access.
	if err != nil {
		glog.Warningf("Failed to read file %s: %v", fileName, err)
	}
	defer file.Close()

	r := bufio.NewReader(file)
	lines := make([]string, 0, total)
	for {
		if line, err := utils.Readln(r); err == nil {
			lines = append(lines, string(line))
		} else {
			break
		}
	}
	if len(lines) > 0 {
		for i := 0; i < total; i++ {
			fileIdLineChan <- lines[rand.Intn(len(lines))]
		}
	}

	close(fileIdLineChan)
}

func removeFileIds(fileName string, fileIdLineChan chan string) {
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

type DelayedFile struct {
	EnterTime time.Time
	Fid       string
}
