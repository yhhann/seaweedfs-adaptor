package main

import (
	"flag"
	"io"
	"os"
	"time"

	"github.com/golang/glog"

	"jingoal.com/seaweedfs-adaptor/utils"
	"jingoal.com/seaweedfs-adaptor/weedfs"
)

var (
	seeds       string
	replication string
	dataCenter  string
	rack        string

	domain       int64
	fullpathname string

	del bool
)

func init() {
	flag.StringVar(&seeds, "seeds", "localhost:9333", "SeaweedFS master seeds location")
	flag.StringVar(&replication, "replication", "000", "replication type")
	flag.StringVar(&dataCenter, "dataCenter", "", "current volume server's data center name")
	flag.StringVar(&rack, "rack", "", "current volume server's rack name")

	flag.Int64Var(&domain, "domain", 1, "corpration ID?")
	flag.StringVar(&fullpathname, "fullpathname", "", "Specify the file to be operated.")

	flag.BoolVar(&del, "del", true, "delete test file")
}

func checkFlags() {
	if seeds == "" {
		glog.Exit("Error: master seeds is required.")
	}
	if replication == "" {
		replication = "000"
	}
	if domain <= 0 {
		glog.Exit("Error: domain is required.")
	}
	if fullpathname == "" {
		glog.Exit("Error: must specify the file to be operated.")
	}
}

func main() {
	glog.MaxSize = 1024 * 1024 * 32
	flag.Parse()
	checkFlags()
	defer glog.Flush()

	reader, err := os.Open(fullpathname)
	if err != nil {
		glog.Exitf("Failed to open: %v", err)
	}
	defer reader.Close()

	origMd5, _, err := utils.Md5Reader(reader)
	if err != nil {
		glog.Exitf("Failed to md5: %v", err)
	}
	reader.Seek(0, 0)
	glog.V(4).Infof("Upload md5: %s", origMd5)

	// upload file
	startTime := time.Now()
	cfile, err := weedfs.Create(reader.Name(), domain, seeds, replication, dataCenter, rack, 0)
	if err != nil {
		glog.Exitf("Failed to create: %v", err)
	}

	_, err = io.Copy(cfile, reader)
	if err != nil {
		glog.Exitf("Failed to copy: %v", err)
	}

	err = cfile.Close()
	if err != nil {
		glog.Exitf("Failed to close: %v", err)
	}
	elapse := time.Since(startTime)
	glog.V(4).Infof("File upload elapse millisecond: %v", float64(elapse.Nanoseconds())/1e6)

	// download file
	startTime = time.Now()
	oFile, err := weedfs.Open(cfile.Fid, domain, seeds)
	if err != nil {
		glog.Exitf("Failed to open: %v", err)
	}
	defer oFile.Close()

	downMd5, _, err := utils.Md5Reader(oFile)
	if err != nil {
		glog.Exitf("Failed to copy: %v", err)
	}

	elapse = time.Since(startTime)
	glog.V(4).Infof("File download md5 %s, elapse millisecond: %v", downMd5, float64(elapse.Nanoseconds())/1e6)

	if downMd5 == origMd5 {
		glog.V(4).Info("MD5 equal.")
	} else {
		glog.Exit("MD5 not equal.")
	}

	if del {
		// delete file
		startTime = time.Now()
		delSucc, err := weedfs.Remove(cfile.Fid, domain, seeds)
		if err != nil {
			glog.Exitf("Failed to remove: %v", err)
		}
		elapse = time.Since(startTime)
		glog.V(4).Infof("File remove success %t, elapse millisecond: %v", delSucc, float64(elapse.Nanoseconds())/1e6)
	}
}
