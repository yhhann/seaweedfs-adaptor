package weedfs

import (
	"io"
	"os"
	"testing"

	"jingoal.com/seaweedfs-adaptor/utils"
)

var (
	seeds              = "localhost:9333,localhost:9334,localhost:9335"
	replication        = "000" // replication strategy
	fullpathname       = ""
	domain       int64 = 1
	dataCenter         = ""
	rack               = ""
)

func TestUpAndDownload(t *testing.T) {
	if fullpathname == "" {
		t.Skip("Please input file full path.")
	}

	reader, err := os.Open(fullpathname)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer reader.Close()

	origMd5, _, err := utils.Md5Reader(reader)
	if err != nil {
		t.Fatalf("Failed to md5: %v", err)
	}
	t.Logf("Orig md5: %s", origMd5)
	reader.Seek(0, 0)

	cfile, err := Create(reader.Name(), domain, seeds, replication, dataCenter, rack, 0)
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}

	_, err = io.Copy(cfile, reader)
	if err != nil {
		t.Fatalf("Failed to copy: %v", err)
	}

	err = cfile.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}
	t.Log("fid:", cfile.Fid)

	oFile, err := Open(cfile.Fid, domain, seeds)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer oFile.Close()

	downMd5, _, err := utils.Md5Reader(oFile)
	if err != nil {
		t.Fatalf("Failed to copy: %v", err)
	}
	if downMd5 == origMd5 {
		t.Log("MD5 equal.")
	} else {
		t.Fatal("MD5 not equal.")
	}

	delSucc, err := Remove(cfile.Fid, domain, seeds)
	if err != nil {
		t.Fatalf("Failed to remove: %v", err)
	}
	t.Logf("Remove file %s, success %t", cfile.Fid, delSucc)
}
