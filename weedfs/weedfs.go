package weedfs

/**
	Reference from operation/submit.go
**/

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/golang/glog"

	"jingoal.com/seaweedfs-adaptor/utils"
)

const (
	MAX_CHUNK_SIZE = int64(1 * 1024 * 1024)
)

var (
	weedChunkSize int64
	defaultTTL    string
)

func init() {
	flag.Int64Var(&weedChunkSize, "weed-chunk-size", 512*1024, "upload chunk size in bytes")
	flag.StringVar(&defaultTTL, "default-ttl", "26w", "default TTL")
}

type WeedFile struct {
	Fid       string
	FileName  string
	RealName  string
	IsGzipped bool
	MimeType  string
	FileUrl   string
	Size      int64 // upload bytes size.
	TTL       string

	buf       *bytes.Buffer
	split     bool               // chunkSize>0 and upload.size>chunkSize, split is true.
	hasErr    bool               // when has error, need delete all uploaded chunks.
	chunkInfo []*utils.ChunkInfo // upload chunk info

	reader   io.ReadCloser // download stream.
	readFlag bool          // distinguish read or write, will do difference close.

	seeds       string
	replication string // replica strategy
	dataCenter  string
	rack        string
	chunkSize   int64
}

func (f *WeedFile) GetChunkSize() int64 {
	return f.chunkSize
}

func (f *WeedFile) SetChunkSize(size int64) {
	f.chunkSize = size
}

func (f *WeedFile) String() string {
	return fmt.Sprintf("Fid:%s, FileName:%s, IsGzipped:%t, MimeType:%s, FileUrl:%s, TTL:%s", f.Fid, f.RealName, f.IsGzipped, f.MimeType, f.FileUrl, f.TTL)
}

// Read reads atmost len(p) bytes into p.
// Returns number of bytes read and an error if any.
func (f *WeedFile) Read(p []byte) (int, error) {
	nr, err := f.reader.Read(p)
	if nr <= 0 {
		return 0, io.EOF
	}

	return nr, err
}

// Write writes len(p) bytes to the file.
// Returns number of bytes written and an error if any.
func (f *WeedFile) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	var err error
	if f.chunkSize > 0 && int64(f.buf.Len()+len(p)) > f.chunkSize { // need split chunk
		var offset int // has writtened from p
		chunks := int64(f.buf.Len()+len(p)) / f.chunkSize
		for i := int64(0); i < chunks; i++ {
			rb := f.chunkSize - int64(f.buf.Len()) + int64(offset) // need read bytes from p
			n, err := f.buf.Write(p[offset:rb])
			if err != nil {
				f.hasErr = true
				return 0, err
			}

			if n > 0 {
				offset += n
			}
			if int64(f.buf.Len()) == f.chunkSize {
				f.split = true // split chunk upload
				if _, err := f.UploadChunk(); err != nil {
					return 0, err
				}
				f.buf.Reset()
			} else {
				i-- // not write full, retry again
			}
		}
		if offset < len(p) {
			_, err = f.buf.Write(p[offset:])
		}
	} else {
		_, err = f.buf.Write(p)
	}

	if err != nil {
		f.hasErr = true
		return 0, err
	}

	return len(p), nil
}

// Close closes an open WeedFile.
// Returns an error on failure.
func (f *WeedFile) Close() error {
	if f.readFlag { // read
		if err := f.reader.Close(); err != nil {
			return err
		}
		glog.V(4).Infof("Succeeded to download %s from %s.", f.FileName, f.FileUrl)
		return nil
	}

	if f.hasErr {
		f.buf.Reset()
		f.DeleteChunks() // if has upload chunk, need to delete.
		return nil
	}

	if !f.split { // splitSize == 0 or not great than splitSize
		_, err := utils.Upload(utils.SanitizeTTL(f.FileUrl, f.TTL), f.FileName, bytes.NewReader(f.buf.Bytes()), f.IsGzipped, f.MimeType)
		if err != nil {
			glog.Warningf("Failed to upload %s to %s, %v", f.RealName, f.FileUrl, err)
			return err
		}
		glog.V(4).Infof("Succeeded to upload %s to %s.", f.RealName, f.FileUrl)
		return nil
	}

	if f.buf.Len() > 0 { // the last chunk
		_, err := f.UploadChunk()
		f.buf.Reset()
		if err != nil {
			f.DeleteChunks()
			glog.Warningf("Failed to upload %s to %s, %v", f.RealName, f.FileUrl, err)
			return err
		}
	}

	if err := f.UploadManifest(); err != nil {
		f.DeleteChunks()
		glog.Warningf("Failed to upload %s to %s, %v", f.RealName, f.FileUrl, err)
		return err
	}
	glog.V(4).Infof("Succeeded to upload %s to %s.", f.RealName, f.FileUrl)

	return nil
}

func (f *WeedFile) DeleteChunks() error {
	delErr := 0
	for _, ci := range f.chunkInfo {
		if err := utils.DeleteFile(f.seeds, ci.Fid); err != nil {
			delErr++
			glog.Warningf("Failed to remove %s from %s, %v", ci.Fid, f.seeds, err)
		}
	}
	if delErr > 0 {
		return errors.New("Not all chunks deleted.")
	}

	return nil
}

func (f *WeedFile) UploadChunk() (retSize int64, err error) {
	chunkIdx := len(f.chunkInfo)
	fname := fmt.Sprintf("%s-%s", f.Fid, strconv.Itoa(chunkIdx+1))
	fid, count, err := f.uploadChunk(fname)
	if err != nil {
		f.hasErr = true
		return 0, err
	}

	f.chunkInfo = append(f.chunkInfo, &utils.ChunkInfo{
		Offset: int64(chunkIdx) * f.chunkSize,
		Size:   int64(count),
		Fid:    fid,
	})
	f.Size += int64(count)

	return f.Size, nil
}

func (f *WeedFile) uploadChunk(filename string) (fid string, size uint32, e error) {
	ar := &utils.VolumeAssignRequest{
		Count:       1,
		Replication: f.replication,
		DataCenter:  f.dataCenter,
		Rack:        f.rack,
		Ttl:         f.TTL,
	}
	ret, err := utils.Assign(f.seeds, ar)
	if err != nil {
		return "", 0, err
	}

	fileUrl := utils.SanitizeTTL(fmt.Sprintf("http://%s/%s", ret.PublicUrl, ret.Fid), utils.AdjustTTL(f.TTL))
	glog.V(4).Infof("Uploading chunk %s to %s...", filename, fileUrl)
	uploadRet, err := utils.Upload(fileUrl, filename, bytes.NewReader(f.buf.Bytes()), false, "application/octet-stream")
	if err != nil {
		return ret.Fid, 0, err
	}

	return ret.Fid, uploadRet.Size, nil
}

func (f *WeedFile) UploadManifest() error {
	cm := utils.ChunkManifest{
		Name:   f.RealName,
		Size:   f.Size,
		Mime:   f.MimeType,
		Chunks: f.chunkInfo[0:len(f.chunkInfo)],
	}

	err := f.uploadManifest(&cm)
	if err != nil {
		return err
	}
	glog.V(4).Infof("Succeeded to upload chunks manifest %s to %s.", cm.Name, f.FileUrl)

	return nil
}

func (f *WeedFile) uploadManifest(manifest *utils.ChunkManifest) error {
	b, err := manifest.Marshal()
	if err != nil {
		return err
	}

	br := bytes.NewReader(b)
	u, _ := url.Parse(f.FileUrl)
	q := u.Query()
	q.Set("cm", "true")
	if f.TTL != "" {
		q.Set("ttl", f.TTL)
	}
	u.RawQuery = q.Encode()
	_, err = utils.Upload(u.String(), manifest.Name, br, false, "application/json")

	return err
}

// Create call with SeaWeedFS interface, create standard io.Writer.
// Provide the stream operation of file for upload.
func Create(name string, domain int64, seeds, replication, dc, rack string, chunkSize int64) (*WeedFile, error) {
	return create(name, domain, seeds, replication, dc, rack, chunkSize, defaultTTL)
}

func create(name string, domain int64, seeds, replication, dc, rack string, chunkSize int64, ttl string) (*WeedFile, error) {
	if weedChunkSize > MAX_CHUNK_SIZE {
		weedChunkSize = MAX_CHUNK_SIZE
		glog.Warningf("weed-chunk-size is set too large, use %d instead.", MAX_CHUNK_SIZE)
	}

	ret := &WeedFile{
		readFlag:    false,
		seeds:       seeds,
		replication: replication,
		dataCenter:  dc,
		rack:        rack,
		chunkSize:   weedChunkSize,
		Size:        0,
		buf:         bytes.NewBuffer(nil),
		split:       false,
		hasErr:      false,
		chunkInfo:   make([]*utils.ChunkInfo, 0),
		TTL:         ttl,
	}

	ar := &utils.VolumeAssignRequest{
		Count:       1,
		Replication: ret.replication,
		DataCenter:  ret.dataCenter,
		Rack:        ret.rack,
		Ttl:         ret.TTL,
	}
	aRet, err := utils.Assign(ret.seeds, ar)
	if err != nil {
		return nil, err
	}
	ret.Fid = aRet.Fid
	ret.FileName = aRet.Fid
	ret.RealName = aRet.Fid
	ret.FileUrl = fmt.Sprintf("http://%s/%s", aRet.PublicUrl, aRet.Fid)

	if name != "" {
		baseName := path.Base(name)
		ret.FileName = baseName
		ret.RealName = baseName
		ext := strings.ToLower(path.Ext(baseName))

		if ext != "" {
			ret.MimeType = mime.TypeByExtension(ext)
			// The SeaweedFS handle the file with .gz suffix for a special treatment,
			// so we need to retain all the file suffix.
			// However, the file with ".css.gz"、".html.gz"、".txt.gz"、".js.gz" etc
			// will remove the ".gz" suffix, fit for the browser download.
			// This will cause the MD5 value of the httpClient request inconsistent.
			if ext == ".gz" {
				ret.IsGzipped = true
				if needRename(baseName, ext) {
					ret.FileName = ret.Fid + ".gz"
				}
			}
		}
	}
	glog.V(4).Infof("Create seaweed file %s", ret)

	return ret, nil
}

// suit for storage/needle.go logic.
func needRename(s, ext string) bool {
	if s == "" || ext == "" {
		return false
	}
	if strings.Index(s, ext) == -1 {
		return false
	}
	s = s[:len(s)-len(ext)]
	ext = strings.ToLower(path.Ext(s))
	if ext == "" {
		return false
	}
	switch ext {
	case ".pdf", ".txt", ".html", ".htm", ".css", ".js", ".json":
		return true
	default:
		return false
	}
}

// Open access the SeaWeedFS file by the rest interface,
// return the standard io.Reader.
// Provide the stream operation of file for download.
func Open(id string, domain int64, seeds string) (*WeedFile, error) {
	ret := &WeedFile{
		Fid:      id,
		readFlag: true,
		seeds:    seeds,
	}

	locations, err := utils.LookupFileId(ret.seeds, ret.Fid)
	if err != nil {
		return nil, err
	}
	// Add retry mechanism
	var fileUrl, filename string
	var rc io.ReadCloser
	for _, location := range locations {
		fileUrl = fmt.Sprintf("http://%s/%s", location.PublicUrl, ret.Fid)
		filename, rc, err = utils.DownloadUrl(fileUrl)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	if filename == "" {
		filename = id
	}
	ret.FileName = filename
	ret.RealName = filename
	ret.FileUrl = fileUrl
	ret.reader = rc

	glog.V(4).Infof("Open seaweed file url: %s...", fileUrl)
	return ret, nil
}

func Remove(id string, domain int64, seeds string) (bool, error) {
	if e := utils.DeleteFile(seeds, id); e != nil {
		return false, e
	}
	return true, nil
}
