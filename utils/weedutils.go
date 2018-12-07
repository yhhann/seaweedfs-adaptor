package utils

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"
)

const (
	BUF_KB int = 1024
)

var (
	sharedBytes []byte
	errWhence   = errors.New("Seek: invalid whence")
	errOffset   = errors.New("Seek: invalid offset")
)

func init() {
	sharedBytes = make([]byte, BUF_KB)
}

func NewFakeReader(id uint64, off int64, n int64) *FakeReader {
	return &FakeReader{
		id:    id,
		base:  off,
		off:   off,
		limit: off + n,
	}
}

// a fake reader to generate content to upload, support seek
type FakeReader struct {
	id    uint64
	base  int64
	off   int64
	limit int64
}

func (r *FakeReader) Read(p []byte) (n int, err error) {
	if r.off >= r.limit {
		return 0, io.EOF
	}

	if size := r.limit - r.off; int64(len(p)) > size {
		n = int(size)
	} else {
		n = len(p)
	}
	if n >= 8 {
		for i := 0; i < 8; i++ {
			p[i] = byte(r.id >> uint(i*8))
		}
	}
	r.off += int64(n)

	return
}

func (r *FakeReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	default:
		return 0, errWhence
	case io.SeekStart:
		offset += r.base
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.limit
	}
	if offset < r.base {
		return 0, errOffset
	}
	r.off = offset

	return offset - r.base, nil
}

func (r *FakeReader) WriteTo(w io.Writer) (n int64, err error) {
	size := r.Size()
	bufferSize := int64(len(sharedBytes))
	for size > 0 {
		tempBuffer := sharedBytes
		if size < bufferSize {
			tempBuffer = sharedBytes[0:size]
		}
		count, e := w.Write(tempBuffer)
		if e != nil {
			return size, e
		}
		size -= int64(count)
	}

	return r.Size(), nil
}

func (r *FakeReader) Size() int64 { return r.limit - r.base }

func Readln(r *bufio.Reader) ([]byte, error) {
	var (
		isPrefix = true
		err      error
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}

	return ln, err
}

// MD5 the io Reader.
func Md5Reader(r io.Reader) (md5str string, written int64, err error) {
	md5Buf := md5.New()
	written, err = io.Copy(md5Buf, r)
	if err != nil {
		return
	}

	md5str = hex.EncodeToString(md5Buf.Sum(nil))
	return
}

// simulated byte array.
func FakeBytes(fileSize int) []byte {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fileSize = fileSize + r.Intn(64)
	var data []byte = make([]byte, fileSize)
	reader, err := os.Open("/dev/urandom")
	if err != nil {
		return data
	}
	defer reader.Close()
	if length, err := io.ReadFull(reader, data); err == nil && length > 0 {
		// Trim byte array.
		data = data[:length]
	}

	return data
}

func RetryPost(seeds string, fn func(seed string) error) error {
	servers := strings.Split(seeds, ",")
	var err error
	for _, server := range servers {
		err = fn(server)
		if err != nil {
			continue
		}
		break
	}
	return err
}
