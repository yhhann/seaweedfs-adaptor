package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"path/filepath"
	"strings"
)

type UploadResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
}

var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

func Upload(uploadUrl string, filename string, reader io.Reader, isGzipped bool, mtype string) (*UploadResult, error) {
	return uploadContent(uploadUrl, func(w io.Writer) (err error) {
		_, err = io.Copy(w, reader)
		return
	}, filename, isGzipped, mtype)
}
func uploadContent(uploadUrl string, fillBufferFunction func(w io.Writer) error, filename string, isGzipped bool, mtype string) (*UploadResult, error) {
	bodyBuf := bytes.NewBufferString("")
	bodyWriter := multipart.NewWriter(bodyBuf)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	if mtype != "" {
		h.Set("Content-Type", mtype)
	}
	if isGzipped {
		h.Set("Content-Encoding", "gzip")
	}
	fileWriter, err := bodyWriter.CreatePart(h)
	if err != nil {
		return nil, err
	}
	if err := fillBufferFunction(fileWriter); err != nil {
		return nil, err
	}
	contentType := bodyWriter.FormDataContentType()
	if err := bodyWriter.Close(); err != nil {
		return nil, err
	}
	respBody, err := PostBytes(uploadUrl, contentType, bodyBuf)
	if err != nil {
		return nil, err
	}

	var ret UploadResult
	err = json.Unmarshal(respBody, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}

	return &ret, nil
}
