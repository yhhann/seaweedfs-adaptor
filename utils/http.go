package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

const (
	MAXIDLECONNSPERHOST int = 1024
)

var (
	client *http.Client
)

func init() {
	transport := &http.Transport{
		MaxIdleConnsPerHost: MAXIDLECONNSPERHOST,
	}
	client = &http.Client{Transport: transport}
}

func PostBytes(url, contentType string, body io.Reader) ([]byte, error) {
	r, err := client.Post(url, contentType, body)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	return ReadAllHandler(r)
}

func Post(url string, values url.Values) ([]byte, error) {
	r, err := client.PostForm(url, values)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	return ReadAllHandler(r)
}

func Get(url string) ([]byte, error) {
	r, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}

	return ReadAllHandler(r)
}

func ReadAllHandler(r *http.Response) ([]byte, error) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func Delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusAccepted, http.StatusOK:
		return nil
	}
	// The following code will return error:
	// If we can unmarshal the error information from the response,
	// then return the details.
	m := make(map[string]interface{})
	if err := json.Unmarshal(body, m); err == nil {
		if s, ok := m["error"].(string); ok {
			return errors.New(s)
		}
	}

	return errors.New(string(body))
}

func GetBufferStream(url string, values url.Values, allocatedBytes []byte, eachBuffer func([]byte)) error {
	r, err := client.PostForm(url, values)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, r.Status)
	}

	bufferSize := len(allocatedBytes)
	for {
		n, err := r.Body.Read(allocatedBytes)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if n == bufferSize {
			eachBuffer(allocatedBytes)
		}
	}
}

func GetUrlStream(url string, values url.Values, readFn func(io.Reader) error) error {
	r, err := client.PostForm(url, values)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, r.Status)
	}

	return readFn(r.Body)
}

func DownloadUrl(url string) (filename string, rc io.ReadCloser, e error) {
	response, err := client.Get(url)
	if err != nil {
		return "", nil, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return "", nil, fmt.Errorf("%s: %s", url, response.Status)
	}

	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		idx := strings.Index(contentDisposition[0], "filename=")
		if idx != -1 {
			filename = contentDisposition[0][idx+len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	rc = response.Body

	return
}

func Do(req *http.Request) (resp *http.Response, err error) {
	return client.Do(req)
}

func SanitizeUrl(url string) string {
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		return url
	}

	return fmt.Sprintf("http://%s", url)
}
