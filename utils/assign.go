package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/golang/glog"
)

type VolumeAssignRequest struct {
	Count       uint64
	Replication string
	Collection  string
	Ttl         string
	DataCenter  string
	Rack        string
	DataNode    string
}

type AssignResult struct {
	Fid       string `json:"fid,omitempty"`
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
	Count     uint64 `json:"count,omitempty"`
	Error     string `json:"error,omitempty"`
}

func Assign(seeds string, r *VolumeAssignRequest) (*AssignResult, error) {
	values := make(url.Values)
	values.Add("count", strconv.FormatUint(r.Count, 10))
	if r.Replication != "" {
		values.Add("replication", r.Replication)
	}
	if r.Collection != "" {
		values.Add("collection", r.Collection)
	}
	if r.Ttl != "" {
		values.Add("ttl", r.Ttl)
	}
	if r.DataCenter != "" {
		values.Add("dataCenter", r.DataCenter)
	}
	if r.Rack != "" {
		values.Add("rack", r.Rack)
	}
	if r.DataNode != "" {
		values.Add("dataNode", r.DataNode)
	}

	var ret AssignResult
	var err error
	var jsonBlob []byte
	err = RetryPost(seeds, func(seed string) error {
		jsonBlob, err = Post(fmt.Sprintf("http://%s/dir/assign", seed), values)
		glog.V(4).Info("Assign result :", string(jsonBlob))

		if err != nil {
			return err
		}
		err = json.Unmarshal(jsonBlob, &ret)
		if err != nil {
			return err
		}
		if ret.Error != "" || ret.Count <= 0 {
			return errors.New(ret.Error)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &ret, nil
}
