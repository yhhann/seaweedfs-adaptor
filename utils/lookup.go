package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}

type LookupResult struct {
	VolumeId  string     `json:"volumeId,omitempty"`
	Locations []Location `json:"locations,omitempty"`
	Error     string     `json:"error,omitempty"`
}

func (lr *LookupResult) String() string {
	return fmt.Sprintf("VolumeId:%s, Locations:%v, Error:%s", lr.VolumeId, lr.Locations, lr.Error)
}

var (
	vc VidCache // Caching of volume locations, re-check if after 10 minutes.
)

func Lookup(server string, vid string) (ret *LookupResult, err error) {
	locations, cacheErr := vc.Get(vid)
	if cacheErr != nil {
		if ret, err = doLookup(server, vid); err == nil {
			vc.Set(vid, ret.Locations, EXPIRED_TIME)
		}
	} else {
		ret = &LookupResult{VolumeId: vid, Locations: locations}
	}

	return
}

func doLookup(seeds string, vid string) (*LookupResult, error) {
	values := make(url.Values)
	values.Add("volumeId", vid)

	var ret LookupResult
	var jsonBlob []byte
	var err error
	err = RetryPost(seeds, func(seed string) error {
		jsonBlob, err = Post(fmt.Sprintf("http://%s/dir/lookup", seed), values)
		if err != nil {
			return err
		}

		err = json.Unmarshal(jsonBlob, &ret)
		if err != nil {
			return err
		}
		if ret.Error != "" {
			return errors.New(ret.Error)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &ret, nil
}

func LookupFileId(server string, fileId string) ([]Location, error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid fileId %s", fileId)
	}

	lookup, err := Lookup(server, parts[0])
	if err != nil {
		return nil, err
	}
	if len(lookup.Locations) == 0 {
		return nil, fmt.Errorf("file not found for %s", fileId)
	}

	return lookup.Locations, nil
}

// LookupVolumeIds find volume locations by cache and actual lookup
func LookupVolumeIds(seeds string, vids []string) (map[string]LookupResult, error) {
	ret := make(map[string]LookupResult)
	var unknownVids []string
	//check vid cache first
	for _, vid := range vids {
		locations, cacheErr := vc.Get(vid)
		if cacheErr == nil {
			ret[vid] = LookupResult{VolumeId: vid, Locations: locations}
		} else {
			unknownVids = append(unknownVids, vid)
		}
	}
	//return success if all volume ids are known
	if len(unknownVids) == 0 {
		return ret, nil
	}

	//only query unknown_vids
	values := make(url.Values)
	for _, vid := range unknownVids {
		values.Add("volumeId", vid)
	}

	var jsonBlob []byte
	var err error
	err = RetryPost(seeds, func(seed string) error {
		jsonBlob, err = Post(fmt.Sprintf("http://%s/vol/lookup", seed), values)
		if err != nil {
			return err
		}
		err = json.Unmarshal(jsonBlob, &ret)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	//set newly checked vids to cache
	var errs []string
	for _, vid := range unknownVids {
		if ret[vid].Error != "" {
			errs = append(errs, fmt.Sprintf("[%s]: %s", ret[vid].VolumeId, ret[vid].Error))
			continue
		}
		locations := ret[vid].Locations
		vc.Set(vid, locations, EXPIRED_TIME)
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "\n"))
	}

	return ret, nil
}
