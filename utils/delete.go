package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"net/http"
)

type DeleteResult struct {
	Fid    string `json:"fid"`
	Size   int    `json:"size"`
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
}

func DeleteFile(seeds, fileId string) error {
	locations, err := LookupFileId(seeds, fileId)
	if err != nil {
		return err
	}

	// All replications are deleted means success,
	// otherwise the error message is return.
	var errs []string
	for _, location := range locations {
		fileUrl := fmt.Sprintf("http://%s/%s", location.PublicUrl, fileId)
		err = Delete(fileUrl)
		if err == nil {
			break
		}
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}

func ParseFileId(fid string) (vid, keyCookie string, err error) {
	commaIndex := strings.Index(fid, ",")
	if commaIndex <= 0 {
		return "", "", fmt.Errorf("wrong fid format: %s", fid)
	}
	return fid[:commaIndex], fid[commaIndex+1:], nil
}

type DeleteFilesResult struct {
	Errors  []string
	Results []DeleteResult
}

func DeleteFiles(seeds string, fileIds []string) (*DeleteFilesResult, error) {
	vidToFileIds := make(map[string][]string)
	ret := &DeleteFilesResult{}
	var vids []string
	for _, fileId := range fileIds {
		vid, _, err := ParseFileId(fileId)
		if err != nil {
			ret.Results = append(ret.Results, DeleteResult{
				Fid:    vid,
				Status: http.StatusBadRequest,
				Error:  err.Error()},
			)
			continue
		}
		if _, ok := vidToFileIds[vid]; !ok {
			vidToFileIds[vid] = make([]string, 0)
			vids = append(vids, vid)
		}
		vidToFileIds[vid] = append(vidToFileIds[vid], fileId)
	}

	lookupResults, err := LookupVolumeIds(seeds, vids)
	if err != nil {
		return ret, err
	}

	serverToFileIds := make(map[string][]string)
	for vid, result := range lookupResults {
		if result.Error != "" {
			ret.Errors = append(ret.Errors, result.Error)
			continue
		}
		for _, location := range result.Locations {
			if _, ok := serverToFileIds[location.Url]; !ok {
				serverToFileIds[location.Url] = make([]string, 0)
			}
			serverToFileIds[location.Url] = append(
				serverToFileIds[location.Url], vidToFileIds[vid]...)
		}
	}

	var wg sync.WaitGroup

	for server, fidList := range serverToFileIds {
		wg.Add(1)
		go func(server string, fidList []string) {
			defer wg.Done()
			values := make(url.Values)
			for _, fid := range fidList {
				values.Add("fid", fid)
			}
			jsonBlob, err := Post(fmt.Sprintf("http://%s/delete", server), values)
			if err != nil {
				ret.Errors = append(ret.Errors, err.Error())
				return
			}
			var result []DeleteResult
			err = json.Unmarshal(jsonBlob, &result)
			if err != nil {
				ret.Errors = append(ret.Errors, err.Error()+" "+string(jsonBlob))
				return
			}
			ret.Results = append(ret.Results, result...)
		}(server, fidList)
	}
	wg.Wait()

	return ret, nil
}
