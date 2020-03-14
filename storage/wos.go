package storage

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type WosStorage struct {
	host          string
	readUrlPrefix string
}

func NewWosStorage(host string) *WosStorage {
	s := &WosStorage{
		host: host,
	}
	u := url.URL{Host: host, Path: "/objects/"}
	u.Scheme = "http"
	s.readUrlPrefix = u.String()
	return s
}

// Read read the wos server and create a wos object
// remember to close the object body after use
func (t *WosStorage) Read(key string) (SyncObject, error) {
	client := http.Client{
		Timeout: time.Duration(WosReadTimeout),
	}

	req, err := http.NewRequest("GET", t.readUrlPrefix+key, nil)
	if err != nil {
		return nil, err
	}
	//req.Header.Set("content-type", "application/octet-stream")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, fmt.Errorf("wos read error %s: http failed code: %d", key, resp.StatusCode)
	}

	wo := SyncObjectImp{
		length: -1,
	}
	ddnStatus := ""
	for k, v := range resp.Header {
		if strings.ToLower(k) == "x-ddn-status" {
			ddnStatus = string(v[0])
		}
		if strings.ToLower(k) == "content-type" {
			wo.contentType = string(v[0])
		}
		if strings.ToLower(k) == "content-length" {
			wo.length, err = strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				resp.Body.Close()
				return nil, fmt.Errorf("wos read content-length error %s: %s", key, err.Error())
			}
		}
	}

	if ddnStatus == "" {
		resp.Body.Close()
		return nil, fmt.Errorf("wos read error %s: not found x-ddn-status", key)
	}

	if ddnStatus != "0 ok" {
		resp.Body.Close()
		return nil, fmt.Errorf(
			"wos read error %s: failed x-ddn-status code: %s",
			key, ddnStatus)
	}

	if wo.contentType == "" {
		resp.Body.Close()
		return nil, fmt.Errorf("wos read error %s: not found contentType", key)
	}

	if wo.length == -1 {
		resp.Body.Close()
		return nil, fmt.Errorf("wos read error %s: not found length", key)
	}

	wo.body = resp.Body
	return &wo, nil
}

// func (t *WosStorage) Verify(key, checksum string) (bool, error) {
// 	if checksum == "" {
// 		log.Warnf("no checksum for key: %s", key)
// 		return false, nil
// 	}
// 	client := http.Client{
// 		Timeout: time.Duration(WosReadTimeout),
// 	}

// 	req, err := http.NewRequest("GET", t.readUrlPrefix+key, nil)
// 	if err != nil {
// 		return false, err
// 	}
// 	req.Header.Set("content-type", "application/octet-stream")
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		return false, err
// 	}

// 	defer resp.Body.Close()
// 	if resp.StatusCode == 200 {
// 		hash := md5.New()
// 		if _, err := io.Copy(hash, resp.Body); err != nil {
// 			return false, err
// 		}
// 		sum := fmt.Sprintf("\"%x\"", hash.Sum(nil))
// 		if sum != checksum {
// 			log.Warnf("%s checksum mismatch, %s want, %s got", key, checksum, sum)
// 			return false, nil
// 		}
// 		return true, nil
// 	}
// 	return false, fmt.Errorf("verify error: %d", resp.StatusCode)
// }
