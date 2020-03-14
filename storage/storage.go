package storage

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	WosWriteTimeout = 300 * time.Second
	WosReadTimeout  = 300 * time.Second
)

type StorDest interface {
	Write(key string, obj SyncObject) (string, error)
	Read(key string) (SyncObject, error)
}

type StorSrc interface {
	Read(key string) (SyncObject, error)
}

type SyncObject interface {
	GetContentType() string
	GetContentLength() int64
	GetBody() io.ReadCloser
}

type SyncObjectImp struct {
	contentType string
	length      int64
	body        io.ReadCloser
}

func (t *SyncObjectImp) GetContentType() string {
	return t.contentType
}

func (t *SyncObjectImp) GetContentLength() int64 {
	return t.length
}

func (t *SyncObjectImp) GetBody() io.ReadCloser {
	return t.body
}

func CalcMD5(data io.ReadCloser) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, data); err != nil {
		return "", err
	}
	sum := fmt.Sprintf("\"%x\"", hash.Sum(nil))
	return sum, nil
}

func init() {
	timeout := os.Getenv("APP_TIMEOUT")
	if timeout != "" {
		i, err := strconv.Atoi(timeout)
		if err != nil {
			log.Errorf("invalid timeout: %s, skip", timeout)
		} else {
			WosWriteTimeout = time.Duration(i) * time.Second
			WosReadTimeout = time.Duration(i) * time.Second
		}
	}
}
