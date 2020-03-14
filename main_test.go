package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"s3sync/storage"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	log "github.com/sirupsen/logrus"
)

var mux sync.Mutex

type memWriter struct {
	data []byte
}

func (t *memWriter) Write(data []byte) (int, error) {

	log.Debugf("writing data: %s", data)
	if t.data == nil {
		t.data = append([]byte{}, data...)
	} else {
		t.data = append(t.data, data...)
	}
	log.Debugf("report data: %s", t.data)
	return len(data), nil
}

type DB struct {
	data map[string][]byte
}

func (t *DB) read(key string) ([]byte, bool) {
	if data, ok := t.data[key]; ok {
		return data, true
	} else {
		return data, false
	}
}

func (t *DB) write(key string, c []byte) {
	t.data[key] = c
}

func setupWosServer(t *testing.T, oids []string) *httptest.Server {
	db := DB{data: map[string][]byte{}}
	for _, oid := range oids {
		db.data[oid] = []byte(oid + " content")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.Lock()
		defer mux.Unlock()
		if r.Method == "POST" {
			wosServePost(t, w, r, db)
		} else if r.Method == "GET" {
			uri := r.URL.String()
			oid := strings.TrimPrefix(uri, "/objects/")
			w.Header().Set("Content-Type", "application/octet-stream")
			if data, ok := db.read(oid); ok {
				w.Header().Set("x-ddn-status", "0 ok")
				w.Header().Set("x-ddn-oid", oid)
				w.Write(data)
			} else {
				w.Header().Set("x-ddn-status", "205 InvalidObjId")
				w.Header().Set("x-ddn-oid", oid)
				w.WriteHeader(404)
			}
		} else {
			t.Errorf("unsupported method: %s", r.Method)
			w.WriteHeader(400)
		}
	}))
	return server
}

func wosServePost(t *testing.T, w http.ResponseWriter, r *http.Request, db DB) {
	if r.URL.String() != "/cmd/put" {
		t.Errorf("unsupported post request: %s", r.URL.String())
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	if r.Body == http.NoBody {
		t.Errorf("empty object\n")
		w.Header().Set("x-ddn-status", "207 EmptyObject")
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	oid := uuid.New().String()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("x-ddn-status", "203 InternalError")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	db.write(oid, body)
	w.Header().Set("x-ddn-status", "0 ok")
	w.Header().Set("x-ddn-oid", oid)
	w.WriteHeader(http.StatusOK)
}

func setupS3Server(bucket string) (*httptest.Server, error) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())

	// configure S3 client
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("u1", "s1", ""),
		Endpoint:         aws.String(ts.URL),
		Region:           aws.String("eu-central-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession := session.New(s3Config)

	s3Client := s3.New(newSession)
	cparams := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}

	_, err := s3Client.CreateBucket(cparams)
	if err != nil {
		return nil, err
	}

	return ts, nil
}

// func TestMigrate(t *testing.T) {
// 	bucket := "bucket1"
// 	keys := []string{"k1", "k2", "k3", "k4"}

// 	wos := setupWosServer(t, keys)
// 	defer wos.Close()

// 	ak := "u1"
// 	sk := "s1"
// 	s3, err := setupS3Server(bucket)
// 	if err != nil {
// 		t.Errorf("failed to prepare test data: %s", err.Error())
// 		return
// 	}
// 	defer s3.Close()

// 	runMigrateTest(t, s3.URL, ak, sk, bucket,
// 		strings.TrimPrefix(wos.URL, "http://"), nil, keys)
// }

// func TestMigrateMoreThan1Page(t *testing.T) {
// 	bucket := "bucket1"
// 	keys := []string{"k1", "k2", "k3", "k4"}
// 	ListPageSize = 2
// 	ak := "u1"
// 	sk := "s1"
// 	s3, err := setupS3Server(bucket, keys)
// 	if err != nil {
// 		t.Errorf("failed to prepare test data: %s", err.Error())
// 		return
// 	}
// 	defer s3.Close()

// 	wos := setupWosServer(t)
// 	defer wos.Close()

// 	runMigrateTest(t, s3.URL, ak, sk, bucket,
// 		strings.TrimPrefix(wos.URL, "http://"), nil, keys)
// }

// func TestMigrateEmptyBucket(t *testing.T) {
// 	bucket := "bucket1"
// 	keys := []string{}
// 	ak := "u1"
// 	sk := "s1"
// 	s3, err := setupS3Server(bucket, keys)
// 	if err != nil {
// 		t.Errorf("failed to prepare test data: %s", err.Error())
// 		return
// 	}
// 	defer s3.Close()

// 	wos := setupWosServer(t)
// 	defer wos.Close()

// 	runMigrateTest(t, s3.URL, ak, sk, bucket,
// 		strings.TrimPrefix(wos.URL, "http://"), nil, keys)
// }

func TestMigrateWithOidFile(t *testing.T) {
	file, err := ioutil.TempFile("", "oidfile")
	if err != nil {
		t.Errorf("failed to create retry file: %s", err.Error())
		return
	}
	defer os.Remove(file.Name())
	w := bufio.NewWriter(file)
	generateOidFileContent(w)
	bucket := "bucket1"
	keys := []string{
		"f423580f-cb2c-40b5-96db-a03553ab70b2",
		"f423580f-cb2c-40b5-96db-a03553ab70b3",
		"f423580f-cb2c-40b5-96db-a03553ab70b4",
		"f423580f-cb2c-40b5-96db-a03553ab70b5",
	}
	s3, err := setupS3Server(bucket)
	if err != nil {
		t.Errorf("failed to prepare test data: %s", err.Error())
		return
	}
	defer s3.Close()

	wos := setupWosServer(t, keys)
	defer wos.Close()
	ak := "u1"
	sk := "s1"

	retryF, err := os.Open(file.Name())
	if err != nil {
		t.Errorf("failed to open retry file %s: %s", file.Name(), err.Error())
		return
	}

	runMigrateTest(t, s3.URL, ak, sk, bucket,
		strings.TrimPrefix(wos.URL, "http://"), retryF, keys)
}

func TestMigrateWithRetry(t *testing.T) {
	file, err := ioutil.TempFile("", "retryfile")
	if err != nil {
		t.Errorf("failed to create retry file: %s", err.Error())
		return
	}
	defer os.Remove(file.Name())
	w := bufio.NewWriter(file)
	generateRetryFileContent(w)
	bucket := "bucket1"
	keys := []string{"k1", "k3"}
	s3, err := setupS3Server(bucket)
	if err != nil {
		t.Errorf("failed to prepare test data: %s", err.Error())
		return
	}
	defer s3.Close()

	wos := setupWosServer(t, keys)
	defer wos.Close()
	ak := "u1"
	sk := "s1"

	retryF, err := os.Open(file.Name())
	if err != nil {
		t.Errorf("failed to open retry file %s: %s", file.Name(), err.Error())
		return
	}

	runMigrateTest(t, s3.URL, ak, sk, bucket,
		strings.TrimPrefix(wos.URL, "http://"), retryF, keys)
}

func verifyReport(t *testing.T, report string, wantKeys []string) {
	entries := strings.Split(report, "\n")
	if len(entries) != len(wantKeys)+1 {
		t.Errorf("report size got %d;want %d:\n %s", len(entries), len(wantKeys)+1, report)
		return
	}
	resultKeys := make([]string, len(entries)-1)
	for i, e := range entries {
		if e == "" {
			continue
		}
		items := strings.Split(e, ",")
		if len(items) != 4 {
			t.Errorf("unexpected report entry: %s", e)
			continue
		}
		if items[1] != "ok" {
			t.Errorf("unexpected report status: %s", e)
			continue
		}

		if items[2] != "true" {
			t.Errorf("unexpected report verification status: %s", e)
			continue
		}
		resultKeys[i] = items[3]
	}
	sort.Strings(resultKeys)
	if !reflect.DeepEqual(wantKeys, resultKeys) {
		t.Errorf("unexpected report key list:\n%s", report)
	}
}

func runMigrateTest(t *testing.T, s3Endpoint, ak, sk, bucket string,
	wosEndpoint string, retryF *os.File,
	expectedKeys []string) {
	dest := storage.NewS3Storage(s3Endpoint, ak, sk, bucket)
	source := storage.NewWosStorage(wosEndpoint)
	report := &memWriter{}
	reportWriter := bufio.NewWriter(report)
	migrate(dest, source, reportWriter, retryF)

	verifyReport(t, string(report.data), expectedKeys)
}

func generateRetryFileContent(w *bufio.Writer) {
	fmt.Fprintln(w,
		"1577358017,fail,false,k1,f423580f-cb2c-40b5-96db-a03553ab70b2")
	fmt.Fprintln(w,
		"1577358017,ok,true,k2,f423580f-cb2c-40b5-96db-a03553ab70b3")
	fmt.Fprintln(w,
		"1577358017,fail,false,k3,f423580f-cb2c-40b5-96db-a03553ab70b4")
	fmt.Fprintln(w,
		"1577358017,ok,true,k4,f423580f-cb2c-40b5-96db-a03553ab70b5")
	w.Flush()
}

func generateOidFileContent(w *bufio.Writer) {
	fmt.Fprintln(w,
		"f423580f-cb2c-40b5-96db-a03553ab70b2")
	fmt.Fprintln(w,
		"f423580f-cb2c-40b5-96db-a03553ab70b3")
	fmt.Fprintln(w,
		"f423580f-cb2c-40b5-96db-a03553ab70b4")
	fmt.Fprintln(w,
		"f423580f-cb2c-40b5-96db-a03553ab70b5")
	w.Flush()
}
