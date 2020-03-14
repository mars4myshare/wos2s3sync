package main

import (
	"bufio"
	"flag"
	"os"
	"s3sync/storage"
	"strconv"

	log "github.com/sirupsen/logrus"
)

var (
	SyncWorkerCnt = 16
	ListPageSize  = 1000
)

func main() {
	ak := flag.String("ak", "", "access key")
	sk := flag.String("sk", "", "secret key")
	endpoint := flag.String("endpoint", "", "secret key")
	bucket := flag.String("bucket", "", "source bucket")
	destHost := flag.String("wos", "", "dest storage")
	reportFile := flag.String("report", "", "sync report")
	oidFile := flag.String("oidfile", "", "oid file or previous report file when retry")
	flag.Parse()
	if *ak == "" ||
		*sk == "" ||
		*endpoint == "" ||
		*bucket == "" ||
		*destHost == "" ||
		*oidFile == "" ||
		*reportFile == "" {
		flag.Usage()
		log.Fatal("missing access key, secret key, endpoint, bucket, dest host, oid list or report file")
	}

	file, err := os.OpenFile(*reportFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open report file(%s): %s", *reportFile, err.Error())
	}
	reportWriter := bufio.NewWriter(file)
	defer file.Close()
	log.Infof("Migrating data from %s/%s to %s with %d worker...",
		*endpoint, *bucket, *destHost, SyncWorkerCnt)
	dest := storage.NewS3Storage(*endpoint, *ak, *sk, *bucket)
	source := storage.NewWosStorage(*destHost)

	oidFH, err := os.Open(*oidFile)
	if err != nil {
		log.Fatalf("failed to open %s: %s", *oidFile, err.Error())
	}
	defer oidFH.Close()
	migrate(dest, source, reportWriter, oidFH)
}

func init() {
	log.SetOutput(os.Stdout)
	logLevel := os.Getenv("APP_LEVEL")
	if logLevel == "DEBUG" {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	workerCnt := os.Getenv("APP_WORKER")
	if workerCnt != "" {
		i, err := strconv.Atoi(workerCnt)
		if err != nil {
			log.Errorf("invalid worker count: %s, skip", workerCnt)
		} else {
			SyncWorkerCnt = i
		}
	}
}
