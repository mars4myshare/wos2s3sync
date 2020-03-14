package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"s3sync/storage"

	log "github.com/sirupsen/logrus"
)

type syncObjItem struct {
	key string
}

type syncResult struct {
	err      error
	verified bool
	oldKey   string
}

// record
// format: ts, sync status, verify status, old key[, error]
func (t *syncResult) record(w *bufio.Writer) {

	if t.err != nil {
		w.WriteString(fmt.Sprintf("%d,fail,false,%s,%s\n",
			time.Now().Unix(), t.oldKey, strings.ReplaceAll(t.err.Error(), "\n", " ")))
	} else {
		w.WriteString(fmt.Sprintf("%d,ok,%t,%s\n",
			time.Now().Unix(), t.verified, t.oldKey))
	}

	w.Flush()
}

func syncObject(syncObj syncObjItem, target storage.StorDest, source storage.StorSrc) syncResult {
	log.Debugf("retriving object: %s", syncObj.key)
	r, err := source.Read(syncObj.key)
	if err != nil {
		return syncResult{oldKey: syncObj.key, err: err}
	}
	log.Debugf("retrived object: %s", syncObj.key)

	log.Debugf("writing object: %s", syncObj.key)
	originMD5, err := target.Write(syncObj.key, r)
	if err != nil {
		return syncResult{oldKey: syncObj.key, err: err}
	}
	log.Debugf("wrote object: %s", syncObj.key)

	log.Debugf("verifying object: %s", syncObj.key)
	targetObj, err := target.Read(syncObj.key)
	if err != nil {
		return syncResult{oldKey: syncObj.key, err: err}
	}
	targetMD5, err := storage.CalcMD5(targetObj.GetBody())
	if err != nil {
		return syncResult{oldKey: syncObj.key, err: err}
	}

	if targetMD5 != originMD5 {
		log.Debugf("failed to verify object %s md5: %s, %s", syncObj.key, originMD5, targetMD5)
		return syncResult{verified: false, oldKey: syncObj.key}
	}
	return syncResult{verified: true, oldKey: syncObj.key}
}

func migrate(
	dest storage.StorDest,
	source storage.StorSrc,
	w *bufio.Writer,
	oidFile *os.File) {
	var stop = make(chan struct{})
	var totalObjectsNum = make(chan int)
	var result = make(chan syncResult, SyncWorkerCnt)
	var toSyncObjs = make(chan syncObjItem, SyncWorkerCnt)

	go getObjListFromFile(oidFile, totalObjectsNum, toSyncObjs)

	for i := 0; i < SyncWorkerCnt; i++ {
		go syncWorker(stop, result, toSyncObjs, dest, source)
	}
	go monitor(totalObjectsNum, result, stop, w)

	<-stop
}

func getObjListFromFile(oidFile *os.File, totalNum chan<- int, toSyncObjs chan<- syncObjItem) {
	if oidFile == nil {
		log.Errorf("no oid file provided")
		return
	}
	total := 0
	rd := bufio.NewReader(oidFile)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Infof("Total objects to be migrated: %d", total)
				totalNum <- total
				return
			}
			log.Errorf("failed to parse result file: %s", err.Error())
			totalNum <- total
			return
		}

		//1577092932,ok,false,file_mpu10,7852f675-458e-49ea-a4b2-e8477b715d1b
		parts := strings.Split(line, ",")
		var key string
		if len(parts) == 1 {
			// oid list
			key = strings.TrimSpace(parts[0])
		} else {
			if len(parts) < 4 {
				log.Errorf("failed to parse result entry: %s, skip", line)
				continue
			}
			if parts[1] == "ok" {
				log.Debugf("migrated object %s, skip", parts[3])
				continue
			}
			key = strings.TrimSpace(parts[3])
		}

		if key == "" {
			log.Errorf("emtpy object name: %s, skip", line)
			continue
		}

		toSyncObjs <- syncObjItem{
			key: key,
		}
		total++
	}
}

func syncWorker(
	stop <-chan struct{},
	result chan<- syncResult,
	toSyncObjs <-chan syncObjItem,
	dest storage.StorDest,
	source storage.StorSrc,
) {
	for {
		select {
		case t := <-toSyncObjs:
			result <- syncObject(t, dest, source)
		case <-stop:
			return
		}
	}
}

func monitor(totalNum <-chan int, result <-chan syncResult, stop chan<- struct{}, w *bufio.Writer) {
	finished := 0
	pass := 0
	totalTasksNum := -1
	for {
		select {
		case r := <-result:
			r.record(w)
			if r.err == nil {
				pass++
			}
			finished++
			if totalTasksNum > 0 && finished >= totalTasksNum {
				log.Infof("Migration Completed: %d/%d", pass, finished)
				close(stop)
			}
		case totalTasksNum = <-totalNum:
			if totalTasksNum == 0 {
				log.Warnf("No objects to be migrated")
				close(stop)
				return
			}
			if totalTasksNum <= finished {
				log.Infof("Migration Completed: %d/%d", pass, finished)
				close(stop)
			}
		}
	}
}
