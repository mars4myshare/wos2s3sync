# s3sync
Sync tool to migrate data from s3 storage to other storage

## Build
```make```

## Run
* Normal
```
./s3syncwos --ak uniquser1 --sk changemechangeme -endpoint http://127.0.0.1:19000 -bucket bucket1 -wos 127.0.0.1:39000 -report /tmp/oid.list -wospolicy dev
```

* With marker
```
./s3syncwos --ak uniquser1 --sk changemechangeme -endpoint http://127.0.0.1:19000 -bucket bucket1 -wos 127.0.0.1:39000 -report /tmp/oid.list -wospolicy dev -mark f1
```

* retry
```
./s3syncwos --ak uniquser1 --sk changemechangeme -endpoint http://127.0.0.1:19000 -bucket bucket1 -wos 127.0.0.1:39000 -report /tmp/oid.list -wospolicy dev -retryfile /tmp/oldoid.list
```

* Some other env
```
APP_WORKER: how many concurrent worker, 16 defaul
APP_TIMEOUT: the http timeout seconds, 300s default. Including list objects, read object and write object
APP_LEVEL: set log level to DEBUG 
```


## Report
* Format
```
timestamp,status,verified,s3_key,wos_oid,failure_reason
```

* Sample
```
1577088611,fail,false,f3,,Post http://127.0.0.1:39000/cmd/put: dial tcp 127.0.0.1:39000: connect: connection refused
1577088930,ok,true,f11,5515780e-e3e9-46a0-97a3-720a4ef4ab63
1577088930,ok,true,f1205,38a63875-f66f-4664-9d7b-d320d5f1e830
1577088930,ok,true,f1,aa274a48-5d1b-48eb-a966-cc9a55c1dadb
1577088930,ok,true,f1206,1eb4766c-96e7-4324-a054-37cf165f7110
1577088930,ok,true,f1207,fe18e7f1-2786-40e8-8e4c-b75c28d5b99a
1577088930,ok,true,f3,13b45009-e9e7-4ab8-a0df-95673407d72a
```
