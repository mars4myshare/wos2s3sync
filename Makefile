BINARY=s3syncwos
BUILDDATE=$(shell date +'%Y-%m-%dT%H:%M:%SZ')
VERSION=1.0.$(shell git rev-list HEAD --count)
LONGVER=${VERSION}@${BUILDDATE}

LDFLAGS=-ldflags "-X main.version=${LONGVER}"

.DEFAULT_GOAL:=default

test:
	go test -race ./... -count=1

testall:
	go test -race -tags=integration ./... --timeout 30m

vet:
	go vet ./...
cover:
	go test -race -tags=integration -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

bench:
	go test -race -bench=. ./... -benchmem


linux64:
	@echo "Building ${BINARY}-${VERSION}"
	env GOOS=linux GOARCH=amd64 go build ${LDFLAGS} -o ${BINARY}-linux .

default:
	@echo "Building ${BINARY}-${VERSION}"
	go build ${LDFLAGS} -o ${BINARY} .

install: default
	install ${BINARY} /usr/local/bin/

clean:
	go clean ./...
	rm -f ./${BINARY}
	rm -f ./${BINARY}-linux


.PHONY: pkg test vet cover bench default install clean testall
