.PHONY: build run-cluster clean

build:
	go build -o bin/node ./cmd/node
	go build -o bin/kvctl ./cmd/kvctl
	go build -o bin/kvcluster ./cmd/kvcluster

run-cluster:
	go run ./cmd/kvcluster start --nodes 3 --replication 3 --read-quorum 2 --write-quorum 2

clean:
	rm -rf bin/ data/
