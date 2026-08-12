# distributed-kv-store

In-process multi-node KV store with consistent hashing, replication, and quorum reads/writes.

## Run

```bash
make build
make run-cluster
```

## Try it

```bash
./bin/kvctl set greeting hello
./bin/kvctl get greeting
```

Single node:

```bash
go run ./cmd/node --node-id node-1 --address http://127.0.0.1:8080
```
