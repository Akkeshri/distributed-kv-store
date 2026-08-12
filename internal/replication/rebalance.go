package replication

import (
	"context"
	"log/slog"

	"github.com/Akkeshri/distributed-kv-store/internal/metrics"
	"github.com/Akkeshri/distributed-kv-store/internal/ring"
	"github.com/Akkeshri/distributed-kv-store/internal/store"
)

type Rebalancer struct {
	ring    *ring.Ring
	metrics *metrics.Registry
	getKeys func(nodeID string) []string
	apply   func(ctx context.Context, targetNodeID, targetAddr string, rec store.Record) error
	addrFn  func(nodeID string) string
	logger  *slog.Logger
}

func NewRebalancer(r *ring.Ring, m *metrics.Registry, getKeys func(string) []string, apply func(context.Context, string, string, store.Record) error, addrFn func(string) string, logger *slog.Logger) *Rebalancer {
	return &Rebalancer{
		ring:    r,
		metrics: m,
		getKeys: getKeys,
		apply:   apply,
		addrFn:  addrFn,
		logger:  logger,
	}
}

func (rb *Rebalancer) OnNodeJoin(ctx context.Context, joinedNodeID string) int {
	moved := 0
	allNodes := rb.ring.Nodes()
	for _, n := range allNodes {
		if n.ID == joinedNodeID {
			continue
		}
		keys := rb.getKeys(n.ID)
		for _, key := range keys {
			replicas := rb.ring.GetReplicas(key, false)
			if !contains(replicas, joinedNodeID) {
				continue
			}
			if replicas[0] != joinedNodeID {
				continue
			}
			addr := rb.addrFn(joinedNodeID)
			if addr == "" {
				continue
			}
			rec, ok := rb.getRecord(n.ID, key)
			if !ok {
				continue
			}
			if err := rb.apply(ctx, joinedNodeID, addr, rec); err == nil {
				moved++
				rb.metrics.RebalanceKeysMoved.Add(1)
			}
		}
	}
	rb.logger.Info("rebalance on join complete", "nodeId", joinedNodeID, "keysMoved", moved)
	return moved
}

func (rb *Rebalancer) getRecord(nodeID, key string) (store.Record, bool) {
	keys := rb.getKeys(nodeID)
	for _, k := range keys {
		if k == key {
			return store.Record{Key: key}, true
		}
	}
	return store.Record{}, false
}

type keyProvider struct {
	stores map[string]*store.Store
}

func NewKeyProvider(stores map[string]*store.Store) func(nodeID string) []string {
	return func(nodeID string) []string {
		s, ok := stores[nodeID]
		if !ok {
			return nil
		}
		return s.Keys()
	}
}

func NewRecordProvider(stores map[string]*store.Store) func(nodeID, key string) (store.Record, bool) {
	return func(nodeID, key string) (store.Record, bool) {
		s, ok := stores[nodeID]
		if !ok {
			return store.Record{}, false
		}
		return s.GetRaw(key)
	}
}
