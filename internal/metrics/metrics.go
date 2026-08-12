package metrics

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

type Registry struct {
	RequestsTotal       atomic.Int64
	QuorumFailuresTotal atomic.Int64
	ConflictsTotal      atomic.Int64
	RebalanceKeysMoved  atomic.Int64
}

func NewRegistry() *Registry {
	return &Registry{}
}

type Snapshot struct {
	KVRequestsTotal         int64            `json:"kv_requests_total"`
	KVQuorumFailuresTotal   int64            `json:"kv_quorum_failures_total"`
	KVConflictsTotal        int64            `json:"kv_conflicts_total"`
	KVRebalanceKeysMoved    int64            `json:"kv_rebalance_keys_moved_total"`
	KVNodeStatus            map[string]string `json:"kv_node_status"`
}

func (r *Registry) Snapshot(nodeStatus map[string]string) Snapshot {
	return Snapshot{
		KVRequestsTotal:       r.RequestsTotal.Load(),
		KVQuorumFailuresTotal: r.QuorumFailuresTotal.Load(),
		KVConflictsTotal:      r.ConflictsTotal.Load(),
		KVRebalanceKeysMoved:  r.RebalanceKeysMoved.Load(),
		KVNodeStatus:          nodeStatus,
	}
}

func (r *Registry) Handler(nodeStatus func() map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.Snapshot(nodeStatus()))
	}
}
