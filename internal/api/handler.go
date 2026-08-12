package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Akkeshri/distributed-kv-store/internal/membership"
	"github.com/Akkeshri/distributed-kv-store/internal/metrics"
	"github.com/Akkeshri/distributed-kv-store/internal/replication"
	"github.com/Akkeshri/distributed-kv-store/internal/ring"
	"github.com/Akkeshri/distributed-kv-store/internal/store"
)

type Handler struct {
	coordinator *replication.Coordinator
	store       *store.Store
	ring        *ring.Ring
	metrics     *metrics.Registry
	membership  *membership.Manager
	nodeID      string
	logger      *slog.Logger
}

func NewHandler(coord *replication.Coordinator, st *store.Store, r *ring.Ring, m *metrics.Registry, mem *membership.Manager, nodeID string, logger *slog.Logger) *Handler {
	return &Handler{
		coordinator: coord,
		store:       st,
		ring:        r,
		metrics:     m,
		membership:  mem,
		nodeID:      nodeID,
		logger:      logger,
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", h.handleKV)
	mux.HandleFunc("/kv/range", h.handleRange)
	mux.HandleFunc("/cluster/state", h.handleClusterState)
	mux.HandleFunc("/cluster/nodes", h.handleAddNode)
	mux.HandleFunc("/cluster/nodes/", h.handleRemoveNode)
	mux.HandleFunc("/cluster/ring/explain/", h.handleExplain)
	mux.HandleFunc("/metrics", h.metrics.Handler(func() map[string]string {
		if h.membership != nil {
			return h.membership.NodeStatuses()
		}
		return map[string]string{}
	}))
	mux.HandleFunc("/internal/replica/set", h.handleReplicaSet)
	mux.HandleFunc("/internal/replica/delete", h.handleReplicaDelete)
	mux.HandleFunc("/internal/replica/get", h.handleReplicaGet)
	mux.HandleFunc("/internal/heartbeat", h.handleHeartbeat)
	mux.HandleFunc("/admin/fail", h.handleAdminFail)
	mux.HandleFunc("/admin/recover", h.handleAdminRecover)
	mux.HandleFunc("/dashboard", h.handleDashboard)
	return mux
}

type setRequest struct {
	Value string `json:"value"`
}

func (h *Handler) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "invalid key path", http.StatusBadRequest)
		return
	}
	explain := r.URL.Query().Get("explain") == "true"

	switch r.Method {
	case http.MethodGet:
		result, err := h.coordinator.Get(r.Context(), key, explain)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if !result.Found {
			w.WriteHeader(http.StatusNotFound)
		}
		writeJSON(w, result)
	case http.MethodPut:
		var req setRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		result, err := h.coordinator.Set(r.Context(), key, req.Value, explain)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, result)
	case http.MethodDelete:
		result, err := h.coordinator.Delete(r.Context(), key, explain)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, result)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	recs := h.store.Range(start, end)
	writeJSON(w, map[string]interface{}{"records": recs})
}

func (h *Handler) handleClusterState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, map[string]interface{}{
		"clusterName":       "local-kv",
		"coordinatorNodeId": h.nodeID,
		"nodes":             h.ring.Nodes(),
		"tokens":            h.ring.AllTokens(),
		"metrics":           h.metrics.Snapshot(h.membership.NodeStatuses()),
	})
}

type addNodeRequest struct {
	NodeID  string `json:"nodeId"`
	Address string `json:"address"`
}

func (h *Handler) handleAddNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req addNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	h.ring.AddNode(req.NodeID, req.Address)
	writeJSON(w, map[string]string{"status": "added", "nodeId": req.NodeID})
}

func (h *Handler) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/cluster/nodes/")
	if id == "" {
		http.Error(w, "node id required", http.StatusBadRequest)
		return
	}
	h.ring.RemoveNode(id)
	writeJSON(w, map[string]string{"status": "removed", "nodeId": id})
}

func (h *Handler) handleExplain(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/cluster/ring/explain/")
	writeJSON(w, h.ring.Explain(key))
}

func (h *Handler) handleReplicaSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Record store.Record `json:"record"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err := h.store.ApplyReplica(req.Record); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *Handler) handleReplicaDelete(w http.ResponseWriter, r *http.Request) {
	h.handleReplicaSet(w, r)
}

func (h *Handler) handleReplicaGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	rec, found := h.store.GetRaw(key)
	writeJSON(w, map[string]interface{}{"found": found, "record": rec})
}

func (h *Handler) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	from := r.Header.Get("X-Node-ID")
	if from == "" {
		http.Error(w, "missing node id", http.StatusBadRequest)
		return
	}
	h.membership.RecordHeartbeat(from)
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *Handler) handleAdminFail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.membership.SimulateFail(h.nodeID)
	h.logger.Warn("node simulated failure", "nodeId", h.nodeID)
	writeJSON(w, map[string]string{"status": "failed", "nodeId": h.nodeID})
}

func (h *Handler) handleAdminRecover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.membership.SimulateRecover(h.nodeID)
	h.logger.Info("node simulated recovery", "nodeId", h.nodeID)
	writeJSON(w, map[string]string{"status": "recovering", "nodeId": h.nodeID})
}

func (h *Handler) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	nodes := h.ring.Nodes()
	html := `<!DOCTYPE html><html><head><title>KV Cluster Dashboard</title>
<style>
body{font-family:system-ui,sans-serif;margin:2rem;background:#0f172a;color:#e2e8f0}
h1{color:#38bdf8}table{border-collapse:collapse;width:100%;margin:1rem 0}
th,td{border:1px solid #334155;padding:.5rem;text-align:left}
.UP{color:#4ade80}.SUSPECT{color:#facc15}.DOWN{color:#f87171}.RECOVERING{color:#60a5fa}
.card{background:#1e293b;padding:1rem;border-radius:8px;margin-bottom:1rem}
</style></head><body>
<h1>Distributed KV Store Dashboard</h1>
<div class="card"><strong>Coordinator:</strong> ` + h.nodeID + `</div>
<h2>Nodes</h2><table><tr><th>ID</th><th>Address</th><th>Status</th><th>Tokens</th></tr>`
	for _, n := range nodes {
		html += `<tr><td>` + n.ID + `</td><td>` + n.Address + `</td><td class="` + string(n.Status) + `">` + string(n.Status) + `</td><td>` + itoa(len(n.Tokens)) + `</td></tr>`
	}
	html += `</table><h2>Metrics</h2><pre>`
	m, _ := json.MarshalIndent(h.metrics.Snapshot(h.membership.NodeStatuses()), "", "  ")
	html += string(m)
	html += `</pre><p><a href="/cluster/state" style="color:#38bdf8">JSON cluster state</a> | <a href="/metrics" style="color:#38bdf8">Metrics JSON</a></p></body></html>`
	w.Write([]byte(html))
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
