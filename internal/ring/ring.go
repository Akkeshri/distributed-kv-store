package ring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

type NodeStatus string

const (
	StatusUP         NodeStatus = "UP"
	StatusSUSPECT    NodeStatus = "SUSPECT"
	StatusDOWN       NodeStatus = "DOWN"
	StatusRECOVERING NodeStatus = "RECOVERING"
)

type NodeInfo struct {
	ID      string     `json:"nodeId"`
	Address string     `json:"address"`
	Status  NodeStatus `json:"status"`
	Tokens  []uint32   `json:"tokens"`
}

type tokenEntry struct {
	token  uint32
	nodeID string
}

type Ring struct {
	mu                  sync.RWMutex
	virtualNodesPerNode int
	replicationFactor   int
	tokens              []tokenEntry
	nodes               map[string]NodeInfo
}

func New(replicationFactor, virtualNodesPerNode int) *Ring {
	return &Ring{
		virtualNodesPerNode: virtualNodesPerNode,
		replicationFactor:   replicationFactor,
		nodes:               make(map[string]NodeInfo),
	}
}

func HashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func (r *Ring) AddNode(id, address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[id]; exists {
		return
	}

	tokens := make([]uint32, 0, r.virtualNodesPerNode)
	for i := 0; i < r.virtualNodesPerNode; i++ {
		token := HashKey(fmt.Sprintf("%s#%d", id, i))
		tokens = append(tokens, token)
		r.tokens = append(r.tokens, tokenEntry{token: token, nodeID: id})
	}
	sort.Slice(r.tokens, func(i, j int) bool {
		return r.tokens[i].token < r.tokens[j].token
	})
	r.nodes[id] = NodeInfo{ID: id, Address: address, Status: StatusUP, Tokens: tokens}
}

func (r *Ring) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.nodes, id)
	filtered := r.tokens[:0]
	for _, t := range r.tokens {
		if t.nodeID != id {
			filtered = append(filtered, t)
		}
	}
	r.tokens = filtered
}

func (r *Ring) SetNodeStatus(id string, status NodeStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[id]; ok {
		n.Status = status
		r.nodes[id] = n
	}
}

func (r *Ring) GetNode(id string) (NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	return n, ok
}

func (r *Ring) Nodes() []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NodeInfo, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Ring) IsNodeHealthy(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return false
	}
	return n.Status == StatusUP || n.Status == StatusRECOVERING
}

func (r *Ring) GetReplicas(key string, skipUnhealthy bool) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.tokens) == 0 {
		return nil
	}

	hash := HashKey(key)
	idx := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i].token >= hash
	})
	if idx == len(r.tokens) {
		idx = 0
	}

	seen := map[string]struct{}{}
	replicas := make([]string, 0, r.replicationFactor)
	for i := 0; i < len(r.tokens) && len(replicas) < r.replicationFactor; i++ {
		pos := (idx + i) % len(r.tokens)
		nodeID := r.tokens[pos].nodeID
		if skipUnhealthy {
			n := r.nodes[nodeID]
			if n.Status == StatusDOWN || n.Status == StatusSUSPECT {
				continue
			}
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		replicas = append(replicas, nodeID)
	}
	return replicas
}

func (r *Ring) Explain(key string) map[string]interface{} {
	replicas := r.GetReplicas(key, false)
	healthy := r.GetReplicas(key, true)
	return map[string]interface{}{
		"key":           key,
		"hash":          HashKey(key),
		"replicas":      replicas,
		"healthyReplicas": healthy,
		"replicationFactor": r.replicationFactor,
	}
}

func (r *Ring) ReplicationFactor() int {
	return r.replicationFactor
}

func (r *Ring) AllTokens() []map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]map[string]interface{}, len(r.tokens))
	for i, t := range r.tokens {
		out[i] = map[string]interface{}{
			"token":  t.token,
			"nodeId": t.nodeID,
		}
	}
	return out
}
