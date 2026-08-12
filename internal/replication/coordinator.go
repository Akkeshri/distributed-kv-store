package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Akkeshri/distributed-kv-store/internal/conflict"
	"github.com/Akkeshri/distributed-kv-store/internal/metrics"
	"github.com/Akkeshri/distributed-kv-store/internal/ring"
	"github.com/Akkeshri/distributed-kv-store/internal/store"
)

type ReplicaClient interface {
	ReplicaSet(ctx context.Context, nodeID, address string, rec store.Record) error
	ReplicaGet(ctx context.Context, nodeID, address, key string) (store.Record, bool, error)
	ReplicaDelete(ctx context.Context, nodeID, address string, rec store.Record) error
}

type HTTPReplicaClient struct {
	client *http.Client
}

func NewHTTPReplicaClient() *HTTPReplicaClient {
	return &HTTPReplicaClient{client: &http.Client{Timeout: 5 * time.Second}}
}

type replicaWriteReq struct {
	Record store.Record `json:"record"`
}

type replicaReadResp struct {
	Found  bool         `json:"found"`
	Record store.Record `json:"record"`
}

func (c *HTTPReplicaClient) ReplicaSet(ctx context.Context, nodeID, address string, rec store.Record) error {
	return c.post(ctx, address+"/internal/replica/set", replicaWriteReq{Record: rec})
}

func (c *HTTPReplicaClient) ReplicaDelete(ctx context.Context, nodeID, address string, rec store.Record) error {
	return c.post(ctx, address+"/internal/replica/delete", replicaWriteReq{Record: rec})
}

func (c *HTTPReplicaClient) ReplicaGet(ctx context.Context, nodeID, address, key string) (store.Record, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address+"/internal/replica/get?key="+key, nil)
	if err != nil {
		return store.Record{}, false, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return store.Record{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return store.Record{}, false, fmt.Errorf("replica get failed: %s", string(body))
	}
	var out replicaReadResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return store.Record{}, false, err
	}
	return out.Record, out.Found, nil
}

func (c *HTTPReplicaClient) post(ctx context.Context, url string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("replica request failed: %s", string(b))
	}
	return nil
}

type Explain struct {
	Key              string   `json:"key"`
	Coordinator      string   `json:"coordinator"`
	Replicas         []string `json:"replicas"`
	QuorumRequired   int      `json:"quorumRequired"`
	Responses        int      `json:"responses"`
	Acks             int      `json:"acks,omitempty"`
	ConflictResolved bool     `json:"conflictResolved"`
	Conflict         bool     `json:"conflict,omitempty"`
}

type GetResult struct {
	Key      string         `json:"key"`
	Value    string         `json:"value"`
	Found    bool           `json:"found"`
	Version  store.Version  `json:"version,omitempty"`
	Replicas []string       `json:"replicas"`
	Conflict bool           `json:"conflict"`
	Explain  *Explain       `json:"explain,omitempty"`
}

type SetResult struct {
	Key      string        `json:"key"`
	Value    string        `json:"value"`
	Version  store.Version `json:"version"`
	Replicas []string      `json:"replicas"`
	Success  bool          `json:"success"`
	Explain  *Explain      `json:"explain,omitempty"`
}

type Coordinator struct {
	nodeID       string
	ring         *ring.Ring
	client       ReplicaClient
	localStore   *store.Store
	resolver     *conflict.Resolver
	metrics      *metrics.Registry
	readQuorum   int
	writeQuorum  int
	readRepair   bool
	localAddress func(nodeID string) string
}

func NewCoordinator(nodeID string, r *ring.Ring, local *store.Store, client ReplicaClient, resolver *conflict.Resolver, m *metrics.Registry, readQ, writeQ int, readRepair bool, addrFn func(string) string) *Coordinator {
	return &Coordinator{
		nodeID:       nodeID,
		ring:         r,
		client:       client,
		localStore:   local,
		resolver:     resolver,
		metrics:      m,
		readQuorum:   readQ,
		writeQuorum:  writeQ,
		readRepair:   readRepair,
		localAddress: addrFn,
	}
}

func (c *Coordinator) Set(ctx context.Context, key, value string, explain bool) (SetResult, error) {
	c.metrics.RequestsTotal.Add(1)
	replicas := c.ring.GetReplicas(key, true)
	if len(replicas) == 0 {
		c.metrics.QuorumFailuresTotal.Add(1)
		return SetResult{}, fmt.Errorf("no replicas available for key")
	}

	rec, err := c.localStore.Set(key, value)
	if err != nil {
		return SetResult{}, err
	}

	acks := 0
	if contains(replicas, c.nodeID) {
		acks++
	}

	for _, repID := range replicas {
		if repID == c.nodeID {
			continue
		}
		addr := c.localAddress(repID)
		if addr == "" {
			continue
		}
		if err := c.client.ReplicaSet(ctx, repID, addr, rec); err == nil {
			acks++
		}
	}

	result := SetResult{
		Key:      key,
		Value:    value,
		Version:  rec.Version,
		Replicas: replicas,
		Success:  acks >= c.writeQuorum,
	}
	if explain {
		result.Explain = &Explain{
			Key:            key,
			Coordinator:    c.nodeID,
			Replicas:       replicas,
			QuorumRequired: c.writeQuorum,
			Responses:      len(replicas),
			Acks:           acks,
		}
	}
	if !result.Success {
		c.metrics.QuorumFailuresTotal.Add(1)
		return result, fmt.Errorf("write quorum not met: got %d need %d", acks, c.writeQuorum)
	}
	return result, nil
}

func (c *Coordinator) Delete(ctx context.Context, key string, explain bool) (SetResult, error) {
	c.metrics.RequestsTotal.Add(1)
	replicas := c.ring.GetReplicas(key, true)
	if len(replicas) == 0 {
		c.metrics.QuorumFailuresTotal.Add(1)
		return SetResult{}, fmt.Errorf("no replicas available for key")
	}

	rec, err := c.localStore.Delete(key)
	if err != nil {
		return SetResult{}, err
	}

	acks := 0
	if contains(replicas, c.nodeID) {
		acks++
	}
	for _, repID := range replicas {
		if repID == c.nodeID {
			continue
		}
		addr := c.localAddress(repID)
		if addr == "" {
			continue
		}
		if err := c.client.ReplicaDelete(ctx, repID, addr, rec); err == nil {
			acks++
		}
	}

	result := SetResult{
		Key:      key,
		Replicas: replicas,
		Success:  acks >= c.writeQuorum,
		Version:  rec.Version,
	}
	if explain {
		result.Explain = &Explain{
			Key:            key,
			Coordinator:    c.nodeID,
			Replicas:       replicas,
			QuorumRequired: c.writeQuorum,
			Responses:      len(replicas),
			Acks:           acks,
		}
	}
	if !result.Success {
		c.metrics.QuorumFailuresTotal.Add(1)
		return result, fmt.Errorf("write quorum not met: got %d need %d", acks, c.writeQuorum)
	}
	return result, nil
}

func (c *Coordinator) Get(ctx context.Context, key string, explain bool) (GetResult, error) {
	c.metrics.RequestsTotal.Add(1)
	replicas := c.ring.GetReplicas(key, true)
	if len(replicas) == 0 {
		c.metrics.QuorumFailuresTotal.Add(1)
		return GetResult{}, fmt.Errorf("no replicas available for key")
	}

	var records []store.Record
	responses := 0
	for _, repID := range replicas {
		var rec store.Record
		var found bool
		var err error
		if repID == c.nodeID {
			rec, found = c.localStore.GetRaw(key)
		} else {
			addr := c.localAddress(repID)
			if addr == "" {
				continue
			}
			rec, found, err = c.client.ReplicaGet(ctx, repID, addr, key)
			if err != nil {
				continue
			}
		}
		responses++
		if found && !rec.Tombstone {
			records = append(records, rec)
		}
	}

	if responses < c.readQuorum {
		c.metrics.QuorumFailuresTotal.Add(1)
		return GetResult{}, fmt.Errorf("read quorum not met: got %d need %d", responses, c.readQuorum)
	}

	resolved := c.resolver.Resolve(records)
	if resolved.Conflict {
		c.metrics.ConflictsTotal.Add(1)
	}

	if c.readRepair && resolved.Resolved && len(records) > 1 {
		c.readRepairReplicas(ctx, replicas, resolved.Record)
	}

	result := GetResult{
		Key:      key,
		Found:    resolved.Resolved && !resolved.Record.Tombstone,
		Value:    resolved.Record.Value,
		Version:  resolved.Record.Version,
		Replicas: replicas,
		Conflict: resolved.Conflict,
	}
	if explain {
		result.Explain = &Explain{
			Key:              key,
			Coordinator:      c.nodeID,
			Replicas:         replicas,
			QuorumRequired:   c.readQuorum,
			Responses:        responses,
			ConflictResolved: resolved.Conflict,
			Conflict:         resolved.Conflict,
		}
	}
	return result, nil
}

func (c *Coordinator) readRepairReplicas(ctx context.Context, replicas []string, winner store.Record) {
	for _, repID := range replicas {
		if repID == c.nodeID {
			_ = c.localStore.ApplyReplica(winner)
			continue
		}
		addr := c.localAddress(repID)
		if addr == "" {
			continue
		}
		_ = c.client.ReplicaSet(ctx, repID, addr, winner)
	}
}

func contains(list []string, item string) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}
