package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Akkeshri/distributed-kv-store/internal/config"
	"github.com/Akkeshri/distributed-kv-store/internal/node"
	"github.com/Akkeshri/distributed-kv-store/internal/replication"
	"github.com/Akkeshri/distributed-kv-store/internal/ring"
	"github.com/Akkeshri/distributed-kv-store/internal/store"
)

type Cluster struct {
	mu      sync.RWMutex
	nodes   map[string]*node.Node
	ring    *ring.Ring
	cfg     config.Config
	logger  *slog.Logger
	dataDir string
}

func New(baseCfg config.Config, logger *slog.Logger) *Cluster {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	r := ring.New(baseCfg.ReplicationFactor, baseCfg.VirtualNodesPerNode)
	return &Cluster{
		nodes:   make(map[string]*node.Node),
		ring:    r,
		cfg:     baseCfg,
		logger:  logger,
		dataDir: baseCfg.DataDir,
	}
}

func (c *Cluster) StartNodes(count int) error {
	for i := 1; i <= count; i++ {
		id := fmt.Sprintf("node-%d", i)
		port, err := freePort()
		if err != nil {
			return err
		}
		addr := fmt.Sprintf("http://127.0.0.1:%d", port)
		if err := c.AddNode(id, addr); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) AddNode(id, address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodes[id]; exists {
		return fmt.Errorf("node %s already exists", id)
	}

	cfg := c.cfg
	cfg.NodeID = id
	cfg.Address = address
	cfg.DataDir = c.dataDir

	n, err := node.New(cfg, c.ring, c.addressLookup, c.logger)
	if err != nil {
		return err
	}
	c.nodes[id] = n

	go func() {
		if err := n.Start(); err != nil && err != http.ErrServerClosed {
			c.logger.Error("node failed", "nodeId", id, "err", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (c *Cluster) RemoveNode(id string) error {
	c.mu.Lock()
	n, ok := c.nodes[id]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("node not found")
	}
	delete(c.nodes, id)
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.ring.RemoveNode(id)
	return n.Shutdown(ctx)
}

func (c *Cluster) addressLookup(nodeID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		info, ok := c.ring.GetNode(nodeID)
		if !ok {
			return ""
		}
		return info.Address
	}
	return n.Config().Address
}

func (c *Cluster) Node(id string) (*node.Node, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.nodes[id]
	return n, ok
}

func (c *Cluster) Nodes() map[string]*node.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]*node.Node, len(c.nodes))
	for k, v := range c.nodes {
		out[k] = v
	}
	return out
}

func (c *Cluster) Ring() *ring.Ring { return c.ring }

func (c *Cluster) FailNode(id string) error {
	n, ok := c.Node(id)
	if !ok {
		return fmt.Errorf("node not found")
	}
	n.SimulateFail()
	return nil
}

func (c *Cluster) RecoverNode(id string) error {
	n, ok := c.Node(id)
	if !ok {
		return fmt.Errorf("node not found")
	}
	n.SimulateRecover()
	return nil
}

func (c *Cluster) RebalanceOnJoin(ctx context.Context, joinedID string) int {
	stores := map[string]*store.Store{}
	for id, n := range c.Nodes() {
		stores[id] = n.Store()
	}
	recordFn := replication.NewRecordProvider(stores)
	client := replication.NewHTTPReplicaClient()
	moved := 0
	for _, n := range c.Nodes() {
		if n.Config().NodeID == joinedID {
			continue
		}
		for _, key := range n.Store().Keys() {
			replicas := c.ring.GetReplicas(key, false)
			if len(replicas) == 0 || replicas[0] != joinedID {
				continue
			}
			rec, ok := recordFn(n.Config().NodeID, key)
			if !ok || rec.Tombstone {
				continue
			}
			addr := c.addressLookup(joinedID)
			if err := client.ReplicaSet(ctx, joinedID, addr, rec); err == nil {
				moved++
			}
		}
	}
	return moved
}

func (c *Cluster) Shutdown() {
	c.mu.Lock()
	nodes := c.nodes
	c.nodes = make(map[string]*node.Node)
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, n := range nodes {
		_ = n.Shutdown(ctx)
	}
}

func (c *Cluster) AnyAddress() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, n := range c.nodes {
		return n.Config().Address
	}
	return ""
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
