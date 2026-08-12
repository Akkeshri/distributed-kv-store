package node

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/Akkeshri/distributed-kv-store/internal/api"
	"github.com/Akkeshri/distributed-kv-store/internal/config"
	"github.com/Akkeshri/distributed-kv-store/internal/conflict"
	"github.com/Akkeshri/distributed-kv-store/internal/membership"
	"github.com/Akkeshri/distributed-kv-store/internal/metrics"
	"github.com/Akkeshri/distributed-kv-store/internal/replication"
	"github.com/Akkeshri/distributed-kv-store/internal/ring"
	"github.com/Akkeshri/distributed-kv-store/internal/store"
)

type Node struct {
	cfg         config.Config
	ring        *ring.Ring
	store       *store.Store
	coordinator *replication.Coordinator
	membership  *membership.Manager
	metrics     *metrics.Registry
	resolver    *conflict.Resolver
	server      *http.Server
	logger      *slog.Logger
	addrLookup  func(nodeID string) string
	mu          sync.RWMutex
}

func New(cfg config.Config, sharedRing *ring.Ring, addrLookup func(nodeID string) string, logger *slog.Logger) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	st, err := store.New(cfg.NodeID, cfg.NodeDataDir(), cfg.MaxKeyLength, cfg.MaxValueSize)
	if err != nil {
		return nil, err
	}

	r := sharedRing
	if r == nil {
		r = ring.New(cfg.ReplicationFactor, cfg.VirtualNodesPerNode)
	}
	r.AddNode(cfg.NodeID, cfg.Address)

	m := metrics.NewRegistry()
	resolver := conflict.NewResolver(cfg.ConflictStrategy)
	client := replication.NewHTTPReplicaClient()

	n := &Node{
		cfg:        cfg,
		ring:       r,
		store:      st,
		metrics:    m,
		resolver:   resolver,
		logger:     logger,
		addrLookup: addrLookup,
	}

	addrFn := func(nodeID string) string {
		if n.addrLookup != nil {
			return n.addrLookup(nodeID)
		}
		info, ok := n.ring.GetNode(nodeID)
		if !ok {
			return ""
		}
		return info.Address
	}

	n.coordinator = replication.NewCoordinator(cfg.NodeID, r, st, client, resolver, m, cfg.ReadQuorum, cfg.WriteQuorum, cfg.ReadRepair, addrFn)

	sender := func(fromID, toID, toAddress string) error {
		if n.membership != nil && n.membership.IsSimulatedDown(fromID) {
			return fmt.Errorf("node simulated down")
		}
		req, err := http.NewRequest(http.MethodPost, toAddress+"/internal/heartbeat", nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Node-ID", fromID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	}

	n.membership = membership.NewManager(
		cfg.NodeID,
		r,
		cfg.HeartbeatInterval(),
		cfg.FailureTimeout(),
		cfg.SuspectTimeout(),
		sender,
		logger,
	)

	handler := api.NewHandler(n.coordinator, n.store, n.ring, n.metrics, n.membership, cfg.NodeID, logger)
	n.server = &http.Server{Addr: cfg.ListenAddr(), Handler: handler.Routes()}

	return n, nil
}

func (n *Node) Start() error {
	n.membership.Start()
	n.logger.Info("node starting", "nodeId", n.cfg.NodeID, "addr", n.cfg.ListenAddr())
	return n.server.ListenAndServe()
}

func (n *Node) Shutdown(ctx context.Context) error {
	n.membership.Stop()
	return n.server.Shutdown(ctx)
}

func (n *Node) Ring() *ring.Ring       { return n.ring }
func (n *Node) Store() *store.Store     { return n.store }
func (n *Node) Coordinator() *replication.Coordinator { return n.coordinator }
func (n *Node) Membership() *membership.Manager { return n.membership }
func (n *Node) Config() config.Config  { return n.cfg }
func (n *Node) Metrics() *metrics.Registry { return n.metrics }

func (n *Node) SimulateFail()  { n.membership.SimulateFail(n.cfg.NodeID) }
func (n *Node) SimulateRecover() { n.membership.SimulateRecover(n.cfg.NodeID) }
