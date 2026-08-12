package membership

import (
	"log/slog"
	"sync"
	"time"

	"github.com/Akkeshri/distributed-kv-store/internal/ring"
)

type HeartbeatSender func(fromID, toID, toAddress string) error

type Manager struct {
	mu                sync.RWMutex
	localID           string
	ring              *ring.Ring
	interval          time.Duration
	failureTimeout    time.Duration
	suspectTimeout    time.Duration
	lastHeartbeat     map[string]time.Time
	stopCh            chan struct{}
	onStatusChange    func(nodeID string, status ring.NodeStatus)
	sendHeartbeat     HeartbeatSender
	logger            *slog.Logger
	simulatedDown     map[string]bool
}

func NewManager(localID string, r *ring.Ring, interval, failure, suspect time.Duration, sender HeartbeatSender, logger *slog.Logger) *Manager {
	return &Manager{
		localID:        localID,
		ring:           r,
		interval:       interval,
		failureTimeout: failure,
		suspectTimeout: suspect,
		lastHeartbeat:  make(map[string]time.Time),
		stopCh:         make(chan struct{}),
		sendHeartbeat:  sender,
		logger:         logger,
		simulatedDown:  make(map[string]bool),
	}
}

func (m *Manager) OnStatusChange(fn func(nodeID string, status ring.NodeStatus)) {
	m.onStatusChange = fn
}

func (m *Manager) Start() {
	go m.loop()
}

func (m *Manager) Stop() {
	close(m.stopCh)
}

func (m *Manager) RecordHeartbeat(fromID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastHeartbeat[fromID] = time.Now()
	n, ok := m.ring.GetNode(fromID)
	if !ok {
		return
	}
	if n.Status == ring.StatusSUSPECT || n.Status == ring.StatusDOWN {
		m.setStatus(fromID, ring.StatusRECOVERING)
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.setStatus(fromID, ring.StatusUP)
		}()
	} else if n.Status != ring.StatusUP {
		m.setStatus(fromID, ring.StatusUP)
	}
}

func (m *Manager) SimulateFail(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.simulatedDown[nodeID] = true
	if nodeID == m.localID {
		m.setStatus(nodeID, ring.StatusDOWN)
	}
}

func (m *Manager) SimulateRecover(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.simulatedDown, nodeID)
	if nodeID == m.localID {
		m.setStatus(nodeID, ring.StatusRECOVERING)
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.setStatus(nodeID, ring.StatusUP)
		}()
	}
}

func (m *Manager) IsSimulatedDown(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.simulatedDown[nodeID]
}

func (m *Manager) loop() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.tick()
		}
	}
}

func (m *Manager) tick() {
	nodes := m.ring.Nodes()
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, n := range nodes {
		if n.ID == m.localID {
			m.lastHeartbeat[n.ID] = now
			continue
		}
		if m.simulatedDown[n.ID] {
			continue
		}
		if m.sendHeartbeat != nil && !m.simulatedDown[m.localID] {
			if err := m.sendHeartbeat(m.localID, n.ID, n.Address); err != nil {
				m.logger.Debug("heartbeat send failed", "to", n.ID, "err", err)
			}
		}
	}

	for _, n := range nodes {
		if n.ID == m.localID {
			continue
		}
		last, ok := m.lastHeartbeat[n.ID]
		if !ok {
			m.lastHeartbeat[n.ID] = now
			continue
		}
		elapsed := now.Sub(last)
		current := n.Status
		switch {
		case elapsed >= m.failureTimeout && current != ring.StatusDOWN:
			m.setStatus(n.ID, ring.StatusDOWN)
		case elapsed >= m.suspectTimeout && current == ring.StatusUP:
			m.setStatus(n.ID, ring.StatusSUSPECT)
		}
	}
}

func (m *Manager) setStatus(nodeID string, status ring.NodeStatus) {
	m.ring.SetNodeStatus(nodeID, status)
	if m.onStatusChange != nil {
		m.onStatusChange(nodeID, status)
	}
	m.logger.Info("node status changed", "nodeId", nodeID, "status", status)
}

func (m *Manager) NodeStatuses() map[string]string {
	out := make(map[string]string)
	for _, n := range m.ring.Nodes() {
		out[n.ID] = string(n.Status)
	}
	return out
}
