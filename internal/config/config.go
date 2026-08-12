package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	ClusterName         string `json:"clusterName"`
	NodeID              string `json:"nodeId"`
	Address             string `json:"address"`
	ReplicationFactor   int    `json:"replicationFactor"`
	ReadQuorum          int    `json:"readQuorum"`
	WriteQuorum         int    `json:"writeQuorum"`
	VirtualNodesPerNode int    `json:"virtualNodesPerNode"`
	HeartbeatIntervalMs int    `json:"heartbeatIntervalMs"`
	FailureTimeoutMs    int    `json:"failureTimeoutMs"`
	SuspectTimeoutMs    int    `json:"suspectTimeoutMs"`
	DataDir             string `json:"dataDir"`
	MaxKeyLength        int    `json:"maxKeyLength"`
	MaxValueSize        int    `json:"maxValueSize"`
	ConflictStrategy    string `json:"conflictStrategy"`
	ReadRepair          bool   `json:"readRepair"`
	SeedNodes           []string `json:"seedNodes"`
}

func Default() Config {
	return Config{
		ClusterName:         "local-kv",
		ReplicationFactor:   3,
		ReadQuorum:          2,
		WriteQuorum:         2,
		VirtualNodesPerNode: 64,
		HeartbeatIntervalMs: 1000,
		FailureTimeoutMs:    5000,
		SuspectTimeoutMs:    3000,
		DataDir:             "./data",
		MaxKeyLength:        256,
		MaxValueSize:        1024 * 1024,
		ConflictStrategy:    "lww",
		ReadRepair:          true,
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("nodeId is required")
	}
	if c.Address == "" {
		return fmt.Errorf("address is required")
	}
	if c.ReplicationFactor < 1 {
		return fmt.Errorf("replicationFactor must be >= 1")
	}
	if c.ReadQuorum < 1 || c.ReadQuorum > c.ReplicationFactor {
		return fmt.Errorf("readQuorum must be between 1 and replicationFactor")
	}
	if c.WriteQuorum < 1 || c.WriteQuorum > c.ReplicationFactor {
		return fmt.Errorf("writeQuorum must be between 1 and replicationFactor")
	}
	if c.VirtualNodesPerNode < 1 {
		return fmt.Errorf("virtualNodesPerNode must be >= 1")
	}
	return nil
}

func (c Config) NodeDataDir() string {
	return fmt.Sprintf("%s/%s", c.DataDir, c.NodeID)
}
