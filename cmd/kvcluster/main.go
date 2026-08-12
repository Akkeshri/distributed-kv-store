package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Akkeshri/distributed-kv-store/internal/cluster"
	"github.com/Akkeshri/distributed-kv-store/internal/config"
	"github.com/spf13/cobra"
)

func main() {
	var (
		nodeCount   int
		replication int
		readQ       int
		writeQ      int
		dataDir     string
	)

	root := &cobra.Command{
		Use:   "kvcluster",
		Short: "Start and manage a local KV cluster",
	}

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start an in-process multi-node cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := config.Default()
			cfg.ReplicationFactor = replication
			cfg.ReadQuorum = readQ
			cfg.WriteQuorum = writeQ
			cfg.DataDir = dataDir
			cfg.VirtualNodesPerNode = 8

			c := cluster.New(cfg, logger)
			if err := c.StartNodes(nodeCount); err != nil {
				return err
			}

			addr := c.AnyAddress()
			fmt.Printf("Cluster started with %d nodes\n", nodeCount)
			fmt.Printf("Coordinator URL: %s\n", addr)
			fmt.Printf("Dashboard: %s/dashboard\n", addr)
			fmt.Printf("Metrics: %s/metrics\n", addr)
			fmt.Println("Press Ctrl+C to stop")

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh
			c.Shutdown()
			return nil
		},
	}
	startCmd.Flags().IntVar(&nodeCount, "nodes", 3, "number of nodes")
	startCmd.Flags().IntVar(&replication, "replication", 3, "replication factor")
	startCmd.Flags().IntVar(&readQ, "read-quorum", 2, "read quorum")
	startCmd.Flags().IntVar(&writeQ, "write-quorum", 2, "write quorum")
	startCmd.Flags().StringVar(&dataDir, "data-dir", "./data", "data directory")

	root.AddCommand(startCmd)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
