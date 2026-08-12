package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Akkeshri/distributed-kv-store/internal/config"
	"github.com/Akkeshri/distributed-kv-store/internal/node"
)

func main() {
	configPath := flag.String("config", "", "path to config JSON")
	nodeID := flag.String("node-id", "node-1", "node identifier")
	address := flag.String("address", "http://127.0.0.1:8080", "node HTTP address")
	dataDir := flag.String("data-dir", "./data", "data directory root")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}
	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if *address != "" {
		cfg.Address = *address
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}

	n, err := node.New(cfg, nil, nil, logger)
	if err != nil {
		logger.Error("create node", "err", err)
		os.Exit(1)
	}

	go func() {
		if err := n.Start(); err != nil {
			logger.Error("node stopped", "err", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = n.Shutdown(ctx)
}
