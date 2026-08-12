package config

import (
	"fmt"
	"strings"
	"time"
)

func (c Config) ListenAddr() string {
	addr := c.Address
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	if !strings.Contains(addr, ":") {
		return fmt.Sprintf(":%s", addr)
	}
	return addr
}

func (c Config) HeartbeatInterval() time.Duration {
	return time.Duration(c.HeartbeatIntervalMs) * time.Millisecond
}

func (c Config) FailureTimeout() time.Duration {
	return time.Duration(c.FailureTimeoutMs) * time.Millisecond
}

func (c Config) SuspectTimeout() time.Duration {
	if c.SuspectTimeoutMs > 0 {
		return time.Duration(c.SuspectTimeoutMs) * time.Millisecond
	}
	return time.Duration(c.FailureTimeoutMs/2) * time.Millisecond
}
