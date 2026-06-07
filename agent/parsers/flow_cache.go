package parsers

import (
	"fmt"
	"sync"
	"time"
)

const flowCacheTTL = 2 * time.Minute

type flowRouteEntry struct {
	route     string
	expiresAt time.Time
}

// FlowCache stores the last parsed route per TCP flow (for split AMQP frames).
type FlowCache struct {
	mu      sync.Mutex
	entries map[string]flowRouteEntry
}

func NewFlowCache() *FlowCache {
	return &FlowCache{entries: make(map[string]flowRouteEntry)}
}

func flowKeys(in ParseInput) []string {
	return []string{
		fmt.Sprintf("%s:%d-%s:%d", in.SrcIP, in.SrcPort, in.DstIP, in.DstPort),
		fmt.Sprintf("%s:%d-%s:%d", in.DstIP, in.DstPort, in.SrcIP, in.SrcPort),
	}
}

func (c *FlowCache) Set(in ParseInput, route string) {
	if route == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := flowRouteEntry{route: route, expiresAt: time.Now().Add(flowCacheTTL)}
	for _, key := range flowKeys(in) {
		c.entries[key] = entry
	}
}

func (c *FlowCache) Get(in ParseInput) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for _, key := range flowKeys(in) {
		entry, ok := c.entries[key]
		if !ok || now.After(entry.expiresAt) {
			delete(c.entries, key)
			continue
		}
		return entry.route, true
	}
	return "", false
}
