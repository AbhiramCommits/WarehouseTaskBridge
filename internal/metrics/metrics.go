// Package metrics provides the small set of process-local counters exposed on
// the bridge's plain-text /metrics endpoint.
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Canonical counter names.
const (
	OrdersReceived    = "orders_received"
	TasksCreated      = "tasks_created"
	OrdersDeduped     = "orders_deduped"
	Quarantined       = "quarantined"
	TasksAssigned     = "tasks_assigned"
	EventsPublished   = "events_published"
	PeerSilenceEvents = "peer_silence_events"
	EventsBuffered    = "events_buffered"
	EventsDrained     = "events_drained"
	OrderGapsDetected = "order_gaps_detected"
	TasksCompleted    = "tasks_completed"
	TasksFailed       = "tasks_failed"
	TasksRequeued     = "tasks_requeued"
	PublishFailures   = "publish_failures"
)

// Counters is a concurrency-safe set of named monotonic counters.
type Counters struct {
	mu     sync.Mutex
	values map[string]int64
}

// New returns an empty counter set.
func New() *Counters {
	return &Counters{values: make(map[string]int64)}
}

// Inc adds one to a counter. A nil receiver is a no-op so instrumentation is
// always optional.
func (c *Counters) Inc(name string) {
	c.Add(name, 1)
}

// Add adds n to a counter. A nil receiver is a no-op.
func (c *Counters) Add(name string, n int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.values == nil {
		c.values = make(map[string]int64)
	}
	c.values[name] += n
}

// Get returns the current value of a counter.
func (c *Counters) Get(name string) int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[name]
}

// Snapshot returns a copy of all counters.
func (c *Counters) Snapshot() map[string]int64 {
	if c == nil {
		return map[string]int64{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.values))
	for k, v := range c.values {
		out[k] = v
	}
	return out
}

// Render produces stable Prometheus-style plain text.
func (c *Counters) Render() string {
	snap := c.Snapshot()
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s %d\n", k, snap[k])
	}
	return b.String()
}
