// Package peer tracks the liveness of the external WMS using heartbeats and
// provides an ordered buffer so outbound state updates pause rather than drop
// while the peer is SILENT.
package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/local/warehousetaskbridge/internal/metrics"
)

// Subjects used for liveness exchange.
const (
	BridgeHeartbeatSubject = "bridge.heartbeat"
	WMSHeartbeatSubject    = "wms.heartbeat"
	// DefaultPeerID is the heartbeat identity used when the WMS omits one.
	DefaultPeerID = "wms"
)

// Clock is an injectable time source so silence can be tested deterministically.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// SystemClock returns the real wall clock.
func SystemClock() Clock { return systemClock{} }

// WatcherConfig configures a liveness Watcher.
type WatcherConfig struct {
	// PeerID identifies the remote WMS.
	PeerID string
	// Timeout is how long the peer may be quiet before it is SILENT.
	Timeout time.Duration
	// Clock is optional; it defaults to the system clock.
	Clock Clock
	// Counters is optional.
	Counters *metrics.Counters
	// Logger is optional.
	Logger *slog.Logger
}

// Watcher tracks inbound WMS heartbeats and flips between live and SILENT.
// All methods are safe for concurrent use.
type Watcher struct {
	peerID   string
	timeout  time.Duration
	clock    Clock
	counters *metrics.Counters
	logger   *slog.Logger

	mu       sync.Mutex
	lastSeen time.Time
	silent   bool
	wake     chan struct{}
}

// NewWatcher builds a Watcher. It starts in the live state with the current
// time as the last-seen value, so a silent WMS is detected one timeout after
// startup even if it never sends a heartbeat.
func NewWatcher(cfg WatcherConfig) *Watcher {
	if cfg.Clock == nil {
		cfg.Clock = SystemClock()
	}
	if cfg.PeerID == "" {
		cfg.PeerID = DefaultPeerID
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Watcher{
		peerID:   cfg.PeerID,
		timeout:  cfg.Timeout,
		clock:    cfg.Clock,
		counters: cfg.Counters,
		logger:   cfg.Logger,
		lastSeen: cfg.Clock.Now(),
		wake:     make(chan struct{}, 1),
	}
}

// Observe records an inbound heartbeat observed at time at. It immediately
// clears SILENT, waking any publisher waiting to drain buffered events.
func (w *Watcher) Observe(at time.Time) {
	if at.IsZero() {
		at = w.clock.Now()
	}
	w.mu.Lock()
	w.lastSeen = at
	recovered := w.silent
	w.silent = false
	w.mu.Unlock()
	if recovered {
		w.logger.Info("peer recovered", "peer_id", w.peerID)
		w.signal()
	}
}

// Evaluate recomputes liveness against now and returns whether the peer is
// SILENT. It emits a structured warning and increments the silence counter only
// on a live-to-silent transition.
func (w *Watcher) Evaluate(now time.Time) bool {
	w.mu.Lock()
	if w.lastSeen.IsZero() {
		w.lastSeen = now
	}
	shouldBeSilent := now.Sub(w.lastSeen) >= w.timeout
	changed := shouldBeSilent != w.silent
	w.silent = shouldBeSilent
	w.mu.Unlock()

	if changed {
		if shouldBeSilent {
			w.counters.Inc(metrics.PeerSilenceEvents)
			w.logger.Warn("peer is silent; pausing outbound state publishing",
				"peer_id", w.peerID,
				"timeout", w.timeout.String(),
				"last_seen", w.lastSeen.UTC().Format(time.RFC3339Nano))
		} else {
			w.logger.Info("peer is live; draining buffered state publishing", "peer_id", w.peerID)
		}
		w.signal()
	}
	return shouldBeSilent
}

// Silent reports the last evaluated liveness state.
func (w *Watcher) Silent() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.silent
}

// LastSeen returns the last observed heartbeat time.
func (w *Watcher) LastSeen() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSeen
}

// WaitCh returns a channel that receives a signal whenever liveness changes.
// The channel is buffered, so missed wakeups are coalesced.
func (w *Watcher) WaitCh() <-chan struct{} {
	return w.wake
}

func (w *Watcher) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run evaluates liveness on every tick until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context, tick time.Duration) {
	if tick <= 0 {
		tick = w.timeout / 4
	}
	if tick <= 0 {
		tick = time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Evaluate(w.clock.Now())
		}
	}
}

// HeartbeatStore persists inbound heartbeat timestamps.
type HeartbeatStore interface {
	RecordHeartbeat(ctx context.Context, peerID string, at time.Time) error
}

// HeartbeatConfig configures the heartbeat exchange.
type HeartbeatConfig struct {
	Conn     *nats.Conn
	Store    HeartbeatStore
	Watcher  *Watcher
	Interval time.Duration
	PeerID   string
	Clock    Clock
	Counters *metrics.Counters
	Logger   *slog.Logger
}

// Heartbeat publishes bridge heartbeats and records inbound WMS heartbeats.
type Heartbeat struct {
	conn     *nats.Conn
	store    HeartbeatStore
	watcher  *Watcher
	interval time.Duration
	peerID   string
	clock    Clock
	counters *metrics.Counters
	logger   *slog.Logger
}

// NewHeartbeat validates and builds a Heartbeat exchange.
func NewHeartbeat(cfg HeartbeatConfig) (*Heartbeat, error) {
	if cfg.Conn == nil {
		return nil, errors.New("peer: heartbeat requires a NATS connection")
	}
	if cfg.Store == nil {
		return nil, errors.New("peer: heartbeat requires a store")
	}
	if cfg.Watcher == nil {
		return nil, errors.New("peer: heartbeat requires a watcher")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("peer: heartbeat interval must be positive, got %s", cfg.Interval)
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock()
	}
	if cfg.PeerID == "" {
		cfg.PeerID = DefaultPeerID
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Heartbeat{
		conn:     cfg.Conn,
		store:    cfg.Store,
		watcher:  cfg.Watcher,
		interval: cfg.Interval,
		peerID:   cfg.PeerID,
		clock:    cfg.Clock,
		counters: cfg.Counters,
		logger:   cfg.Logger,
	}, nil
}

// heartbeatPayload is the JSON body exchanged on heartbeat subjects.
type heartbeatPayload struct {
	PeerID string    `json:"peer_id"`
	SentAt time.Time `json:"sent_at"`
}

// Run subscribes to WMS heartbeats and publishes the bridge heartbeat until ctx
// is cancelled. The subscription is drained and removed before returning.
func (h *Heartbeat) Run(ctx context.Context) error {
	sub, err := h.conn.Subscribe(WMSHeartbeatSubject, func(msg *nats.Msg) {
		now := h.clock.Now()
		peerID := h.peerID
		var payload heartbeatPayload
		if err := json.Unmarshal(msg.Data, &payload); err == nil && payload.PeerID != "" {
			peerID = payload.PeerID
		}
		h.watcher.Observe(now)
		recordCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := h.store.RecordHeartbeat(recordCtx, peerID, now); err != nil {
			h.logger.Error("record peer heartbeat", "peer_id", peerID, "error", err)
		}
	})
	if err != nil {
		return fmt.Errorf("peer: subscribe heartbeat: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	h.publishHeartbeat()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			h.publishHeartbeat()
			h.watcher.Evaluate(h.clock.Now())
		}
	}
}

func (h *Heartbeat) publishHeartbeat() {
	now := h.clock.Now()
	body, err := json.Marshal(heartbeatPayload{PeerID: "bridge", SentAt: now.UTC()})
	if err != nil {
		h.logger.Error("encode bridge heartbeat", "error", err)
		return
	}
	if err := h.conn.Publish(BridgeHeartbeatSubject, body); err != nil {
		h.logger.Error("publish bridge heartbeat", "error", err)
	}
}

// BufferStats reports ordered-buffer activity.
type BufferStats struct {
	Buffered int64 `json:"buffered"`
	Drained  int64 `json:"drained"`
	Depth    int   `json:"depth"`
}

// Buffer is an ordered, duplicate-suppressing holding area used while the peer
// is silent. It is safe for concurrent use.
type Buffer[T any] struct {
	mu       sync.Mutex
	items    []buffered[T]
	buffered int64
	drained  int64
}

type buffered[T any] struct {
	seq   uint64
	value T
}

// NewBuffer returns an empty buffer.
func NewBuffer[T any]() *Buffer[T] {
	return &Buffer[T]{}
}

// Add inserts a value in seq order. Adding a seq that is already buffered is a
// no-op and returns false. Adding an older seq after newer ones is allowed so a
// recovery drain can never reorder events.
func (b *Buffer[T]) Add(seq uint64, value T) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, item := range b.items {
		if item.seq == seq {
			return false
		}
	}
	pos := len(b.items)
	for i, item := range b.items {
		if seq < item.seq {
			pos = i
			break
		}
	}
	b.items = append(b.items, buffered[T]{})
	copy(b.items[pos+1:], b.items[pos:])
	b.items[pos] = buffered[T]{seq: seq, value: value}
	b.buffered++
	return true
}

// Len returns the number of buffered values.
func (b *Buffer[T]) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

// Stats returns cumulative activity plus current depth.
func (b *Buffer[T]) Stats() BufferStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BufferStats{Buffered: b.buffered, Drained: b.drained, Depth: len(b.items)}
}

// Drain publishes buffered values in ascending seq order. It stops at the first
// error, keeps every unpublished value, and returns how many were published.
func (b *Buffer[T]) Drain(ctx context.Context, publish func(seq uint64, value T) error) (int, error) {
	b.mu.Lock()
	items := append([]buffered[T](nil), b.items...)
	b.mu.Unlock()

	published := 0
	var lastSeq uint64
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			break
		}
		if err := publish(item.seq, item.value); err != nil {
			b.commitDrain(published, lastSeq)
			return published, err
		}
		published++
		lastSeq = item.seq
	}
	b.commitDrain(published, lastSeq)
	return published, nil
}

func (b *Buffer[T]) commitDrain(published int, lastSeq uint64) {
	if published == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	cut := 0
	for cut < len(b.items) && b.items[cut].seq <= lastSeq {
		cut++
	}
	b.items = append([]buffered[T](nil), b.items[cut:]...)
	b.drained += int64(published)
}
