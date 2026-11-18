// Package feedback drains the durable task_events outbox to the external WMS
// with ordered, at-least-once delivery and peer-silence-aware buffering.
package feedback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/local/warehousetaskbridge/internal/metrics"
	"github.com/local/warehousetaskbridge/internal/taskpb"
	"github.com/local/warehousetaskbridge/internal/translate"
)

// Status stream topology.
const (
	// StatusStream is the JetStream stream retaining outbound status updates.
	StatusStream = "WMS_OUT"
	// StatusSubjectPrefix prefixes every per-order outbound subject.
	StatusSubjectPrefix = "wms.status."
)

// EnsureStatusStream creates the WMS_OUT stream when absent.
func EnsureStatusStream(js nats.JetStreamContext) error {
	if js == nil {
		return errors.New("feedback: nil JetStream context")
	}
	_, err := js.StreamInfo(StatusStream)
	if err == nil {
		return nil
	}
	if !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("feedback: inspect status stream: %w", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:       StatusStream,
		Subjects:   []string{StatusSubjectPrefix + ">"},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		MaxAge:     24 * time.Hour,
		Duplicates: 2 * time.Minute,
	}); err != nil {
		return fmt.Errorf("feedback: create status stream: %w", err)
	}
	return nil
}

// StatusSubject converts an order reference into a safe NATS subject token.
func StatusSubject(orderRef string) string {
	var b strings.Builder
	for _, r := range orderRef {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	token := b.String()
	if token == "" {
		token = "unknown"
	}
	return StatusSubjectPrefix + token
}

// Sender performs an acknowledged publish of one status update.
type Sender interface {
	Publish(ctx context.Context, subject string, data []byte, seq uint64) error
}

// NATSSender publishes status updates to JetStream and waits for a pub ack.
type NATSSender struct {
	js nats.JetStreamContext
}

// NewNATSSender wraps a JetStream context.
func NewNATSSender(js nats.JetStreamContext) (*NATSSender, error) {
	if js == nil {
		return nil, errors.New("feedback: NATS sender requires a JetStream context")
	}
	return &NATSSender{js: js}, nil
}

// Publish sends one JSON status update and returns only after a JetStream ack.
func (s *NATSSender) Publish(ctx context.Context, subject string, data []byte, _ uint64) error {
	if _, err := s.js.Publish(subject, data, nats.Context(ctx)); err != nil {
		return fmt.Errorf("feedback: publish %s: %w", subject, err)
	}
	return nil
}

// Outbox is the durable event source and delivery marker.
type Outbox interface {
	UnpublishedEvents(ctx context.Context, limit int) ([]*taskpb.TaskStateEvent, error)
	MarkPublished(ctx context.Context, seq uint64) error
}

// Gate reports peer liveness and signals state changes.
type Gate interface {
	Silent() bool
	WaitCh() <-chan struct{}
}

// Config configures the outbox publisher.
type Config struct {
	Outbox       Outbox
	Sender       Sender
	Gate         Gate
	Metrics      *metrics.Counters
	Logger       *slog.Logger
	PollInterval time.Duration
	BatchSize    int
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
}

// Publisher drains task_events to the WMS.
type Publisher struct {
	outbox       Outbox
	sender       Sender
	gate         Gate
	counters     *metrics.Counters
	logger       *slog.Logger
	pollInterval time.Duration
	batchSize    int
	baseBackoff  time.Duration
	maxBackoff   time.Duration

	buffer *buffer

	mu       sync.Mutex
	lastSeq  map[string]uint64
	gaps     int64
	buffered int64
	drained  int64
}

// NewPublisher validates configuration and builds a Publisher.
func NewPublisher(cfg Config) (*Publisher, error) {
	if cfg.Outbox == nil {
		return nil, errors.New("feedback: publisher requires an outbox")
	}
	if cfg.Sender == nil {
		return nil, errors.New("feedback: publisher requires a sender")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 100 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Publisher{
		outbox:       cfg.Outbox,
		sender:       cfg.Sender,
		gate:         cfg.Gate,
		counters:     cfg.Metrics,
		logger:       cfg.Logger,
		pollInterval: cfg.PollInterval,
		batchSize:    cfg.BatchSize,
		baseBackoff:  cfg.BaseBackoff,
		maxBackoff:   cfg.MaxBackoff,
		buffer:       newBuffer(),
		lastSeq:      make(map[string]uint64),
	}, nil
}

// Run drains the outbox until ctx is cancelled. It retries failures with
// exponential backoff and never marks an event published without an ack.
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()
	backoff := p.baseBackoff

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.DrainOnce(ctx); err != nil {
				p.counters.Inc(metrics.PublishFailures)
				p.logger.Warn("outbox drain failed; retrying with backoff",
					"error", err, "backoff", backoff.String())
				if err := wait(ctx, backoff); err != nil {
					return nil
				}
				backoff *= 2
				if backoff > p.maxBackoff {
					backoff = p.maxBackoff
				}
				continue
			}
			backoff = p.baseBackoff
		}
	}
}

// DrainOnce performs one publishing pass. While the peer is SILENT it buffers
// unpublished events in memory (they remain unpublished in the database). When
// live it first drains the buffer in seq order, then publishes fresh events.
func (p *Publisher) DrainOnce(ctx context.Context) error {
	if p.gate != nil && p.gate.Silent() {
		events, err := p.outbox.UnpublishedEvents(ctx, p.batchSize)
		if err != nil {
			return err
		}
		for _, ev := range events {
			if p.buffer.add(ev.Seq, ev) {
				p.mu.Lock()
				p.buffered++
				p.mu.Unlock()
				p.counters.Inc(metrics.EventsBuffered)
			}
		}
		return nil
	}

	drained, err := p.buffer.drain(ctx, func(seq uint64, ev *taskpb.TaskStateEvent) error {
		return p.publishEvent(ctx, ev)
	})
	if drained > 0 {
		p.counters.Add(metrics.EventsDrained, int64(drained))
	}
	if err != nil {
		return err
	}
	for {
		events, err := p.outbox.UnpublishedEvents(ctx, p.batchSize)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		for _, ev := range events {
			if err := p.publishEvent(ctx, ev); err != nil {
				return err
			}
		}
		if len(events) < p.batchSize {
			return nil
		}
	}
}

func (p *Publisher) publishEvent(ctx context.Context, ev *taskpb.TaskStateEvent) error {
	update, warnings := translate.ToExternal(ev)
	for _, warning := range warnings {
		if warning.Code == translate.CodeLossyStateCollapse {
			p.counters.Inc(metrics.LossyStateCollapses)
			p.logger.Debug("lossy external state collapse",
				"order_ref", ev.OrderRef, "seq", ev.Seq, "detail", warning.Detail)
		}
	}
	data, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("feedback: marshal update for seq %d: %w", ev.Seq, err)
	}
	if err := p.sender.Publish(ctx, StatusSubject(update.OrderRef), data, ev.Seq); err != nil {
		return err
	}
	if err := p.outbox.MarkPublished(ctx, ev.Seq); err != nil {
		return err
	}
	p.counters.Inc(metrics.EventsPublished)

	p.mu.Lock()
	if prev, ok := p.lastSeq[ev.OrderRef]; ok && ev.Seq < prev {
		p.gaps++
		p.counters.Inc(metrics.OrderGapsDetected)
	}
	if prev := p.lastSeq[ev.OrderRef]; ev.Seq > prev {
		p.lastSeq[ev.OrderRef] = ev.Seq
	}
	p.drained++
	p.mu.Unlock()
	return nil
}

// PublisherStats reports buffer and ordering activity.
type PublisherStats struct {
	Buffered int64 `json:"buffered"`
	Drained  int64 `json:"drained"`
	Gaps     int64 `json:"gaps"`
}

// Stats returns cumulative publisher activity.
func (p *Publisher) Stats() PublisherStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PublisherStats{Buffered: p.buffered, Drained: p.drained, Gaps: p.gaps}
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// buffer is the publisher's ordered holding area while the peer is silent.
type buffer struct {
	mu       sync.Mutex
	items    []bufferedEvent
	buffered int64
}

type bufferedEvent struct {
	seq   uint64
	event *taskpb.TaskStateEvent
}

func newBuffer() *buffer { return &buffer{} }

func (b *buffer) add(seq uint64, ev *taskpb.TaskStateEvent) bool {
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
	b.items = append(b.items, bufferedEvent{})
	copy(b.items[pos+1:], b.items[pos:])
	b.items[pos] = bufferedEvent{seq: seq, event: ev}
	b.buffered++
	return true
}

func (b *buffer) drain(ctx context.Context, publish func(uint64, *taskpb.TaskStateEvent) error) (int, error) {
	b.mu.Lock()
	items := append([]bufferedEvent(nil), b.items...)
	b.mu.Unlock()

	published := 0
	var lastSeq uint64
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		if err := publish(item.seq, item.event); err != nil {
			b.commit(published, lastSeq)
			return published, err
		}
		published++
		lastSeq = item.seq
	}
	b.commit(published, lastSeq)
	return published, nil
}

func (b *buffer) commit(published int, lastSeq uint64) {
	if published == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	cut := 0
	for cut < len(b.items) && b.items[cut].seq <= lastSeq {
		cut++
	}
	b.items = append([]bufferedEvent(nil), b.items[cut:]...)
}
