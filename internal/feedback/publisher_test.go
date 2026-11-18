package feedback

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/local/warehousetaskbridge/internal/metrics"
	"github.com/local/warehousetaskbridge/internal/taskpb"
	"github.com/local/warehousetaskbridge/internal/wms"
)

type fakeOutbox struct {
	mu        sync.Mutex
	events    []*taskpb.TaskStateEvent
	published map[uint64]bool
	markErr   map[uint64]error
	marks     []uint64
}

func newFakeOutbox(events ...*taskpb.TaskStateEvent) *fakeOutbox {
	return &fakeOutbox{events: events, published: make(map[uint64]bool), markErr: make(map[uint64]error)}
}

func (f *fakeOutbox) UnpublishedEvents(_ context.Context, limit int) ([]*taskpb.TaskStateEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*taskpb.TaskStateEvent
	for _, ev := range f.events {
		if f.published[ev.Seq] {
			continue
		}
		out = append(out, ev)
		if len(out) >= limit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (f *fakeOutbox) MarkPublished(_ context.Context, seq uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.markErr[seq]; err != nil {
		delete(f.markErr, seq)
		return err
	}
	f.published[seq] = true
	f.marks = append(f.marks, seq)
	return nil
}

type sentMessage struct {
	subject string
	seq     uint64
	update  wms.OrderStatusUpdate
}

type fakeSender struct {
	mu        sync.Mutex
	sent      []sentMessage
	failSeq   map[uint64]int
	available bool
}

func newFakeSender() *fakeSender {
	return &fakeSender{failSeq: make(map[uint64]int)}
}

func (f *fakeSender) Publish(_ context.Context, subject string, data []byte, seq uint64) error {
	f.mu.Lock()
	if f.failSeq[seq] > 0 {
		f.failSeq[seq]--
		f.mu.Unlock()
		return errors.New("simulated publish failure")
	}
	f.mu.Unlock()
	var update wms.OrderStatusUpdate
	if err := json.Unmarshal(data, &update); err != nil {
		return err
	}
	f.mu.Lock()
	f.sent = append(f.sent, sentMessage{subject: subject, seq: seq, update: update})
	f.mu.Unlock()
	return nil
}

func (f *fakeSender) messages() []sentMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sentMessage(nil), f.sent...)
}

type fakeGate struct {
	mu     sync.Mutex
	silent bool
	ch     chan struct{}
}

func (g *fakeGate) Silent() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.silent
}

func (g *fakeGate) WaitCh() <-chan struct{} { return g.ch }

func (g *fakeGate) setSilent(silent bool) {
	g.mu.Lock()
	g.silent = silent
	g.mu.Unlock()
}

func event(seq uint64, order string, from, to taskpb.TaskState) *taskpb.TaskStateEvent {
	return &taskpb.TaskStateEvent{
		Seq:       seq,
		TaskId:    "task-" + order,
		OrderRef:  order,
		FromState: from,
		ToState:   to,
		BotId:     "bot-01",
		Reason:    "test",
		EmittedAt: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
}

func testPublisher(t *testing.T, outbox Outbox, sender Sender, gate Gate, counters *metrics.Counters) *Publisher {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p, err := NewPublisher(Config{
		Outbox: outbox, Sender: sender, Gate: gate, Metrics: counters, Logger: logger,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	return p
}

func TestPublisherOrdersEventsAndSubjects(t *testing.T) {
	events := []*taskpb.TaskStateEvent{
		event(4, "ORD-B", taskpb.TaskState_RUNNING, taskpb.TaskState_DONE),
		event(1, "ORD-A", taskpb.TaskState_PENDING, taskpb.TaskState_ASSIGNED),
		event(3, "ORD-A", taskpb.TaskState_ASSIGNED, taskpb.TaskState_RUNNING),
		event(2, "ORD-B", taskpb.TaskState_PENDING, taskpb.TaskState_ASSIGNED),
		event(5, "ORD-A", taskpb.TaskState_RUNNING, taskpb.TaskState_DONE),
	}
	outbox := newFakeOutbox(events...)
	sender := newFakeSender()
	counters := metrics.New()
	p := testPublisher(t, outbox, sender, nil, counters)

	if err := p.DrainOnce(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	sent := sender.messages()
	if len(sent) != 5 {
		t.Fatalf("sent %d messages, want 5", len(sent))
	}
	for i, msg := range sent {
		if msg.seq != uint64(i+1) {
			t.Fatalf("message %d has seq %d, want %d", i, msg.seq, i+1)
		}
		if msg.update.StateSeq != msg.seq {
			t.Fatalf("StateSeq %d != outbox seq %d", msg.update.StateSeq, msg.seq)
		}
		if got, want := msg.subject, StatusSubject(msg.update.OrderRef); got != want {
			t.Fatalf("subject = %q, want %q", got, want)
		}
	}
	if got := counters.Get(metrics.EventsPublished); got != 5 {
		t.Fatalf("events_published = %d, want 5", got)
	}
	stats := p.Stats()
	if stats.Gaps != 0 || stats.Drained != 5 {
		t.Fatalf("stats = %+v, want 0 gaps and 5 drained", stats)
	}
	if len(outbox.marks) != 5 {
		t.Fatalf("marked %d events, want 5", len(outbox.marks))
	}
}

func TestPublisherNoLossOnPublishFailure(t *testing.T) {
	outbox := newFakeOutbox(
		event(1, "ORD-A", taskpb.TaskState_PENDING, taskpb.TaskState_ASSIGNED),
		event(2, "ORD-A", taskpb.TaskState_ASSIGNED, taskpb.TaskState_RUNNING),
		event(3, "ORD-A", taskpb.TaskState_RUNNING, taskpb.TaskState_DONE),
	)
	sender := newFakeSender()
	sender.failSeq[2] = 1
	counters := metrics.New()
	p := testPublisher(t, outbox, sender, nil, counters)

	if err := p.DrainOnce(context.Background()); err == nil {
		t.Fatal("expected first drain to fail")
	}
	if got := len(sender.messages()); got != 1 {
		t.Fatalf("delivered %d messages before failure, want 1", got)
	}
	if outbox.published[1] != true || outbox.published[2] || outbox.published[3] {
		t.Fatalf("published flags after failure = %+v", outbox.published)
	}

	if err := p.DrainOnce(context.Background()); err != nil {
		t.Fatalf("retry drain: %v", err)
	}
	sent := sender.messages()
	if len(sent) != 3 {
		t.Fatalf("delivered %d messages after retry, want 3", len(sent))
	}
	for i, msg := range sent {
		if msg.seq != uint64(i+1) {
			t.Fatalf("delivery order = %+v", sent)
		}
	}
	if !outbox.published[1] || !outbox.published[2] || !outbox.published[3] {
		t.Fatalf("not every event marked published: %+v", outbox.published)
	}
}

func TestPublisherAtLeastOnceDuplicateCarriesSameStateSeq(t *testing.T) {
	outbox := newFakeOutbox(
		event(1, "ORD-A", taskpb.TaskState_PENDING, taskpb.TaskState_ASSIGNED),
	)
	outbox.markErr[1] = errors.New("database write failed after publish")
	sender := newFakeSender()
	p := testPublisher(t, outbox, sender, nil, metrics.New())

	if err := p.DrainOnce(context.Background()); err == nil {
		t.Fatal("expected drain to fail when marking published fails")
	}
	if err := p.DrainOnce(context.Background()); err != nil {
		t.Fatalf("retry drain: %v", err)
	}
	sent := sender.messages()
	if len(sent) != 2 {
		t.Fatalf("delivered %d messages, want 2 (one replay)", len(sent))
	}
	if sent[0].seq != sent[1].seq || sent[0].update.StateSeq != sent[1].update.StateSeq {
		t.Fatalf("replay changed StateSeq: %+v", sent)
	}
	if stats := p.Stats(); stats.Gaps != 0 {
		t.Fatalf("duplicate replay counted as a gap: %+v", stats)
	}
}

func TestPublisherHonorsPeerSilence(t *testing.T) {
	outbox := newFakeOutbox(
		event(1, "ORD-A", taskpb.TaskState_PENDING, taskpb.TaskState_ASSIGNED),
		event(2, "ORD-A", taskpb.TaskState_ASSIGNED, taskpb.TaskState_RUNNING),
		event(3, "ORD-A", taskpb.TaskState_RUNNING, taskpb.TaskState_DONE),
	)
	sender := newFakeSender()
	gate := &fakeGate{silent: true, ch: make(chan struct{}, 1)}
	counters := metrics.New()
	p := testPublisher(t, outbox, sender, gate, counters)

	// While silent, events are buffered and nothing is delivered.
	if err := p.DrainOnce(context.Background()); err != nil {
		t.Fatalf("silent drain: %v", err)
	}
	if got := len(sender.messages()); got != 0 {
		t.Fatalf("delivered %d messages while silent", got)
	}
	if stats := p.Stats(); stats.Buffered != 3 || stats.Drained != 0 {
		t.Fatalf("stats while silent = %+v", stats)
	}
	if got := counters.Get(metrics.EventsBuffered); got != 3 {
		t.Fatalf("events_buffered = %d, want 3", got)
	}

	// Recovery drains the buffer in order and marks the events published.
	gate.setSilent(false)
	if err := p.DrainOnce(context.Background()); err != nil {
		t.Fatalf("recovery drain: %v", err)
	}
	sent := sender.messages()
	if len(sent) != 3 {
		t.Fatalf("delivered %d messages after recovery, want 3", len(sent))
	}
	for i, msg := range sent {
		if msg.seq != uint64(i+1) {
			t.Fatalf("recovered delivery order = %+v", sent)
		}
	}
	if stats := p.Stats(); stats.Buffered != 3 || stats.Drained != 3 || stats.Gaps != 0 {
		t.Fatalf("stats after recovery = %+v", stats)
	}
	if got := counters.Get(metrics.EventsDrained); got != 3 {
		t.Fatalf("events_drained = %d, want 3", got)
	}
	if err := p.DrainOnce(context.Background()); err != nil {
		t.Fatalf("no-op drain: %v", err)
	}
	if got := len(sender.messages()); got != 3 {
		t.Fatalf("duplicate delivery after drain: %d messages", got)
	}
}

func TestStatusSubjectSanitizesOrderRef(t *testing.T) {
	got := StatusSubject("ORD 1.a*b>c")
	want := "wms.status.ORD_1_a_b_c"
	if got != want {
		t.Fatalf("StatusSubject = %q, want %q", got, want)
	}
	if StatusSubject("") != "wms.status.unknown" {
		t.Fatalf("empty order ref = %q", StatusSubject(""))
	}
}
