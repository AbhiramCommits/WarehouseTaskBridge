package peer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/local/warehousetaskbridge/internal/metrics"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWatcherSilenceDetection(t *testing.T) {
	clock := newFakeClock()
	counters := metrics.New()
	watcher := NewWatcher(WatcherConfig{
		PeerID:   "wms",
		Timeout:  10 * time.Second,
		Clock:    clock,
		Counters: counters,
		Logger:   testLogger(),
	})
	if watcher.Silent() {
		t.Fatal("watcher started silent")
	}

	// Still within the timeout: no silence.
	now := clock.Advance(9 * time.Second)
	if watcher.Evaluate(now) {
		t.Fatal("peer marked silent before the timeout elapsed")
	}
	if got := counters.Get(metrics.PeerSilenceEvents); got != 0 {
		t.Fatalf("peer_silence_events = %d before timeout", got)
	}

	// Cross the timeout boundary.
	now = clock.Advance(2 * time.Second)
	if !watcher.Evaluate(now) {
		t.Fatal("peer not marked silent after the timeout elapsed")
	}
	if !watcher.Silent() {
		t.Fatal("Silent() disagrees with Evaluate")
	}
	if got := counters.Get(metrics.PeerSilenceEvents); got != 1 {
		t.Fatalf("peer_silence_events = %d, want 1", got)
	}

	// Re-evaluating while already silent must not double-count.
	watcher.Evaluate(clock.Advance(time.Second))
	if got := counters.Get(metrics.PeerSilenceEvents); got != 1 {
		t.Fatalf("peer_silence_events = %d after repeated evaluation, want 1", got)
	}

	// A heartbeat immediately restores the live state.
	watcher.Observe(clock.Now())
	if watcher.Silent() {
		t.Fatal("peer still silent after a heartbeat")
	}
	if !watcher.LastSeen().Equal(clock.Now()) {
		t.Fatalf("last seen = %s, want %s", watcher.LastSeen(), clock.Now())
	}
}

func TestBufferDrainsInOrderAfterRecovery(t *testing.T) {
	clock := newFakeClock()
	watcher := NewWatcher(WatcherConfig{
		PeerID: "wms", Timeout: 5 * time.Second, Clock: clock, Logger: testLogger(),
	})
	buffer := NewBuffer[string]()

	// Peer goes silent: events are buffered, not published.
	watcher.Evaluate(clock.Advance(6 * time.Second))
	if !watcher.Silent() {
		t.Fatal("expected silent peer")
	}
	var published []string
	publish := func(seq uint64, value string) error {
		published = append(published, strconv.FormatUint(seq, 10)+":"+value)
		return nil
	}
	for seq, value := range []string{"assigned", "running", "done"} {
		if !buffer.Add(uint64(seq+1), value) {
			t.Fatalf("buffer rejected new seq %d", seq+1)
		}
	}
	if buffer.Add(2, "assigned") {
		t.Fatal("buffer accepted a duplicate seq")
	}
	if len(published) != 0 {
		t.Fatalf("published %v while peer silent", published)
	}
	if stats := buffer.Stats(); stats.Buffered != 3 || stats.Depth != 3 {
		t.Fatalf("buffer stats while silent = %+v", stats)
	}

	// Recovery: drain in seq order.
	watcher.Observe(clock.Now())
	if watcher.Silent() {
		t.Fatal("peer should be live after heartbeat")
	}
	n, err := buffer.Drain(context.Background(), publish)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 3 {
		t.Fatalf("drained %d events, want 3", n)
	}
	want := []string{"1:assigned", "2:running", "3:done"}
	if len(published) != len(want) {
		t.Fatalf("published %v, want %v", published, want)
	}
	for i := range want {
		if published[i] != want[i] {
			t.Fatalf("published %v, want %v", published, want)
		}
	}
	stats := buffer.Stats()
	if stats.Drained != 3 || stats.Depth != 0 {
		t.Fatalf("buffer stats after drain = %+v", stats)
	}
}

func TestBufferKeepsUnpublishedOnFailure(t *testing.T) {
	buffer := NewBuffer[int]()
	buffer.Add(1, 10)
	buffer.Add(2, 20)
	buffer.Add(3, 30)

	fail := errors.New("publish failed")
	calls := 0
	n, err := buffer.Drain(context.Background(), func(seq uint64, value int) error {
		calls++
		if seq == 2 {
			return fail
		}
		return nil
	})
	if !errors.Is(err, fail) {
		t.Fatalf("drain error = %v, want publish failure", err)
	}
	if n != 1 || calls != 2 {
		t.Fatalf("drain published %d after %d calls, want 1 after 2", n, calls)
	}
	if buffer.Len() != 2 {
		t.Fatalf("buffer depth = %d, want 2 retained events", buffer.Len())
	}

	calls = 0
	n, err = buffer.Drain(context.Background(), func(seq uint64, value int) error {
		calls++
		if seq != 2 && seq != 3 {
			t.Fatalf("retried unexpected seq %d", seq)
		}
		return nil
	})
	if err != nil || n != 2 || calls != 2 {
		t.Fatalf("retry drain = (%d,%v) after %d calls, want 2,nil,2", n, err, calls)
	}
	if buffer.Len() != 0 {
		t.Fatalf("buffer depth after retry = %d, want 0", buffer.Len())
	}
}
