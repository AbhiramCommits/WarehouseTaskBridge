package sim

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/local/warehousetaskbridge/internal/metrics"
	"github.com/local/warehousetaskbridge/internal/store"
	"github.com/local/warehousetaskbridge/internal/taskpb"
)

type fakeSimStore struct {
	mu      sync.Mutex
	tasks   map[string]*taskpb.Task
	bots    map[string]store.BotRecord
	events  chan *taskpb.TaskStateEvent
	botsSet bool
}

func newFakeSimStore(tasks ...*taskpb.Task) *fakeSimStore {
	byID := make(map[string]*taskpb.Task)
	for _, task := range tasks {
		byID[task.Id] = task
	}
	return &fakeSimStore{
		tasks:  byID,
		bots:   make(map[string]store.BotRecord),
		events: make(chan *taskpb.TaskStateEvent, 64),
	}
}

func (f *fakeSimStore) ListPendingTasks(context.Context, int) ([]*taskpb.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*taskpb.Task
	for _, task := range f.tasks {
		if task.State == taskpb.TaskState_PENDING {
			out = append(out, task)
		}
	}
	return out, nil
}

func (f *fakeSimStore) ListActiveTasks(context.Context) ([]*taskpb.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*taskpb.Task
	for _, task := range f.tasks {
		if task.State == taskpb.TaskState_ASSIGNED || task.State == taskpb.TaskState_RUNNING {
			out = append(out, task)
		}
	}
	return out, nil
}

func (f *fakeSimStore) ListBots(context.Context) ([]store.BotRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.BotRecord, 0, len(f.bots))
	for _, bot := range f.bots {
		out = append(out, bot)
	}
	return out, nil
}

func (f *fakeSimStore) EnsureBots(_ context.Context, bots []store.BotRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.botsSet = true
	for _, bot := range bots {
		f.bots[bot.ID] = bot
	}
	return nil
}

func (f *fakeSimStore) UpdateBot(_ context.Context, id string, busyUntil time.Time, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	bot, ok := f.bots[id]
	if !ok {
		return errors.New("bot not found")
	}
	bot.BusyUntil = busyUntil
	bot.Status = status
	f.bots[id] = bot
	return nil
}

func (f *fakeSimStore) TransitionTask(_ context.Context, taskID string, from, to taskpb.TaskState, botID, _ string, at time.Time) (uint64, error) {
	f.mu.Lock()
	task, ok := f.tasks[taskID]
	if !ok {
		f.mu.Unlock()
		return 0, errors.New("task not found")
	}
	if task.State != from {
		current := task.State
		f.mu.Unlock()
		return 0, errors.New("state conflict: " + current.String())
	}
	task.State = to
	task.AssignedBotId = botID
	task.UpdatedAt = nil
	ev := &taskpb.TaskStateEvent{TaskId: taskID, OrderRef: task.OrderRef, FromState: from, ToState: to, Seq: uint64(len(f.events) + 1)}
	f.mu.Unlock()
	f.events <- ev
	return ev.Seq, nil
}

func (f *fakeSimStore) waitFor(t *testing.T, state taskpb.TaskState) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-f.events:
			if ev.ToState == state {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for state %s", state)
		}
	}
}

func simTask(id string, priority int32) *taskpb.Task {
	return &taskpb.Task{
		Id:         id,
		OrderRef:   "ORD-" + id,
		TaskType:   taskpb.TaskType_PICK,
		Priority:   priority,
		Zone:       "FORWARD",
		SourceSlot: "A-1-1",
		DestSlot:   "A-2-1",
		Quantity:   1,
		State:      taskpb.TaskState_PENDING,
	}
}

func testEngine(t *testing.T, st Store, failure float64) *Engine {
	t.Helper()
	engine, err := NewEngine(st, Config{
		BotCount:     2,
		SpeedFactor:  1000,
		FailureRate:  failure,
		Seed:         7,
		TickInterval: 20 * time.Millisecond,
		Metrics:      metrics.New(),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return engine
}

func TestFleetIsDeterministic(t *testing.T) {
	engine := testEngine(t, newFakeSimStore(), 0)
	fleet := engine.Fleet()
	if len(fleet) != 2 {
		t.Fatalf("fleet size = %d", len(fleet))
	}
	if fleet[0].ID != "bot-01" || fleet[0].Zone != "FORWARD" || fleet[0].HomeSlot != "A-1-1" {
		t.Fatalf("bot 0 = %+v", fleet[0])
	}
	if fleet[1].ID != "bot-02" || fleet[1].Zone != "RESERVE" || fleet[1].HomeSlot != "G-1-1" {
		t.Fatalf("bot 1 = %+v", fleet[1])
	}
}

func TestEngineCompletesTask(t *testing.T) {
	st := newFakeSimStore(simTask("task-1", 10))
	engine := testEngine(t, st, 0)
	ctx := context.Background()
	if err := engine.EnsureFleet(ctx); err != nil {
		t.Fatalf("ensure fleet: %v", err)
	}
	assigned, err := engine.Step(ctx)
	if err != nil || assigned != 1 {
		t.Fatalf("step = (%d,%v), want 1", assigned, err)
	}
	st.waitFor(t, taskpb.TaskState_ASSIGNED)
	st.waitFor(t, taskpb.TaskState_RUNNING)
	st.waitFor(t, taskpb.TaskState_DONE)

	deadline := time.After(2 * time.Second)
	for {
		st.mu.Lock()
		status := st.bots["bot-01"].Status
		st.mu.Unlock()
		if status == "IDLE" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("bot did not return to IDLE")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestEngineFailureRequeues(t *testing.T) {
	st := newFakeSimStore(simTask("task-1", 10))
	engine := testEngine(t, st, 1.0)
	ctx := context.Background()
	if err := engine.EnsureFleet(ctx); err != nil {
		t.Fatalf("ensure fleet: %v", err)
	}
	if assigned, err := engine.Step(ctx); err != nil || assigned != 1 {
		t.Fatalf("step = (%d,%v)", assigned, err)
	}
	st.waitFor(t, taskpb.TaskState_FAILED)
	st.waitFor(t, taskpb.TaskState_PENDING)
}

func TestRecoverOrphanedTasks(t *testing.T) {
	task := simTask("task-1", 10)
	task.State = taskpb.TaskState_ASSIGNED
	st := newFakeSimStore(task)
	engine := testEngine(t, st, 0)
	recovered, err := engine.Recover(context.Background())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}
	st.mu.Lock()
	state := st.tasks["task-1"].State
	st.mu.Unlock()
	if state != taskpb.TaskState_PENDING {
		t.Fatalf("task state = %s, want PENDING", state)
	}
}

func TestNewEngineValidation(t *testing.T) {
	st := newFakeSimStore()
	for _, cfg := range []Config{
		{BotCount: 0, SpeedFactor: 1},
		{BotCount: 1, SpeedFactor: 0},
		{BotCount: 1, SpeedFactor: 1, FailureRate: 2},
	} {
		if _, err := NewEngine(st, cfg); err == nil {
			t.Fatalf("NewEngine(%+v) accepted invalid config", cfg)
		}
	}
}
