package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/local/warehousetaskbridge/internal/taskpb"
)

func testTask(id string, priority int32, zone, source string, created time.Time) *taskpb.Task {
	return &taskpb.Task{
		Id:         id,
		OrderRef:   "ORD-" + id,
		Priority:   priority,
		Zone:       zone,
		SourceSlot: source,
		DestSlot:   "A-1-1",
		Quantity:   1,
		State:      taskpb.TaskState_PENDING,
		CreatedAt:  timestamppb.New(created),
	}
}

func testBot(id, zone, home string) Bot {
	return Bot{ID: id, Zone: zone, HomeSlot: home, Status: "IDLE"}
}

func TestAssignPriorityOrdering(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tasks := []*taskpb.Task{
		testTask("bulk", 90, "FORWARD", "A-1-1", now),
		testTask("expedite", 10, "FORWARD", "A-1-1", now),
		testTask("standard", 50, "FORWARD", "A-1-1", now),
	}
	assignments := Assign(context.Background(), tasks, []Bot{testBot("bot-01", "FORWARD", "A-1-1")}, now)
	if len(assignments) != 1 {
		t.Fatalf("assignments = %d, want 1", len(assignments))
	}
	if assignments[0].Task.Id != "expedite" {
		t.Fatalf("assigned %s, want expedite (lower priority number)", assignments[0].Task.Id)
	}
}

func TestAssignZoneAffinity(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	task := testTask("task-1", 50, "FORWARD", "A-10-1", now)
	// The same-zone bot is farther away, but the zone penalty dominates.
	bots := []Bot{
		testBot("bot-away", "FORWARD", "Z-1-1"),
		testBot("bot-close", "RESERVE", "A-1-1"),
	}
	assignments := Assign(context.Background(), []*taskpb.Task{task}, bots, now)
	if len(assignments) != 1 || assignments[0].Bot.ID != "bot-away" {
		t.Fatalf("assignment = %+v, want same-zone bot-away", assignments)
	}
	if assignments[0].ZonePenalty != 0 {
		t.Fatalf("same-zone assignment carries penalty %f", assignments[0].ZonePenalty)
	}
}

func TestAssignTravelCost(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	task := testTask("task-1", 50, "FORWARD", "A-1-1", now)
	bots := []Bot{
		testBot("bot-far", "FORWARD", "C-5-3"),
		testBot("bot-near", "FORWARD", "A-1-2"),
	}
	assignments := Assign(context.Background(), []*taskpb.Task{task}, bots, now)
	if len(assignments) != 1 || assignments[0].Bot.ID != "bot-near" {
		t.Fatalf("assignment = %+v, want bot-near", assignments)
	}
	if got, want := assignments[0].TravelCost, TravelCost("A-1-2", "A-1-1")*WeightTravel; got != want {
		t.Fatalf("travel cost = %f, want %f", got, want)
	}
}

func TestAssignTieBreakingIsDeterministic(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tasks := []*taskpb.Task{
		testTask("task-c", 50, "FORWARD", "A-1-1", now),
		testTask("task-a", 50, "FORWARD", "A-1-1", now),
		testTask("task-b", 50, "FORWARD", "A-1-1", now),
	}
	bots := []Bot{
		testBot("bot-02", "FORWARD", "A-1-1"),
		testBot("bot-01", "FORWARD", "A-1-1"),
	}

	var baseline string
	for run := 0; run < 100; run++ {
		assignments := Assign(context.Background(), tasks, bots, now)
		var b strings.Builder
		for _, a := range assignments {
			fmt.Fprintf(&b, "%s->%s;", a.Task.Id, a.Bot.ID)
		}
		got := b.String()
		if run == 0 {
			baseline = got
			// CreatedAt ties fall through to lexicographic task id, and equal
			// costs fall through to lexicographic bot id.
			if got != "task-a->bot-01;task-b->bot-02;" {
				t.Fatalf("tie-break order = %q", got)
			}
			continue
		}
		if got != baseline {
			t.Fatalf("run %d produced %q, baseline %q", run, got, baseline)
		}
	}
}

func TestAssignRespectsBusyAndOfflineBots(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	task := testTask("task-1", 10, "FORWARD", "A-1-1", now)
	busy := testBot("bot-busy", "FORWARD", "A-1-1")
	busy.BusyUntil = now.Add(time.Minute)
	offline := testBot("bot-offline", "FORWARD", "A-1-1")
	offline.Status = "OFFLINE"

	if got := Assign(context.Background(), []*taskpb.Task{task}, []Bot{busy, offline}, now); len(got) != 0 {
		t.Fatalf("assigned to unavailable bots: %+v", got)
	}
	idle := testBot("bot-idle", "FORWARD", "A-1-1")
	if got := Assign(context.Background(), []*taskpb.Task{task}, []Bot{busy, idle, offline}, now); len(got) != 1 || got[0].Bot.ID != "bot-idle" {
		t.Fatalf("expected only idle bot, got %+v", got)
	}
}

func TestExplain(t *testing.T) {
	now := time.Now()
	task := testTask("task-1", 10, "FORWARD", "A-1-1", now)
	assignments := Assign(context.Background(), []*taskpb.Task{task},
		[]Bot{testBot("bot-01", "FORWARD", "A-1-1")}, now)
	if len(assignments) != 1 {
		t.Fatal("expected one assignment")
	}
	explanation := Explain(assignments[0])
	for _, want := range []string{"task-1", "ORD-task-1", "bot-01", "priority 10", "cost"} {
		if !strings.Contains(explanation, want) {
			t.Errorf("explanation missing %q: %s", want, explanation)
		}
	}
}

func TestStateMachineTransitionTable(t *testing.T) {
	legal := []struct{ from, to taskpb.TaskState }{
		{taskpb.TaskState_PENDING, taskpb.TaskState_ASSIGNED},
		{taskpb.TaskState_ASSIGNED, taskpb.TaskState_RUNNING},
		{taskpb.TaskState_ASSIGNED, taskpb.TaskState_FAILED},
		{taskpb.TaskState_RUNNING, taskpb.TaskState_DONE},
		{taskpb.TaskState_RUNNING, taskpb.TaskState_FAILED},
		{taskpb.TaskState_FAILED, taskpb.TaskState_PENDING},
	}
	for _, edge := range legal {
		if !CanTransition(edge.from, edge.to) {
			t.Errorf("expected legal: %s -> %s", edge.from, edge.to)
		}
	}
	illegal := []struct{ from, to taskpb.TaskState }{
		{taskpb.TaskState_PENDING, taskpb.TaskState_DONE},
		{taskpb.TaskState_PENDING, taskpb.TaskState_RUNNING},
		{taskpb.TaskState_DONE, taskpb.TaskState_RUNNING},
		{taskpb.TaskState_DONE, taskpb.TaskState_PENDING},
		{taskpb.TaskState_RUNNING, taskpb.TaskState_PENDING},
		{taskpb.TaskState_FAILED, taskpb.TaskState_DONE},
	}
	for _, edge := range illegal {
		if CanTransition(edge.from, edge.to) {
			t.Errorf("expected illegal: %s -> %s", edge.from, edge.to)
		}
		err := ValidateTransition(edge.from, edge.to)
		if !errors.Is(err, ErrIllegalTransition) {
			t.Errorf("ValidateTransition(%s,%s) = %v, want ErrIllegalTransition", edge.from, edge.to, err)
		}
	}
}

type fakeStateStore struct {
	calls int
	seq   uint64
}

func (f *fakeStateStore) TransitionTask(_ context.Context, _ string, _, _ taskpb.TaskState, _, _ string, _ time.Time) (uint64, error) {
	f.calls++
	f.seq++
	return f.seq, nil
}

func TestMachineRejectsIllegalBeforeStore(t *testing.T) {
	st := &fakeStateStore{}
	machine := NewMachine(st)
	_, err := machine.Transition(context.Background(), "task-1",
		taskpb.TaskState_DONE, taskpb.TaskState_RUNNING, "bot-01", "nope", time.Now())
	if !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("error = %v, want ErrIllegalTransition", err)
	}
	if st.calls != 0 {
		t.Fatalf("illegal transition reached the store %d times", st.calls)
	}
	seq, err := machine.Transition(context.Background(), "task-1",
		taskpb.TaskState_PENDING, taskpb.TaskState_ASSIGNED, "bot-01", "ok", time.Now())
	if err != nil || seq != 1 || st.calls != 1 {
		t.Fatalf("legal transition = (seq=%d, err=%v, calls=%d)", seq, err, st.calls)
	}
}

func BenchmarkAssign(b *testing.B) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tasks := make([]*taskpb.Task, 10_000)
	zones := []string{"FORWARD", "RESERVE", "BULK", "OVERFLOW"}
	aisles := []string{"A", "G", "M", "S"}
	for i := range tasks {
		zone := i % len(zones)
		tasks[i] = testTask(
			fmt.Sprintf("task-%05d", i),
			int32(10+(i%9)*10),
			zones[zone],
			fmt.Sprintf("%s-%d-%d", aisles[zone], 1+i%50, 1+i%4),
			now.Add(time.Duration(i)*time.Millisecond),
		)
	}
	bots := make([]Bot, 50)
	for i := range bots {
		zone := i % len(zones)
		bots[i] = testBot(
			fmt.Sprintf("bot-%02d", i+1),
			zones[zone],
			fmt.Sprintf("%s-%d-%d", aisles[zone], 1+i%50, 1+i%4),
		)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	var assigned int
	for i := 0; i < b.N; i++ {
		assigned = len(Assign(ctx, tasks, bots, now))
	}
	b.StopTimer()
	if assigned != 50 {
		b.Fatalf("assigned %d tasks, want 50 (one per bot)", assigned)
	}
}
