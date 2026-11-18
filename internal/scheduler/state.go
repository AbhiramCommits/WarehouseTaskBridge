package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/local/warehousetaskbridge/internal/taskpb"
)

// ErrIllegalTransition is returned when a state change is not in the legal
// transition table. It is never silently applied.
var ErrIllegalTransition = errors.New("scheduler: illegal state transition")

// legalTransitions is the explicit state machine. FAILED->PENDING is the one
// recovery edge, used to requeue work after a simulated bot failure.
var legalTransitions = map[taskpb.TaskState]map[taskpb.TaskState]bool{
	taskpb.TaskState_PENDING: {
		taskpb.TaskState_ASSIGNED: true,
	},
	taskpb.TaskState_ASSIGNED: {
		taskpb.TaskState_RUNNING: true,
		taskpb.TaskState_FAILED:  true,
	},
	taskpb.TaskState_RUNNING: {
		taskpb.TaskState_DONE:   true,
		taskpb.TaskState_FAILED: true,
	},
	taskpb.TaskState_DONE: {},
	taskpb.TaskState_FAILED: {
		taskpb.TaskState_PENDING: true,
	},
}

// CanTransition reports whether from -> to is legal.
func CanTransition(from, to taskpb.TaskState) bool {
	return legalTransitions[from][to]
}

// ValidateTransition returns ErrIllegalTransition when the edge is not legal.
func ValidateTransition(from, to taskpb.TaskState) error {
	if CanTransition(from, to) {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrIllegalTransition, from, to)
}

// StateStore performs the transactional task update plus outbox insert.
type StateStore interface {
	TransitionTask(ctx context.Context, taskID string, from, to taskpb.TaskState, botID, reason string, at time.Time) (uint64, error)
}

// Machine applies validated transitions through a transactional store.
type Machine struct {
	store StateStore
}

// NewMachine builds a state machine over a transactional store.
func NewMachine(store StateStore) *Machine {
	return &Machine{store: store}
}

// Transition validates the edge, then atomically updates the task row and
// inserts a task_events outbox row. It returns the outbox sequence.
func (m *Machine) Transition(ctx context.Context, taskID string, from, to taskpb.TaskState, botID, reason string, at time.Time) (uint64, error) {
	if err := ValidateTransition(from, to); err != nil {
		return 0, err
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	return m.store.TransitionTask(ctx, taskID, from, to, botID, reason, at)
}
