// Package sim runs a fleet of simulated bots: it assigns pending tasks through
// the scheduler, then drives ASSIGNED -> RUNNING -> DONE/FAILED with
// time-scaled travel and handling delays and an optional failure rate.
package sim

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/local/warehousetaskbridge/internal/metrics"
	"github.com/local/warehousetaskbridge/internal/scheduler"
	"github.com/local/warehousetaskbridge/internal/store"
	"github.com/local/warehousetaskbridge/internal/taskpb"
)

// Store is the persistence surface required by the simulator.
type Store interface {
	ListPendingTasks(ctx context.Context, limit int) ([]*taskpb.Task, error)
	ListActiveTasks(ctx context.Context) ([]*taskpb.Task, error)
	ListBots(ctx context.Context) ([]store.BotRecord, error)
	EnsureBots(ctx context.Context, bots []store.BotRecord) error
	UpdateBot(ctx context.Context, id string, busyUntil time.Time, status string) error
	TransitionTask(ctx context.Context, taskID string, from, to taskpb.TaskState, botID, reason string, at time.Time) (uint64, error)
}

// Config configures the simulated fleet.
type Config struct {
	// BotCount is the fleet size.
	BotCount int
	// SpeedFactor divides simulated durations so demos run quickly; 1 is real time.
	SpeedFactor float64
	// FailureRate is the probability that a running task fails and is requeued.
	FailureRate float64
	// Seed makes failure decisions reproducible.
	Seed int64
	// TickInterval is how often the engine attempts assignments.
	TickInterval time.Duration
	// BatchSize caps tasks examined per tick.
	BatchSize int
	// Metrics and Logger are optional.
	Metrics *metrics.Counters
	Logger  *slog.Logger
}

// Engine owns the simulated bot fleet.
type Engine struct {
	store     Store
	counters  *metrics.Counters
	logger    *slog.Logger
	machine   *scheduler.Machine
	botCount  int
	speed     float64
	failure   float64
	tick      time.Duration
	batchSize int

	mu  sync.Mutex
	rng *rand.Rand
	wg  sync.WaitGroup
}

// NewEngine validates configuration and builds an Engine.
func NewEngine(st Store, cfg Config) (*Engine, error) {
	if st == nil {
		return nil, fmt.Errorf("sim: engine requires a store")
	}
	if cfg.BotCount <= 0 {
		return nil, fmt.Errorf("sim: bot count must be positive, got %d", cfg.BotCount)
	}
	if cfg.SpeedFactor <= 0 {
		return nil, fmt.Errorf("sim: speed factor must be positive, got %f", cfg.SpeedFactor)
	}
	if cfg.FailureRate < 0 || cfg.FailureRate > 1 {
		return nil, fmt.Errorf("sim: failure rate must be within [0,1], got %f", cfg.FailureRate)
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Engine{
		store:     st,
		counters:  cfg.Metrics,
		logger:    cfg.Logger,
		machine:   scheduler.NewMachine(st),
		botCount:  cfg.BotCount,
		speed:     cfg.SpeedFactor,
		failure:   cfg.FailureRate,
		tick:      cfg.TickInterval,
		batchSize: cfg.BatchSize,
		rng:       rand.New(rand.NewSource(cfg.Seed)),
	}, nil
}

// zoneOrder is the deterministic zone rotation for the fleet.
var zoneOrder = []string{"FORWARD", "RESERVE", "BULK", "OVERFLOW"}

// homeAisle maps a zone to a home aisle letter.
var homeAisle = map[string]string{
	"FORWARD":  "A",
	"RESERVE":  "G",
	"BULK":     "M",
	"OVERFLOW": "S",
}

// Fleet returns the deterministic bot roster for the configured fleet size.
func (e *Engine) Fleet() []store.BotRecord {
	bots := make([]store.BotRecord, 0, e.botCount)
	for i := 0; i < e.botCount; i++ {
		zone := zoneOrder[i%len(zoneOrder)]
		bots = append(bots, store.BotRecord{
			ID:       fmt.Sprintf("bot-%02d", i+1),
			Zone:     zone,
			HomeSlot: fmt.Sprintf("%s-%d-%d", homeAisle[zone], 1+i/len(zoneOrder), 1),
			Status:   "IDLE",
		})
	}
	return bots
}

// EnsureFleet inserts the fleet if it does not already exist.
func (e *Engine) EnsureFleet(ctx context.Context) error {
	return e.store.EnsureBots(ctx, e.Fleet())
}

// Recover requeues tasks orphaned by a previous process: each ASSIGNED or
// RUNNING task is failed with an explicit reason and then returned to PENDING.
// Every step emits an outbox event, so the WMS sees honest recovery state.
func (e *Engine) Recover(ctx context.Context) (int, error) {
	tasks, err := e.store.ListActiveTasks(ctx)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, task := range tasks {
		state := task.GetState()
		if state != taskpb.TaskState_ASSIGNED && state != taskpb.TaskState_RUNNING {
			continue
		}
		if _, err := e.machine.Transition(ctx, task.GetId(), state, taskpb.TaskState_FAILED,
			"", "bridge restart: orphaned in-flight task", time.Now().UTC()); err != nil {
			e.logger.Warn("recover orphaned task", "task_id", task.GetId(), "error", err)
			continue
		}
		if _, err := e.machine.Transition(ctx, task.GetId(), taskpb.TaskState_FAILED,
			taskpb.TaskState_PENDING, "", "requeue after bridge restart", time.Now().UTC()); err != nil {
			e.logger.Warn("requeue orphaned task", "task_id", task.GetId(), "error", err)
			continue
		}
		e.counters.Inc(metrics.TasksFailed)
		e.counters.Inc(metrics.TasksRequeued)
		recovered++
	}
	if recovered > 0 {
		e.logger.Info("recovered orphaned tasks", "count", recovered)
	}
	return recovered, nil
}

// Run assigns and executes work until ctx is cancelled, then waits for in-flight
// simulated bots to finish their current handling step.
func (e *Engine) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			e.wg.Wait()
			return nil
		case <-ticker.C:
			if _, err := e.Step(ctx); err != nil {
				e.logger.Error("simulator step failed", "error", err)
			}
		}
	}
}

// Step performs one deterministic assignment pass and launches a worker for
// each assignment. It returns the number of tasks assigned.
func (e *Engine) Step(ctx context.Context) (int, error) {
	tasks, err := e.store.ListPendingTasks(ctx, e.batchSize)
	if err != nil {
		return 0, err
	}
	if len(tasks) == 0 {
		return 0, nil
	}
	records, err := e.store.ListBots(ctx)
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	bots := make([]scheduler.Bot, 0, len(records))
	for _, rec := range records {
		bots = append(bots, scheduler.Bot{
			ID: rec.ID, Zone: rec.Zone, HomeSlot: rec.HomeSlot,
			BusyUntil: rec.BusyUntil, Status: rec.Status,
		})
	}
	assignments := scheduler.Assign(ctx, tasks, bots, now)
	for _, assignment := range assignments {
		task := assignment.Task
		reason := "scheduler: " + scheduler.Explain(assignment)
		if _, err := e.machine.Transition(ctx, task.GetId(), taskpb.TaskState_PENDING,
			taskpb.TaskState_ASSIGNED, assignment.Bot.ID, reason, now); err != nil {
			e.logger.Warn("assign task transition failed", "task_id", task.GetId(), "error", err)
			continue
		}
		e.counters.Inc(metrics.TasksAssigned)

		duration := e.durationFor(task, assignment.Bot)
		if err := e.store.UpdateBot(ctx, assignment.Bot.ID, now.Add(duration), "BUSY"); err != nil {
			e.logger.Warn("mark bot busy", "bot_id", assignment.Bot.ID, "error", err)
		}
		fail := e.shouldFail()
		e.wg.Add(1)
		go e.runTask(task, assignment.Bot, duration, fail)
	}
	if len(assignments) > 0 {
		e.logger.Debug("simulator assigned tasks", "count", len(assignments))
	}
	return len(assignments), nil
}

// durationFor derives a simulated handling duration from the cost model,
// divided by the configured speed factor.
func (e *Engine) durationFor(task *taskpb.Task, bot scheduler.Bot) time.Duration {
	cost := scheduler.Cost(task, bot)
	base := 500*time.Millisecond +
		time.Duration(cost)*20*time.Millisecond +
		time.Duration(task.GetQuantity())*30*time.Millisecond
	scaled := time.Duration(float64(base) / e.speed)
	if scaled < 5*time.Millisecond {
		scaled = 5 * time.Millisecond
	}
	if scaled > 30*time.Second {
		scaled = 30 * time.Second
	}
	return scaled
}

func (e *Engine) shouldFail() bool {
	if e.failure <= 0 {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rng.Float64() < e.failure
}

func (e *Engine) runTask(task *taskpb.Task, bot scheduler.Bot, duration time.Duration, fail bool) {
	defer e.wg.Done()
	bg, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	half := duration / 2
	if half < time.Millisecond {
		half = time.Millisecond
	}
	time.Sleep(half)

	if _, err := e.machine.Transition(bg, task.GetId(), taskpb.TaskState_ASSIGNED,
		taskpb.TaskState_RUNNING, bot.ID, "bot started handling", time.Now().UTC()); err != nil {
		e.logger.Warn("start task transition failed", "task_id", task.GetId(), "error", err)
		e.freeBot(bot.ID)
		return
	}

	time.Sleep(duration - half)
	target := taskpb.TaskState_DONE
	reason := "bot completed handling"
	if fail {
		target = taskpb.TaskState_FAILED
		reason = "simulated bot handling failure"
	}
	if _, err := e.machine.Transition(bg, task.GetId(), taskpb.TaskState_RUNNING,
		target, bot.ID, reason, time.Now().UTC()); err != nil {
		e.logger.Warn("finish task transition failed", "task_id", task.GetId(), "error", err)
		e.freeBot(bot.ID)
		return
	}
	if fail {
		e.counters.Inc(metrics.TasksFailed)
		if _, err := e.machine.Transition(bg, task.GetId(), taskpb.TaskState_FAILED,
			taskpb.TaskState_PENDING, "", "requeue after simulated failure", time.Now().UTC()); err != nil {
			e.logger.Warn("requeue failed task", "task_id", task.GetId(), "error", err)
		} else {
			e.counters.Inc(metrics.TasksRequeued)
		}
	} else {
		e.counters.Inc(metrics.TasksCompleted)
	}
	e.freeBot(bot.ID)
}

func (e *Engine) freeBot(botID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.store.UpdateBot(ctx, botID, time.Time{}, "IDLE"); err != nil {
		e.logger.Warn("free bot", "bot_id", botID, "error", err)
	}
}
