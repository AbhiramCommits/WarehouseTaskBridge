// Package store provides PostgreSQL persistence for tasks, the task-event
// outbox, bots, quarantine entries, and peer heartbeats. It uses pgx's standard
// connection pool and plain SQL; there is no ORM.
package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/local/warehousetaskbridge/internal/taskpb"
	"github.com/local/warehousetaskbridge/migrations"
)

// advisoryLockKey serializes concurrent migrations across processes.
const advisoryLockKey int64 = 741_852_963

// Store wraps a pgxpool connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open creates a Store and verifies connectivity with a ping.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.MaxConnLifetime = time.Hour
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// New wraps an existing pool. It is primarily useful for tests.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Close releases all pooled connections.
func (s *Store) Close() {
	s.pool.Close()
}

// Pool exposes the underlying pgx pool for components that need it.
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// migrationFile is one parsed, ordered migration.
type migrationFile struct {
	version int
	name    string
	body    string
}

// Migrate applies every embedded SQL migration in version order, exactly once.
// It is idempotent and safe to call concurrently from multiple processes.
func (s *Store) Migrate(ctx context.Context) error {
	files, err := loadMigrations()
	if err != nil {
		return err
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("store: acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("store: acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", advisoryLockKey)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer PRIMARY KEY,
		name       text        NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("store: create schema_migrations: %w", err)
	}

	for _, file := range files {
		var exists bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
			file.version).Scan(&exists); err != nil {
			return fmt.Errorf("store: check migration %s: %w", file.name, err)
		}
		if exists {
			continue
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, file.body); err != nil {
				return fmt.Errorf("apply: %w", err)
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
				file.version, file.name)
			return err
		})
		if err != nil {
			return fmt.Errorf("store: migration %s: %w", file.name, err)
		}
	}
	return nil
}

func loadMigrations() ([]migrationFile, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("store: read migrations: %w", err)
	}
	var files []migrationFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version, err := parseVersion(entry.Name())
		if err != nil {
			return nil, err
		}
		body, err := fs.ReadFile(migrations.FS, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("store: read migration %s: %w", entry.Name(), err)
		}
		files = append(files, migrationFile{version: version, name: entry.Name(), body: string(body)})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	return files, nil
}

func parseVersion(name string) (int, error) {
	base, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, fmt.Errorf("store: migration %q must be named NNNN_description.sql", name)
	}
	v, err := strconv.Atoi(base)
	if err != nil {
		return 0, fmt.Errorf("store: migration %q has a non-numeric version prefix", name)
	}
	return v, nil
}

// ErrTaskNotFound is returned when a task lookup yields no row.
var ErrTaskNotFound = errors.New("store: task not found")

// taskColumns is the canonical projection for task scans.
const taskColumns = `id, order_ref, idempotency_key, task_type, priority, zone,
	source_slot, dest_slot, quantity, state, COALESCE(assigned_bot_id, ''),
	created_at, updated_at`

// scanTask converts a row into the internal protobuf task.
func scanTask(row pgx.Row) (*taskpb.Task, error) {
	var (
		id, orderRef, idemKey, taskType, zone string
		sourceSlot, destSlot, state, botID    string
		priority, quantity                    int32
		createdAt, updatedAt                  time.Time
	)
	err := row.Scan(&id, &orderRef, &idemKey, &taskType, &priority, &zone,
		&sourceSlot, &destSlot, &quantity, &state, &botID, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	return &taskpb.Task{
		Id:             id,
		OrderRef:       orderRef,
		TaskType:       taskpb.TaskType(taskpb.TaskType_value[taskType]),
		Priority:       priority,
		Zone:           zone,
		SourceSlot:     sourceSlot,
		DestSlot:       destSlot,
		Quantity:       quantity,
		State:          taskpb.TaskState(taskpb.TaskState_value[state]),
		AssignedBotId:  botID,
		CreatedAt:      timestamppbNew(createdAt),
		UpdatedAt:      timestamppbNew(updatedAt),
		IdempotencyKey: idemKey,
	}, nil
}

// InsertTask attempts to insert a task keyed by its idempotency key.
// It returns the task id and true when the task was created, or the id of the
// existing task and false when the key was already present.
func (s *Store) InsertTask(ctx context.Context, task *taskpb.Task, payloadHash string) (string, bool, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO tasks (id, order_ref, idempotency_key, payload_hash, task_type,
			priority, zone, source_slot, dest_slot, quantity, state, assigned_bot_id,
			created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, ''), $13, $14)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`,
		task.Id, task.OrderRef, task.IdempotencyKey, payloadHash,
		taskpb.TaskType_name[int32(task.TaskType)], task.Priority, task.Zone,
		task.SourceSlot, task.DestSlot, task.Quantity,
		taskpb.TaskState_name[int32(task.State)], task.AssignedBotId,
		timestampOf(task.CreatedAt), timestampOf(task.UpdatedAt),
	).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, fmt.Errorf("store: insert task: %w", err)
	}
	existing, _, err := s.TaskByIdempotencyKey(ctx, task.IdempotencyKey)
	if err != nil {
		return "", false, err
	}
	return existing.Id, false, nil
}

// TaskByIdempotencyKey returns an existing task and its stored payload hash.
func (s *Store) TaskByIdempotencyKey(ctx context.Context, key string) (*taskpb.Task, string, error) {
	var hash string
	row := s.pool.QueryRow(ctx,
		`SELECT `+taskColumns+`, payload_hash FROM tasks WHERE idempotency_key = $1`, key)
	// Scan manually so the extra hash column stays last.
	var (
		id, orderRef, idemKey, taskType, zone string
		sourceSlot, destSlot, state, botID    string
		priority, quantity                    int32
		createdAt, updatedAt                  time.Time
	)
	if err := row.Scan(&id, &orderRef, &idemKey, &taskType, &priority, &zone,
		&sourceSlot, &destSlot, &quantity, &state, &botID, &createdAt, &updatedAt, &hash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", ErrTaskNotFound
		}
		return nil, "", fmt.Errorf("store: task by idempotency key: %w", err)
	}
	return &taskpb.Task{
		Id:             id,
		OrderRef:       orderRef,
		TaskType:       taskpb.TaskType(taskpb.TaskType_value[taskType]),
		Priority:       priority,
		Zone:           zone,
		SourceSlot:     sourceSlot,
		DestSlot:       destSlot,
		Quantity:       quantity,
		State:          taskpb.TaskState(taskpb.TaskState_value[state]),
		AssignedBotId:  botID,
		CreatedAt:      timestamppbNew(createdAt),
		UpdatedAt:      timestamppbNew(updatedAt),
		IdempotencyKey: idemKey,
	}, hash, nil
}

// GetTask returns a task by id.
func (s *Store) GetTask(ctx context.Context, id string) (*taskpb.Task, error) {
	task, err := scanTask(s.pool.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get task: %w", err)
	}
	return task, nil
}

// ListTasks returns tasks filtered by state, or all tasks when state is empty.
// Results are ordered newest-first for stable API output.
func (s *Store) ListTasks(ctx context.Context, state string, limit int) ([]*taskpb.Task, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var (
		rows pgx.Rows
		err  error
	)
	if state == "" {
		rows, err = s.pool.Query(ctx,
			`SELECT `+taskColumns+` FROM tasks ORDER BY created_at DESC, id LIMIT $1`, limit)
	} else {
		rows, err = s.pool.Query(ctx,
			`SELECT `+taskColumns+` FROM tasks WHERE state = $1 ORDER BY created_at DESC, id LIMIT $2`,
			state, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("store: list tasks: %w", err)
	}
	defer rows.Close()

	var out []*taskpb.Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan task: %w", err)
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

// ListPendingTasks returns PENDING tasks ordered by priority and age.
func (s *Store) ListPendingTasks(ctx context.Context, limit int) ([]*taskpb.Task, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, `SELECT `+taskColumns+`
		FROM tasks WHERE state = 'PENDING'
		ORDER BY priority ASC, created_at ASC, id ASC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list pending tasks: %w", err)
	}
	defer rows.Close()
	var out []*taskpb.Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan pending task: %w", err)
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

// InsertQuarantine records a payload that could not be accepted.
func (s *Store) InsertQuarantine(ctx context.Context, subject string, raw []byte, reason string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO quarantine (subject, raw, reason) VALUES ($1, $2, $3)`,
		subject, raw, reason)
	if err != nil {
		return fmt.Errorf("store: insert quarantine: %w", err)
	}
	return nil
}

// QuarantineCount returns the number of quarantined payloads.
func (s *Store) QuarantineCount(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM quarantine`).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count quarantine: %w", err)
	}
	return n, nil
}

// RecordHeartbeat upserts the last-seen timestamp for a peer.
func (s *Store) RecordHeartbeat(ctx context.Context, peerID string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO peer_heartbeats (peer_id, last_seen_at) VALUES ($1, $2)
		ON CONFLICT (peer_id) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at`,
		peerID, at)
	if err != nil {
		return fmt.Errorf("store: record heartbeat: %w", err)
	}
	return nil
}

// LastHeartbeat returns the most recent inbound heartbeat timestamp for a peer.
func (s *Store) LastHeartbeat(ctx context.Context, peerID string) (time.Time, error) {
	var at time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT last_seen_at FROM peer_heartbeats WHERE peer_id = $1`, peerID).Scan(&at)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrTaskNotFound
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("store: last heartbeat: %w", err)
	}
	return at, nil
}

// EnsureBots inserts missing bots and refreshes zone/home slot for existing ones.
// Busy state is preserved so restarts do not free genuinely busy bots.
func (s *Store) EnsureBots(ctx context.Context, bots []BotRecord) error {
	batch := &pgx.Batch{}
	for _, b := range bots {
		batch.Queue(`
			INSERT INTO bots (id, zone, home_slot, status) VALUES ($1, $2, $3, 'IDLE')
			ON CONFLICT (id) DO UPDATE SET zone = EXCLUDED.zone, home_slot = EXCLUDED.home_slot`,
			b.ID, b.Zone, b.HomeSlot)
	}
	results := s.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range bots {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("store: ensure bots: %w", err)
		}
	}
	return nil
}

// ListBots returns the fleet ordered by id.
func (s *Store) ListBots(ctx context.Context) ([]BotRecord, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, zone, home_slot, busy_until, status FROM bots ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("store: list bots: %w", err)
	}
	defer rows.Close()
	var out []BotRecord
	for rows.Next() {
		var (
			b         BotRecord
			busyUntil *time.Time
		)
		if err := rows.Scan(&b.ID, &b.Zone, &b.HomeSlot, &busyUntil, &b.Status); err != nil {
			return nil, fmt.Errorf("store: scan bot: %w", err)
		}
		if busyUntil != nil {
			b.BusyUntil = *busyUntil
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// UpdateBot writes a bot's busy-until time and status.
func (s *Store) UpdateBot(ctx context.Context, id string, busyUntil time.Time, status string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE bots SET busy_until = $2, status = $3 WHERE id = $1`, id, busyUntil, status)
	if err != nil {
		return fmt.Errorf("store: update bot: %w", err)
	}
	return nil
}

// BotRecord is the persistence view of a simulated bot.
type BotRecord struct {
	ID        string
	Zone      string
	HomeSlot  string
	BusyUntil time.Time
	Status    string
}

// ErrTransitionConflict is returned when a task is not in the expected state.
var ErrTransitionConflict = errors.New("store: task state conflict")

// TransitionTask atomically updates a task and appends its outbox event in a
// single transaction. It returns the new task_events seq. If the task is not in
// fromState, the update is rejected and no event is written.
func (s *Store) TransitionTask(ctx context.Context, taskID string, from, to taskpb.TaskState, botID, reason string, at time.Time) (uint64, error) {
	var seq uint64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var orderRef string
		var assignedBot *string
		if botID != "" && to != taskpb.TaskState_PENDING {
			assignedBot = &botID
		}
		err := tx.QueryRow(ctx, `
			UPDATE tasks
			SET state = $2,
			    assigned_bot_id = CASE
			        WHEN $2 = 'PENDING' THEN NULL
			        WHEN $3::text IS NOT NULL THEN $3
			        ELSE assigned_bot_id
			    END,
			    updated_at = $4
			WHERE id = $1 AND state = $5
			RETURNING order_ref`,
			taskID, taskpb.TaskState_name[int32(to)], assignedBot, at,
			taskpb.TaskState_name[int32(from)]).Scan(&orderRef)
		if errors.Is(err, pgx.ErrNoRows) {
			var current string
			lookupErr := tx.QueryRow(ctx, `SELECT state FROM tasks WHERE id = $1`, taskID).Scan(&current)
			if errors.Is(lookupErr, pgx.ErrNoRows) {
				return ErrTaskNotFound
			}
			if lookupErr != nil {
				return fmt.Errorf("store: inspect task state: %w", lookupErr)
			}
			return fmt.Errorf("%w: task %s is %s, expected %s", ErrTransitionConflict, taskID, current, from)
		}
		if err != nil {
			return fmt.Errorf("store: update task state: %w", err)
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO task_events (task_id, order_ref, from_state, to_state, bot_id, reason, emitted_at)
			VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7)
			RETURNING seq`,
			taskID, orderRef, taskpb.TaskState_name[int32(from)],
			taskpb.TaskState_name[int32(to)], botID, reason, at).Scan(&seq)
		if err != nil {
			return fmt.Errorf("store: append task event: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// UnpublishedEvents returns outbox events that have not been acknowledged by
// the WMS, in ascending seq order.
func (s *Store) UnpublishedEvents(ctx context.Context, limit int) ([]*taskpb.TaskStateEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT seq, task_id, order_ref, from_state, to_state,
		       COALESCE(bot_id, ''), reason, emitted_at
		FROM task_events
		WHERE published = false
		ORDER BY seq ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list unpublished events: %w", err)
	}
	defer rows.Close()

	var out []*taskpb.TaskStateEvent
	for rows.Next() {
		var (
			seq                             uint64
			taskID, orderRef, from, to, bot string
			reason                          string
			emittedAt                       time.Time
		)
		if err := rows.Scan(&seq, &taskID, &orderRef, &from, &to, &bot, &reason, &emittedAt); err != nil {
			return nil, fmt.Errorf("store: scan unpublished event: %w", err)
		}
		out = append(out, &taskpb.TaskStateEvent{
			Seq:       seq,
			TaskId:    taskID,
			OrderRef:  orderRef,
			FromState: stateFromName(from),
			ToState:   stateFromName(to),
			BotId:     bot,
			Reason:    reason,
			EmittedAt: timestamppbNew(emittedAt),
		})
	}
	return out, rows.Err()
}

// MarkPublished marks one outbox event as delivered. It is idempotent.
func (s *Store) MarkPublished(ctx context.Context, seq uint64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE task_events SET published = true WHERE seq = $1`, seq)
	if err != nil {
		return fmt.Errorf("store: mark event published: %w", err)
	}
	return nil
}

// CountUnpublished returns how many outbox events are still awaiting delivery.
func (s *Store) CountUnpublished(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_events WHERE published = false`).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count unpublished events: %w", err)
	}
	return n, nil
}

// stateFromName converts a persisted state name to its enum value.
func stateFromName(name string) taskpb.TaskState {
	if v, ok := taskpb.TaskState_value[name]; ok {
		return taskpb.TaskState(v)
	}
	return taskpb.TaskState_TASK_STATE_UNSPECIFIED
}

// ListActiveTasks returns tasks currently assigned to or running on a bot.
func (s *Store) ListActiveTasks(ctx context.Context) ([]*taskpb.Task, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+taskColumns+`
		FROM tasks WHERE state IN ('ASSIGNED', 'RUNNING')
		ORDER BY created_at ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("store: list active tasks: %w", err)
	}
	defer rows.Close()
	var out []*taskpb.Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan active task: %w", err)
		}
		out = append(out, task)
	}
	return out, rows.Err()
}
