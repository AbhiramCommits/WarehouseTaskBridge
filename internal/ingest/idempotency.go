package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/local/warehousetaskbridge/internal/metrics"
	"github.com/local/warehousetaskbridge/internal/taskpb"
	"github.com/local/warehousetaskbridge/internal/translate"
	"github.com/local/warehousetaskbridge/internal/wms"
)

// Quarantine reasons recorded with every rejected payload.
const (
	ReasonMalformedJSON        = "MALFORMED_JSON"
	ReasonUnsupportedSchema    = "UNSUPPORTED_SCHEMA_VERSION"
	ReasonTranslationError     = "TRANSLATION_ERROR"
	ReasonConflictingDuplicate = "CONFLICTING_DUPLICATE"
)

// CodeConflictingDuplicate is surfaced alongside the existing task id when a
// repeat idempotency key carries a different payload.
const CodeConflictingDuplicate = "CONFLICTING_DUPLICATE"

// Store is the persistence surface required by ingest.
type Store interface {
	InsertTask(ctx context.Context, task *taskpb.Task, payloadHash string) (string, bool, error)
	TaskByIdempotencyKey(ctx context.Context, key string) (*taskpb.Task, string, error)
	InsertQuarantine(ctx context.Context, subject string, raw []byte, reason string) error
	GetTask(ctx context.Context, id string) (*taskpb.Task, error)
	ListTasks(ctx context.Context, state string, limit int) ([]*taskpb.Task, error)
	Ping(ctx context.Context) error
}

// Rejection is a permanent, caller-correctable input error. Transports ack and
// discard these instead of retrying them forever.
type Rejection struct {
	Reason string
	Status int
	Err    error
}

// Error implements error.
func (r *Rejection) Error() string {
	if r.Err == nil {
		return "ingest: rejected (" + r.Reason + ")"
	}
	return fmt.Sprintf("ingest: rejected (%s): %v", r.Reason, r.Err)
}

// Unwrap exposes the underlying cause.
func (r *Rejection) Unwrap() error { return r.Err }

// AcceptResult is the outcome of accepting one order.
type AcceptResult struct {
	TaskID   string              `json:"task_id"`
	OrderRef string              `json:"order_ref"`
	Created  bool                `json:"created"`
	Warnings []translate.Warning `json:"warnings,omitempty"`
}

// Acceptor validates, translates, and persists external work orders with
// idempotent semantics.
type Acceptor struct {
	store          Store
	metrics        *metrics.Counters
	report         *translate.ReconciliationReport
	logger         *slog.Logger
	quarantineSink func(subject string, raw []byte, reason string)
}

// NewAcceptor builds an Acceptor.
func NewAcceptor(st Store, counters *metrics.Counters, report *translate.ReconciliationReport, logger *slog.Logger) *Acceptor {
	if logger == nil {
		logger = slog.Default()
	}
	if report == nil {
		report = translate.NewReconciliationReport()
	}
	return &Acceptor{store: st, metrics: counters, report: report, logger: logger}
}

// SetQuarantineSink installs a side channel that receives every quarantined
// payload, used to mirror quarantine entries onto wms.quarantine.
func (a *Acceptor) SetQuarantineSink(sink func(subject string, raw []byte, reason string)) {
	a.quarantineSink = sink
}

// AcceptOrder decodes, validates, translates, and persists one external order.
// Duplicated idempotency keys return the original task id with created=false.
// Permanent rejections are quarantined before returning a *Rejection.
func (a *Acceptor) AcceptOrder(ctx context.Context, subject string, raw []byte) (AcceptResult, error) {
	a.metrics.Inc(metrics.OrdersReceived)

	var wo wms.WorkOrder
	if err := json.Unmarshal(raw, &wo); err != nil {
		return AcceptResult{}, a.quarantine(ctx, subject, raw, ReasonMalformedJSON,
			fmt.Sprintf("%s: %v", ReasonMalformedJSON, err), httpStatusBadRequest)
	}

	if !wms.SupportedSchemaVersions[wo.SchemaVersion] {
		return AcceptResult{}, a.quarantine(ctx, subject, raw, ReasonUnsupportedSchema,
			fmt.Sprintf("%s: %q", ReasonUnsupportedSchema, wo.SchemaVersion), httpStatusUnprocessable)
	}

	task, warnings, err := translate.ToInternal(wo)
	if err != nil {
		a.report.Add(warnings...)
		return AcceptResult{}, a.quarantine(ctx, subject, raw, ReasonTranslationError,
			fmt.Sprintf("%s: %v", ReasonTranslationError, err), httpStatusUnprocessable)
	}

	hash := translate.PayloadHash(wo)
	taskID, created, err := a.store.InsertTask(ctx, task, hash)
	if err != nil {
		return AcceptResult{}, err
	}

	if created {
		a.report.Add(warnings...)
		return AcceptResult{TaskID: taskID, OrderRef: task.OrderRef, Created: true, Warnings: warnings}, nil
	}

	a.metrics.Inc(metrics.OrdersDeduped)
	existing, existingHash, err := a.store.TaskByIdempotencyKey(ctx, wo.IdempotencyKey)
	if err != nil {
		return AcceptResult{}, err
	}
	if existingHash != hash {
		reason := fmt.Sprintf("%s: key %q replayed with a different payload; original task %s kept",
			ReasonConflictingDuplicate, wo.IdempotencyKey, existing.Id)
		if qErr := a.recordQuarantine(ctx, subject, raw, reason); qErr != nil {
			return AcceptResult{}, qErr
		}
		warnings = append(warnings, translate.Warning{
			Code:     CodeConflictingDuplicate,
			Field:    "idempotency_key",
			Detail:   reason,
			Severity: translate.SeverityWarning,
		})
		a.report.Add(warnings...)
		a.logger.Warn("conflicting duplicate quarantined", "subject", subject, "idempotency_key", wo.IdempotencyKey)
	}
	return AcceptResult{TaskID: existing.Id, OrderRef: existing.OrderRef, Created: false, Warnings: warnings}, nil
}

const (
	httpStatusBadRequest    = 400
	httpStatusUnprocessable = 422
)

// quarantine persists and mirrors a rejected payload. A failure to persist is
// transient and must be retried by the transport.
func (a *Acceptor) quarantine(ctx context.Context, subject string, raw []byte, reasonCode, detail string, status int) error {
	if err := a.recordQuarantine(ctx, subject, raw, detail); err != nil {
		return err
	}
	return &Rejection{Reason: reasonCode, Status: status, Err: errors.New(detail)}
}

func (a *Acceptor) recordQuarantine(ctx context.Context, subject string, raw []byte, reason string) error {
	if err := a.store.InsertQuarantine(ctx, subject, raw, reason); err != nil {
		return fmt.Errorf("ingest: persist quarantine: %w", err)
	}
	a.metrics.Inc(metrics.Quarantined)
	if a.quarantineSink != nil {
		a.quarantineSink(subject, raw, reason)
	}
	a.logger.Warn("payload quarantined", "subject", subject, "reason", reason)
	return nil
}

// StateFilter normalizes and validates an internal task state filter. An empty
// filter means "all states".
func StateFilter(raw string) (string, bool) {
	state := strings.ToUpper(strings.TrimSpace(raw))
	if state == "" {
		return "", true
	}
	if _, ok := taskpb.TaskState_value[state]; ok && state != "TASK_STATE_UNSPECIFIED" {
		return state, true
	}
	return "", false
}
