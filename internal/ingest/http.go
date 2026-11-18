package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/local/warehousetaskbridge/internal/metrics"
	"github.com/local/warehousetaskbridge/internal/store"
	"github.com/local/warehousetaskbridge/internal/translate"
)

// MaxOrderBytes caps an accepted external order body.
const MaxOrderBytes = 1 << 20

// HTTPConfig configures the REST API.
type HTTPConfig struct {
	Store    Store
	Acceptor *Acceptor
	Report   *translate.ReconciliationReport
	Metrics  *metrics.Counters
	Logger   *slog.Logger
}

// NewHTTPHandler builds the REST API handler.
func NewHTTPHandler(cfg HTTPConfig) http.Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Report == nil {
		cfg.Report = translate.NewReconciliationReport()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.New()
	}
	api := &api{
		store:    cfg.Store,
		acceptor: cfg.Acceptor,
		report:   cfg.Report,
		metrics:  cfg.Metrics,
		logger:   cfg.Logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", api.postOrder)
	mux.HandleFunc("GET /v1/tasks/{id}", api.getTask)
	mux.HandleFunc("GET /v1/tasks", api.listTasks)
	mux.HandleFunc("GET /v1/reconciliation", api.reconciliation)
	mux.HandleFunc("GET /healthz", api.health)
	mux.HandleFunc("GET /metrics", api.metricsText)
	return mux
}

type api struct {
	store    Store
	acceptor *Acceptor
	report   *translate.ReconciliationReport
	metrics  *metrics.Counters
	logger   *slog.Logger
}

func (a *api) postOrder(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxOrderBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "read_body", err)
		return
	}
	result, err := a.acceptor.AcceptOrder(r.Context(), "rest.orders", body)
	if err != nil {
		var rejection *Rejection
		if errors.As(err, &rejection) {
			writeError(w, rejection.Status, rejection.Reason, rejection.Err)
			return
		}
		a.logger.Error("accept order failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(w, a.logger, status, result)
}

func (a *api) getTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing_task_id", errors.New("task id is required"))
		return
	}
	task, err := a.store.GetTask(r.Context(), id)
	if errors.Is(err, store.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task_not_found", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", err)
		return
	}
	writeProto(w, a.logger, http.StatusOK, task)
}

type taskList struct {
	Count int               `json:"count"`
	State string            `json:"state,omitempty"`
	Tasks []json.RawMessage `json:"tasks"`
}

func (a *api) listTasks(w http.ResponseWriter, r *http.Request) {
	state, ok := StateFilter(r.URL.Query().Get("state"))
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_state",
			fmt.Errorf("state must be one of PENDING, ASSIGNED, RUNNING, DONE, FAILED"))
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_limit", fmt.Errorf("limit must be a positive integer"))
			return
		}
		limit = n
	}
	tasks, err := a.store.ListTasks(r.Context(), state, limit)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", err)
		return
	}
	out := taskList{Count: len(tasks), State: state, Tasks: make([]json.RawMessage, 0, len(tasks))}
	for _, task := range tasks {
		raw, err := protojson.Marshal(task)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "marshal_task", err)
			return
		}
		out.Tasks = append(out.Tasks, raw)
	}
	writeJSON(w, a.logger, http.StatusOK, out)
}

func (a *api) reconciliation(w http.ResponseWriter, r *http.Request) {
	if strings.EqualFold(r.URL.Query().Get("format"), "markdown") {
		w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, a.report.Markdown())
		return
	}
	writeJSON(w, a.logger, http.StatusOK, a.report.Snapshot())
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.store.Ping(ctx); err != nil {
		writeJSON(w, a.logger, http.StatusServiceUnavailable, map[string]string{
			"status": "degraded", "database": "unavailable",
		})
		return
	}
	writeJSON(w, a.logger, http.StatusOK, map[string]string{
		"status": "ok", "database": "ok",
	})
}

func (a *api) metricsText(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, a.metrics.Render())
}

func writeJSON(w http.ResponseWriter, logger *slog.Logger, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logger.Error("write json response", "error", err)
	}
}

func writeProto(w http.ResponseWriter, logger *slog.Logger, status int, msg proto.Message) {
	raw, err := protojson.Marshal(msg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "marshal_task", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(raw); err != nil {
		logger.Error("write protobuf json response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, code string, err error) {
	detail := ""
	if err != nil {
		detail = err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "detail": detail})
}
