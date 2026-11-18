package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/local/warehousetaskbridge/internal/metrics"
	"github.com/local/warehousetaskbridge/internal/store"
	"github.com/local/warehousetaskbridge/internal/taskpb"
	"github.com/local/warehousetaskbridge/internal/translate"
)

type quarantineRecord struct {
	Subject string
	Raw     []byte
	Reason  string
}

type fakeStore struct {
	mu sync.Mutex

	tasksByID  map[string]*taskpb.Task
	idByKey    map[string]string
	hashByKey  map[string]string
	quarantine []quarantineRecord

	insertErr     error
	quarantineErr error
	pingErr       error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		tasksByID: make(map[string]*taskpb.Task),
		idByKey:   make(map[string]string),
		hashByKey: make(map[string]string),
	}
}

func (f *fakeStore) InsertTask(_ context.Context, task *taskpb.Task, hash string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertErr != nil {
		return "", false, f.insertErr
	}
	if id, ok := f.idByKey[task.IdempotencyKey]; ok {
		return id, false, nil
	}
	f.tasksByID[task.Id] = task
	f.idByKey[task.IdempotencyKey] = task.Id
	f.hashByKey[task.IdempotencyKey] = hash
	return task.Id, true, nil
}

func (f *fakeStore) TaskByIdempotencyKey(_ context.Context, key string) (*taskpb.Task, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.idByKey[key]
	if !ok {
		return nil, "", store.ErrTaskNotFound
	}
	return f.tasksByID[id], f.hashByKey[key], nil
}

func (f *fakeStore) InsertQuarantine(_ context.Context, subject string, raw []byte, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.quarantineErr != nil {
		return f.quarantineErr
	}
	f.quarantine = append(f.quarantine, quarantineRecord{Subject: subject, Raw: append([]byte(nil), raw...), Reason: reason})
	return nil
}

func (f *fakeStore) GetTask(_ context.Context, id string) (*taskpb.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.tasksByID[id]
	if !ok {
		return nil, store.ErrTaskNotFound
	}
	return task, nil
}

func (f *fakeStore) ListTasks(_ context.Context, state string, limit int) ([]*taskpb.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*taskpb.Task, 0, len(f.tasksByID))
	for _, task := range f.tasksByID {
		if state == "" || taskpb.TaskState_name[int32(task.State)] == state {
			out = append(out, task)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

func (f *fakeStore) taskCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.tasksByID)
}

func (f *fakeStore) quarantineRecords() []quarantineRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]quarantineRecord(nil), f.quarantine...)
}

func validOrderJSON(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	body := map[string]any{
		"order_ref":       "ORD-1",
		"kind":            "pick",
		"priority_band":   "EXPEDITE",
		"source_location": "A-1-1",
		"dest_location":   "B-2-2",
		"qty":             3,
		"due_by":          "2026-03-01T12:00:00Z",
		"client_meta":     map[string]string{"po": "42"},
		"schema_version":  "2025-01-15",
		"idempotency_key": "idem-1",
	}
	if mutate != nil {
		mutate(body)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal order: %v", err)
	}
	return raw
}

func newTestAcceptor(st Store) (*Acceptor, *metrics.Counters, *translate.ReconciliationReport) {
	counters := metrics.New()
	report := translate.NewReconciliationReport()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewAcceptor(st, counters, report, logger), counters, report
}

func TestAcceptOrderCreatesOnceAndDedupes(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()
	acceptor, counters, report := newTestAcceptor(st)

	var sinkCalls int
	acceptor.SetQuarantineSink(func(string, []byte, string) { sinkCalls++ })

	raw := validOrderJSON(t, nil)
	first, err := acceptor.AcceptOrder(ctx, "test", raw)
	if err != nil {
		t.Fatalf("first accept: %v", err)
	}
	if !first.Created || first.TaskID == "" {
		t.Fatalf("first accept result = %+v, want created", first)
	}
	second, err := acceptor.AcceptOrder(ctx, "test", raw)
	if err != nil {
		t.Fatalf("second accept: %v", err)
	}
	if second.Created {
		t.Fatalf("second accept created a duplicate: %+v", second)
	}
	if second.TaskID != first.TaskID {
		t.Fatalf("duplicate task id %q != original %q", second.TaskID, first.TaskID)
	}
	if got := st.taskCount(); got != 1 {
		t.Fatalf("task rows = %d, want 1", got)
	}
	if got := counters.Get(metrics.OrdersDeduped); got != 1 {
		t.Fatalf("orders_deduped = %d, want 1", got)
	}
	if got := counters.Get(metrics.Quarantined); got != 0 {
		t.Fatalf("quarantined = %d, want 0", got)
	}
	if sinkCalls != 0 {
		t.Fatalf("quarantine sink called %d times for a clean duplicate", sinkCalls)
	}
	// The reconciliation report is populated once per unique accepted order.
	if snap := report.Snapshot(); snap.Total == 0 {
		t.Fatal("expected mapping warnings from the accepted order")
	}
}

func TestAcceptOrderConflictingDuplicate(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()
	acceptor, counters, _ := newTestAcceptor(st)

	first, err := acceptor.AcceptOrder(ctx, "test", validOrderJSON(t, nil))
	if err != nil {
		t.Fatalf("first accept: %v", err)
	}
	// Same idempotency key, different quantity => conflicting duplicate.
	second, err := acceptor.AcceptOrder(ctx, "test", validOrderJSON(t, func(m map[string]any) { m["qty"] = 99 }))
	if err != nil {
		t.Fatalf("conflicting accept should keep the original and not fail: %v", err)
	}
	if second.TaskID != first.TaskID || second.Created {
		t.Fatalf("conflict result = %+v, want original id %q not created", second, first.TaskID)
	}
	if got := st.taskCount(); got != 1 {
		t.Fatalf("task rows = %d, want 1", got)
	}
	records := st.quarantineRecords()
	if len(records) != 1 || !strings.Contains(records[0].Reason, ReasonConflictingDuplicate) {
		t.Fatalf("quarantine records = %+v, want one conflicting duplicate", records)
	}
	if got := counters.Get(metrics.Quarantined); got != 1 {
		t.Fatalf("quarantined = %d, want 1", got)
	}
	if got := counters.Get(metrics.OrdersDeduped); got != 1 {
		t.Fatalf("orders_deduped = %d, want 1", got)
	}
}

func TestAcceptOrderQuarantinesBadInput(t *testing.T) {
	tests := []struct {
		name       string
		raw        func(*testing.T) []byte
		wantReason string
	}{
		{
			name:       "malformed json",
			raw:        func(*testing.T) []byte { return []byte(`{"order_ref":`) },
			wantReason: ReasonMalformedJSON,
		},
		{
			name: "unsupported schema",
			raw: func(t *testing.T) []byte {
				return validOrderJSON(t, func(m map[string]any) { m["schema_version"] = "1999-01-01" })
			},
			wantReason: ReasonUnsupportedSchema,
		},
		{
			name: "unparseable slot",
			raw: func(t *testing.T) []byte {
				return validOrderJSON(t, func(m map[string]any) { m["source_location"] = "not-a-slot" })
			},
			wantReason: ReasonTranslationError,
		},
		{
			name: "invalid quantity",
			raw: func(t *testing.T) []byte {
				return validOrderJSON(t, func(m map[string]any) { m["qty"] = 0 })
			},
			wantReason: ReasonTranslationError,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := newFakeStore()
			acceptor, counters, _ := newTestAcceptor(st)
			var mirrored int
			acceptor.SetQuarantineSink(func(string, []byte, string) { mirrored++ })

			_, err := acceptor.AcceptOrder(ctx, "test.subject", tc.raw(t))
			var rejection *Rejection
			if !errors.As(err, &rejection) {
				t.Fatalf("error = %v, want permanent *Rejection", err)
			}
			if rejection.Reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", rejection.Reason, tc.wantReason)
			}
			records := st.quarantineRecords()
			if len(records) != 1 {
				t.Fatalf("quarantine rows = %d, want 1", len(records))
			}
			if !strings.Contains(records[0].Reason, tc.wantReason) {
				t.Fatalf("quarantine reason = %q, want prefix %q", records[0].Reason, tc.wantReason)
			}
			if records[0].Subject != "test.subject" {
				t.Fatalf("quarantine subject = %q", records[0].Subject)
			}
			if mirrored != 1 {
				t.Fatalf("quarantine mirror calls = %d, want 1", mirrored)
			}
			if got := counters.Get(metrics.Quarantined); got != 1 {
				t.Fatalf("quarantined = %d, want 1", got)
			}
			if got := counters.Get(metrics.OrdersReceived); got != 1 {
				t.Fatalf("orders_received = %d, want 1", got)
			}
			if st.taskCount() != 0 {
				t.Fatal("rejected payload created a task")
			}
		})
	}
}

func TestAcceptOrderTransientStoreFailure(t *testing.T) {
	st := newFakeStore()
	st.insertErr = errors.New("db down")
	acceptor, _, _ := newTestAcceptor(st)

	_, err := acceptor.AcceptOrder(context.Background(), "test", validOrderJSON(t, nil))
	var rejection *Rejection
	if errors.As(err, &rejection) {
		t.Fatalf("db failure was misclassified as permanent: %v", err)
	}
	if err == nil {
		t.Fatal("expected transient error")
	}
	if st.taskCount() != 0 {
		t.Fatal("transient failure created a task")
	}
}

func TestHTTPAPI(t *testing.T) {
	st := newFakeStore()
	acceptor, counters, report := newTestAcceptor(st)
	handler := NewHTTPHandler(HTTPConfig{
		Store: st, Acceptor: acceptor, Report: report, Metrics: counters,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	post := func(t *testing.T, body []byte) *http.Response {
		t.Helper()
		resp, err := http.Post(srv.URL+"/v1/orders", "application/json", strings.NewReader(string(body)))
		if err != nil {
			t.Fatalf("post order: %v", err)
		}
		return resp
	}

	raw := validOrderJSON(t, nil)
	resp := post(t, raw)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("first POST status = %d, want 201", resp.StatusCode)
	}
	var created AcceptResult
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}

	resp2 := post(t, raw)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("duplicate POST status = %d, want 200", resp2.StatusCode)
	}
	var duplicate AcceptResult
	if err := json.NewDecoder(resp2.Body).Decode(&duplicate); err != nil {
		t.Fatalf("decode duplicate: %v", err)
	}
	if duplicate.TaskID != created.TaskID || duplicate.Created {
		t.Fatalf("duplicate response = %+v, want existing id and created=false", duplicate)
	}

	bad := post(t, []byte(`not json`))
	defer bad.Body.Close()
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed POST status = %d, want 400", bad.StatusCode)
	}

	// Counters endpoint reflects both dedupe and quarantine activity.
	metricsResp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	defer metricsResp.Body.Close()
	metricsBody, _ := io.ReadAll(metricsResp.Body)
	for _, want := range []string{"orders_deduped 1", "quarantined 1", "orders_received 3"} {
		if !strings.Contains(string(metricsBody), want) {
			t.Errorf("metrics missing %q:\n%s", want, metricsBody)
		}
	}

	taskResp, err := http.Get(srv.URL + "/v1/tasks/" + created.TaskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	defer taskResp.Body.Close()
	if taskResp.StatusCode != http.StatusOK {
		t.Fatalf("GET task status = %d", taskResp.StatusCode)
	}

	listResp, err := http.Get(srv.URL + "/v1/tasks?state=PENDING")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	defer listResp.Body.Close()
	var list taskList
	if err := json.NewDecoder(listResp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if list.Count != 1 {
		t.Fatalf("listed %d tasks, want 1", list.Count)
	}

	invalidResp, err := http.Get(srv.URL + "/v1/tasks?state=BOGUS")
	if err != nil {
		t.Fatalf("invalid state request: %v", err)
	}
	defer invalidResp.Body.Close()
	if invalidResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid state status = %d, want 400", invalidResp.StatusCode)
	}

	reconResp, err := http.Get(srv.URL + "/v1/reconciliation?format=markdown")
	if err != nil {
		t.Fatalf("get reconciliation: %v", err)
	}
	defer reconResp.Body.Close()
	reconBody, _ := io.ReadAll(reconResp.Body)
	if !strings.Contains(string(reconBody), "DROPPED_CLIENT_META") {
		t.Fatalf("reconciliation missing mapping gap:\n%s", reconBody)
	}

	healthResp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get healthz: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", healthResp.StatusCode)
	}
	st.pingErr = errors.New("db down")
	unhealthy, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get unhealthy: %v", err)
	}
	defer unhealthy.Body.Close()
	if unhealthy.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("degraded health status = %d, want 503", unhealthy.StatusCode)
	}
}

func TestStateFilter(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
		ok   bool
	}{
		{in: "", want: "", ok: true},
		{in: "pending", want: "PENDING", ok: true},
		{in: " DONE ", want: "DONE", ok: true},
		{in: "RUNNING", want: "RUNNING", ok: true},
		{in: "TASK_STATE_UNSPECIFIED", ok: false},
		{in: "BOGUS", ok: false},
	} {
		got, ok := StateFilter(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("StateFilter(%q) = (%q,%v), want (%q,%v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
