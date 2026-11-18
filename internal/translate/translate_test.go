package translate

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/local/warehousetaskbridge/internal/taskpb"
	"github.com/local/warehousetaskbridge/internal/wms"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/golden files from current translation output")

func validOrder() wms.WorkOrder {
	due := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return wms.WorkOrder{
		OrderRef:       "ORD-1001",
		Kind:           "pick",
		PriorityBand:   wms.PriorityExpedite,
		SourceLocation: "G-12-3",
		DestLocation:   "D-4-1",
		Qty:            3,
		DueBy:          &due,
		SchemaVersion:  "2025-01-15",
		IdempotencyKey: "idem-1001",
	}
}

func warningCodes(warnings []Warning) map[string]int {
	out := make(map[string]int)
	for _, w := range warnings {
		out[w.Code]++
	}
	return out
}

func TestToInternalMapping(t *testing.T) {
	tests := []struct {
		name         string
		mutate       func(*wms.WorkOrder)
		wantType     taskpb.TaskType
		wantPriority int32
		wantZone     string
		wantWarning  string
		wantErr      bool
	}{
		{name: "pick default", mutate: func(*wms.WorkOrder) {}, wantType: taskpb.TaskType_PICK, wantPriority: 10, wantZone: "RESERVE"},
		{name: "pickup alias", mutate: func(w *wms.WorkOrder) { w.Kind = "Pickup" }, wantType: taskpb.TaskType_PICK, wantPriority: 10, wantZone: "RESERVE"},
		{name: "putaway uses destination zone", mutate: func(w *wms.WorkOrder) { w.Kind = "putaway"; w.DestLocation = "N-2-4" }, wantType: taskpb.TaskType_PUT, wantPriority: 10, wantZone: "BULK"},
		{name: "stow alias", mutate: func(w *wms.WorkOrder) { w.Kind = "STOW"; w.DestLocation = "B-2-4" }, wantType: taskpb.TaskType_PUT, wantPriority: 10, wantZone: "FORWARD"},
		{name: "relocate", mutate: func(w *wms.WorkOrder) { w.Kind = "relocate" }, wantType: taskpb.TaskType_MOVE, wantPriority: 10, wantZone: "RESERVE"},
		{name: "move alias", mutate: func(w *wms.WorkOrder) { w.Kind = "MOVE" }, wantType: taskpb.TaskType_MOVE, wantPriority: 10, wantZone: "RESERVE"},
		{name: "unknown kind accepted", mutate: func(w *wms.WorkOrder) { w.Kind = "kitting" }, wantType: taskpb.TaskType_UNKNOWN, wantPriority: 10, wantZone: "RESERVE", wantWarning: CodeUnmappedKind},
		{name: "standard band", mutate: func(w *wms.WorkOrder) { w.PriorityBand = wms.PriorityStandard }, wantType: taskpb.TaskType_PICK, wantPriority: 50, wantZone: "RESERVE"},
		{name: "bulk band", mutate: func(w *wms.WorkOrder) { w.PriorityBand = wms.PriorityBulk }, wantType: taskpb.TaskType_PICK, wantPriority: 90, wantZone: "RESERVE"},
		{name: "unknown band defaults standard", mutate: func(w *wms.WorkOrder) { w.PriorityBand = "TURBO" }, wantType: taskpb.TaskType_PICK, wantPriority: 50, wantZone: "RESERVE", wantWarning: CodeUnknownPriorityBand},
		{name: "forward zone", mutate: func(w *wms.WorkOrder) { w.SourceLocation = "C-1-1" }, wantType: taskpb.TaskType_PICK, wantPriority: 10, wantZone: "FORWARD"},
		{name: "overflow zone", mutate: func(w *wms.WorkOrder) { w.SourceLocation = "T-1-1" }, wantType: taskpb.TaskType_PICK, wantPriority: 10, wantZone: "OVERFLOW"},
		{name: "bad source slot", mutate: func(w *wms.WorkOrder) { w.SourceLocation = "nope" }, wantErr: true, wantWarning: CodeUnparseableSlot},
		{name: "bad dest slot", mutate: func(w *wms.WorkOrder) { w.DestLocation = "A-0-1" }, wantErr: true, wantWarning: CodeUnparseableSlot},
		{name: "zero quantity", mutate: func(w *wms.WorkOrder) { w.Qty = 0 }, wantErr: true, wantWarning: CodeInvalidQuantity},
		{name: "negative quantity", mutate: func(w *wms.WorkOrder) { w.Qty = -5 }, wantErr: true, wantWarning: CodeInvalidQuantity},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wo := validOrder()
			tc.mutate(&wo)
			task, warnings, err := ToInternal(wo)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got task %+v", task)
				}
				if tc.wantWarning != "" && warningCodes(warnings)[tc.wantWarning] == 0 {
					t.Fatalf("expected warning %s, got %+v", tc.wantWarning, warnings)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if task.TaskType != tc.wantType {
				t.Errorf("task type = %v, want %v", task.TaskType, tc.wantType)
			}
			if task.Priority != tc.wantPriority {
				t.Errorf("priority = %d, want %d", task.Priority, tc.wantPriority)
			}
			if task.Zone != tc.wantZone {
				t.Errorf("zone = %q, want %q", task.Zone, tc.wantZone)
			}
			if task.State != taskpb.TaskState_PENDING {
				t.Errorf("state = %v, want PENDING", task.State)
			}
			if task.IdempotencyKey != wo.IdempotencyKey || task.OrderRef != wo.OrderRef {
				t.Errorf("identity fields not preserved: %+v", task)
			}
			if tc.wantWarning != "" && warningCodes(warnings)[tc.wantWarning] == 0 {
				t.Errorf("expected warning %s, got %+v", tc.wantWarning, warnings)
			}
		})
	}
}

func TestToInternalWarnings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*wms.WorkOrder)
		code   string
	}{
		{name: "unmapped kind", mutate: func(w *wms.WorkOrder) { w.Kind = "cycle-count" }, code: CodeUnmappedKind},
		{name: "unknown priority", mutate: func(w *wms.WorkOrder) { w.PriorityBand = "NOW" }, code: CodeUnknownPriorityBand},
		{name: "dropped client meta", mutate: func(w *wms.WorkOrder) { w.ClientMeta = map[string]string{"po": "42"} }, code: CodeDroppedClientMeta},
		{name: "missing due by", mutate: func(w *wms.WorkOrder) { w.DueBy = nil }, code: CodeMissingDueBy},
		{name: "unknown schema", mutate: func(w *wms.WorkOrder) { w.SchemaVersion = "1999-01-01" }, code: CodeUnknownSchemaVersion},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wo := validOrder()
			tc.mutate(&wo)
			_, warnings, err := ToInternal(wo)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if warningCodes(warnings)[tc.code] == 0 {
				t.Fatalf("expected warning %s, got %+v", tc.code, warnings)
			}
		})
	}
}

func TestParseSlot(t *testing.T) {
	tests := []struct {
		raw    string
		want   Slot
		wantOK bool
	}{
		{raw: "A-12-3", want: Slot{Aisle: 1, Bay: 12, Level: 3, Zone: "FORWARD"}, wantOK: true},
		{raw: " f-2-9 ", want: Slot{Aisle: 6, Bay: 2, Level: 9, Zone: "FORWARD"}, wantOK: true},
		{raw: "g-1-1", want: Slot{Aisle: 7, Bay: 1, Level: 1, Zone: "RESERVE"}, wantOK: true},
		{raw: "M-7-2", want: Slot{Aisle: 13, Bay: 7, Level: 2, Zone: "BULK"}, wantOK: true},
		{raw: "Z-99-99", want: Slot{Aisle: 26, Bay: 99, Level: 99, Zone: "OVERFLOW"}, wantOK: true},
		{raw: "AA-1-1", want: Slot{Aisle: 27, Bay: 1, Level: 1, Zone: "OVERFLOW"}, wantOK: true},
		{raw: "", wantOK: false},
		{raw: "A-1", wantOK: false},
		{raw: "A-1-1-1", wantOK: false},
		{raw: "1-2-3", wantOK: false},
		{raw: "A-x-3", wantOK: false},
		{raw: "A-1-0", wantOK: false},
		{raw: "A--1-1", wantOK: false},
		{raw: "ABCD-1-1", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := ParseSlot(tc.raw)
			if !tc.wantOK {
				if err == nil {
					t.Fatalf("expected error for %q, got %+v", tc.raw, got)
				}
				if !errors.Is(err, ErrUnparseableSlot) {
					t.Fatalf("error %v does not wrap ErrUnparseableSlot", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestToExternalStateMapping(t *testing.T) {
	tests := []struct {
		state        taskpb.TaskState
		want         string
		wantCollapse bool
	}{
		{state: taskpb.TaskState_PENDING, want: wms.ExternalStateReceived},
		{state: taskpb.TaskState_ASSIGNED, want: wms.ExternalStateInProgress, wantCollapse: true},
		{state: taskpb.TaskState_RUNNING, want: wms.ExternalStateInProgress, wantCollapse: true},
		{state: taskpb.TaskState_DONE, want: wms.ExternalStateComplete},
		{state: taskpb.TaskState_FAILED, want: wms.ExternalStateRejected},
	}
	for _, tc := range tests {
		ev := &taskpb.TaskStateEvent{
			TaskId:    "task-1",
			OrderRef:  "ORD-1",
			FromState: taskpb.TaskState_PENDING,
			ToState:   tc.state,
			Seq:       7,
			EmittedAt: timestamppb.New(time.Unix(1700000000, 0)),
		}
		got, warnings := ToExternal(ev)
		if got.ExternalState != tc.want {
			t.Errorf("state %v -> %q, want %q", tc.state, got.ExternalState, tc.want)
		}
		if got.OrderRef != "ORD-1" || got.StateSeq != 7 {
			t.Errorf("identity fields lost: %+v", got)
		}
		hasCollapse := warningCodes(warnings)[CodeLossyStateCollapse] > 0
		if hasCollapse != tc.wantCollapse {
			t.Errorf("state %v collapse warning = %v, want %v (%+v)", tc.state, hasCollapse, tc.wantCollapse, warnings)
		}
	}
}

func TestToExternalNilEventDoesNotPanic(t *testing.T) {
	got, warnings := ToExternal(nil)
	if got.OrderRef != "" || len(warnings) == 0 {
		t.Fatalf("unexpected nil-event result: %+v %+v", got, warnings)
	}
}

func TestRoundTripPreservesOrderRef(t *testing.T) {
	for _, band := range []string{wms.PriorityExpedite, wms.PriorityStandard, wms.PriorityBulk, "UNKNOWN"} {
		for _, kind := range []string{"pick", "putaway", "relocate", "mystery"} {
			wo := validOrder()
			wo.PriorityBand = band
			wo.Kind = kind
			task, _, err := ToInternal(wo)
			if err != nil {
				t.Fatalf("ToInternal(%s/%s): %v", band, kind, err)
			}
			ev := &taskpb.TaskStateEvent{
				TaskId:    task.Id,
				OrderRef:  task.OrderRef,
				FromState: taskpb.TaskState_PENDING,
				ToState:   taskpb.TaskState_RUNNING,
				Seq:       1,
				EmittedAt: timestamppb.New(time.Now()),
			}
			update, _ := ToExternal(ev)
			if update.OrderRef != wo.OrderRef {
				t.Fatalf("order_ref not preserved: %q != %q", update.OrderRef, wo.OrderRef)
			}
		}
	}
}

func FuzzParseSlot(f *testing.F) {
	for _, seed := range []string{"A-12-3", "", "-", "A-1-1", "ZZ-999-999", "A-1", "a-0-b", "AA-2-3"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		slot, err := ParseSlot(raw)
		if err != nil {
			if !errors.Is(err, ErrUnparseableSlot) {
				t.Fatalf("unexpected error class: %v", err)
			}
			return
		}
		if slot.Aisle <= 0 || slot.Bay <= 0 || slot.Level <= 0 {
			t.Fatalf("successful parse produced non-positive coordinates: %+v", slot)
		}
		if slot.Zone == "" {
			t.Fatalf("successful parse produced empty zone: %+v", slot)
		}
		// A parsed slot must round-trip through its canonical string form.
		again, err := ParseSlot(slot.String())
		if err != nil {
			t.Fatalf("canonical form %q did not parse: %v", slot.String(), err)
		}
		if again != slot {
			t.Fatalf("canonical round-trip mismatch: %+v != %+v", again, slot)
		}
	})
}

func TestReconciliationReport(t *testing.T) {
	report := NewReconciliationReport()
	report.Add(
		Warning{Code: CodeUnmappedKind, Severity: SeverityWarning, Detail: "kind x"},
		Warning{Code: CodeUnmappedKind, Severity: SeverityWarning, Detail: "kind y"},
		Warning{Code: CodeMissingDueBy, Severity: SeverityInfo, Detail: "no due date"},
	)
	snap := report.Snapshot()
	if snap.Total != 3 {
		t.Fatalf("total = %d, want 3", snap.Total)
	}
	if len(snap.Warnings) != 2 {
		t.Fatalf("entries = %d, want 2", len(snap.Warnings))
	}
	if snap.Warnings[0].Code != CodeMissingDueBy || snap.Warnings[0].Count != 1 {
		t.Fatalf("unexpected first entry: %+v", snap.Warnings[0])
	}
	if snap.Warnings[1].Code != CodeUnmappedKind || snap.Warnings[1].Count != 2 {
		t.Fatalf("unexpected second entry: %+v", snap.Warnings[1])
	}
	md := report.Markdown()
	if !strings.Contains(md, CodeUnmappedKind) || !strings.Contains(md, "**3**") {
		t.Fatalf("markdown missing aggregate: %s", md)
	}
	if !strings.Contains(NewReconciliationReport().Markdown(), "none") {
		t.Fatal("empty report markdown should say none")
	}
}

func TestReconciliationReportSampleCorpus(t *testing.T) {
	report := NewReconciliationReport()
	entries, err := os.ReadDir("testdata/orders")
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("sample corpus is empty")
	}
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join("testdata/orders", entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		var wo wms.WorkOrder
		if err := json.Unmarshal(raw, &wo); err != nil {
			t.Fatalf("decode %s: %v", entry.Name(), err)
		}
		_, warnings, err := ToInternal(wo)
		if err != nil {
			t.Fatalf("translate %s: %v", entry.Name(), err)
		}
		report.Add(warnings...)
	}
	snap := report.Snapshot()
	if snap.Total == 0 {
		t.Fatal("reconciliation report has no warning counts for the sample corpus")
	}
	for _, required := range []string{CodeUnmappedKind, CodeDroppedClientMeta, CodeMissingDueBy, CodeUnknownPriorityBand} {
		found := false
		for _, e := range snap.Warnings {
			if e.Code == required && e.Count > 0 {
				found = true
			}
		}
		if !found {
			t.Errorf("sample corpus did not exercise %s; report=%+v", required, snap)
		}
	}
	if !strings.Contains(report.Markdown(), "Total") {
		t.Fatal("markdown report is not renderable")
	}
	t.Logf("sample corpus reconciliation:\n%s", report.Markdown())
}

// normalizeTask pins nondeterministic fields so golden files are stable.
func normalizeTask(task *taskpb.Task) {
	task.Id = "00000000-0000-4000-8000-000000000000"
	fixed := timestamppb.New(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	task.CreatedAt = fixed
	task.UpdatedAt = fixed
}

func TestGoldenTranslations(t *testing.T) {
	orderDir := "testdata/orders"
	goldenDir := "testdata/golden"
	entries, err := os.ReadDir(orderDir)
	if err != nil {
		t.Fatalf("read orders: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no golden input orders")
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(orderDir, entry.Name()))
			if err != nil {
				t.Fatalf("read order: %v", err)
			}
			var wo wms.WorkOrder
			if err := json.Unmarshal(raw, &wo); err != nil {
				t.Fatalf("decode order: %v", err)
			}
			task, _, err := ToInternal(wo)
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			normalizeTask(task)
			got, err := protojson.MarshalOptions{Indent: "  ", EmitUnpopulated: false}.Marshal(task)
			if err != nil {
				t.Fatalf("marshal task: %v", err)
			}
			got = append(got, '\n')
			goldenPath := filepath.Join(goldenDir, strings.TrimSuffix(entry.Name(), ".json")+".json")

			if *updateGolden {
				if err := os.MkdirAll(goldenDir, 0o755); err != nil {
					t.Fatalf("mkdir golden: %v", err)
				}
				if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				return
			}

			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("read golden (run go test -run TestGoldenTranslations -update): %v", err)
			}
			var wantTask taskpb.Task
			if err := protojson.Unmarshal(want, &wantTask); err != nil {
				t.Fatalf("decode golden: %v", err)
			}
			var gotTask taskpb.Task
			if err := protojson.Unmarshal(got, &gotTask); err != nil {
				t.Fatalf("decode actual: %v", err)
			}
			if !proto.Equal(&gotTask, &wantTask) {
				t.Fatalf("golden mismatch for %s\n--- got ---\n%s\n--- want ---\n%s", entry.Name(), got, want)
			}
		})
	}
}
