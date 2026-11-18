// Package translate maps the external WMS schema onto the internal task model
// and back. The two schemas intentionally differ; every lossy or unmappable
// field is reported as a Warning so integration gaps are visible instead of
// silently dropped.
package translate

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/local/warehousetaskbridge/internal/taskpb"
	"github.com/local/warehousetaskbridge/internal/wms"
)

// Warning codes emitted by the translation layer.
const (
	CodeUnmappedKind         = "UNMAPPED_KIND"
	CodeUnknownPriorityBand  = "UNKNOWN_PRIORITY_BAND"
	CodeLossyStateCollapse   = "LOSSY_STATE_COLLAPSE"
	CodeDroppedClientMeta    = "DROPPED_CLIENT_META"
	CodeMissingDueBy         = "MISSING_DUE_BY"
	CodeUnparseableSlot      = "UNPARSEABLE_SLOT"
	CodeUnknownSchemaVersion = "UNKNOWN_SCHEMA_VERSION"
	CodeInvalidQuantity      = "INVALID_QUANTITY"
	CodeLengthyDetail        = "TRUNCATED_DETAIL"
)

// Warning severities.
const (
	SeverityInfo    = "INFO"
	SeverityWarning = "WARNING"
	SeverityError   = "ERROR"
)

// Warning describes one field-level mapping gap.
type Warning struct {
	Code     string `json:"code"`
	Field    string `json:"field,omitempty"`
	Detail   string `json:"detail"`
	Severity string `json:"severity"`
}

// ErrUnparseableSlot is returned when a location cannot be interpreted.
var ErrUnparseableSlot = errors.New("translate: unparseable slot")

// ErrInvalidQuantity is returned when the external quantity is not positive.
var ErrInvalidQuantity = errors.New("translate: invalid quantity")

// priorityByBand maps external priority bands to numeric internal priority.
// Lower numbers are more urgent.
var priorityByBand = map[string]int32{
	wms.PriorityExpedite: 10,
	wms.PriorityStandard: 50,
	wms.PriorityBulk:     90,
}

// kindByExternal maps the external free-text kind to the internal task type.
var kindByExternal = map[string]taskpb.TaskType{
	"pick":     taskpb.TaskType_PICK,
	"pickup":   taskpb.TaskType_PICK,
	"putaway":  taskpb.TaskType_PUT,
	"stow":     taskpb.TaskType_PUT,
	"relocate": taskpb.TaskType_MOVE,
	"move":     taskpb.TaskType_MOVE,
}

// Slot is a parsed aisle-bay-level address plus its derived zone.
type Slot struct {
	Aisle int
	Bay   int
	Level int
	Zone  string
}

// String renders the slot in the external A-12-3 form, including multi-letter
// aisles such as AA.
func (s Slot) String() string {
	return fmt.Sprintf("%s-%d-%d", encodeAisle(s.Aisle), s.Bay, s.Level)
}

// encodeAisle converts a 1-based aisle number to bijective base-26 letters.
func encodeAisle(aisle int) string {
	if aisle <= 0 {
		return ""
	}
	var b strings.Builder
	for aisle > 0 {
		aisle--
		b.WriteByte(byte('A' + aisle%26))
		aisle /= 26
	}
	out := []byte(b.String())
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

// zoneForAisle derives the internal zone from the aisle number.
// Aisles A-F (1-6) are FORWARD, G-L (7-12) RESERVE, M-R (13-18) BULK,
// and everything above is OVERFLOW.
func zoneForAisle(aisle int) string {
	switch {
	case aisle >= 1 && aisle <= 6:
		return "FORWARD"
	case aisle <= 12:
		return "RESERVE"
	case aisle <= 18:
		return "BULK"
	default:
		return "OVERFLOW"
	}
}

// ParseSlot parses the external aisle-bay-level location format, for example
// "A-12-3". Aisles are letters, bay and level are positive integers. It never
// panics; every malformed input returns ErrUnparseableSlot.
func ParseSlot(raw string) (Slot, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return Slot{}, fmt.Errorf("%w: empty location", ErrUnparseableSlot)
	}
	parts := strings.Split(s, "-")
	if len(parts) != 3 {
		return Slot{}, fmt.Errorf("%w: %q must be AISLE-BAY-LEVEL", ErrUnparseableSlot, raw)
	}
	letters := strings.ToUpper(strings.TrimSpace(parts[0]))
	if letters == "" || len(letters) > 3 {
		return Slot{}, fmt.Errorf("%w: %q has an invalid aisle", ErrUnparseableSlot, raw)
	}
	aisle := 0
	for _, r := range letters {
		if r < 'A' || r > 'Z' {
			return Slot{}, fmt.Errorf("%w: %q has a non-letter aisle", ErrUnparseableSlot, raw)
		}
		aisle = aisle*26 + int(r-'A'+1)
	}
	bay, err := parsePositiveInt(parts[1])
	if err != nil {
		return Slot{}, fmt.Errorf("%w: %q has an invalid bay: %v", ErrUnparseableSlot, raw, err)
	}
	level, err := parsePositiveInt(parts[2])
	if err != nil {
		return Slot{}, fmt.Errorf("%w: %q has an invalid level: %v", ErrUnparseableSlot, raw, err)
	}
	return Slot{Aisle: aisle, Bay: bay, Level: level, Zone: zoneForAisle(aisle)}, nil
}

func parsePositiveInt(raw string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("not an integer")
	}
	if n <= 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return n, nil
}

// payloadHash returns a stable hash input for idempotency conflict detection.
// Callers hash the returned bytes.
func payloadHash(wo wms.WorkOrder) string {
	b, err := json.Marshal(wo)
	if err != nil {
		return fmt.Sprintf("%#v", wo)
	}
	return string(b)
}

// PayloadHash exposes the canonical external payload representation used to
// distinguish a clean duplicate from a conflicting duplicate.
func PayloadHash(wo wms.WorkOrder) string {
	return payloadHash(wo)
}

// ToInternal translates an external work order into a PENDING internal task.
// Unmappable fields produce warnings and are preserved as far as the internal
// model allows; an unparseable location or non-positive quantity is a hard
// error because accepting it would create unroutable work.
func ToInternal(wo wms.WorkOrder) (*taskpb.Task, []Warning, error) {
	var warnings []Warning

	if strings.TrimSpace(wo.OrderRef) == "" {
		return nil, nil, fmt.Errorf("translate: order_ref is required")
	}
	if strings.TrimSpace(wo.IdempotencyKey) == "" {
		return nil, nil, fmt.Errorf("translate: idempotency_key is required")
	}
	if wo.Qty <= 0 {
		return nil, append(warnings, Warning{
			Code:     CodeInvalidQuantity,
			Field:    "qty",
			Detail:   fmt.Sprintf("quantity %d is not positive", wo.Qty),
			Severity: SeverityError,
		}), fmt.Errorf("%w: %d", ErrInvalidQuantity, wo.Qty)
	}

	if !wms.SupportedSchemaVersions[wo.SchemaVersion] {
		warnings = append(warnings, Warning{
			Code:     CodeUnknownSchemaVersion,
			Field:    "schema_version",
			Detail:   fmt.Sprintf("schema version %q is not in the supported set; translating best-effort", wo.SchemaVersion),
			Severity: SeverityWarning,
		})
	}

	taskType, kindOK := kindByExternal[strings.ToLower(strings.TrimSpace(wo.Kind))]
	if !kindOK {
		taskType = taskpb.TaskType_UNKNOWN
		warnings = append(warnings, Warning{
			Code:     CodeUnmappedKind,
			Field:    "kind",
			Detail:   fmt.Sprintf("kind %q has no internal mapping; accepted as UNKNOWN", wo.Kind),
			Severity: SeverityWarning,
		})
	}

	priority, bandOK := priorityByBand[strings.ToUpper(strings.TrimSpace(wo.PriorityBand))]
	if !bandOK {
		priority = priorityByBand[wms.PriorityStandard]
		warnings = append(warnings, Warning{
			Code:     CodeUnknownPriorityBand,
			Field:    "priority_band",
			Detail:   fmt.Sprintf("priority band %q is unknown; defaulted to STANDARD (50)", wo.PriorityBand),
			Severity: SeverityWarning,
		})
	}

	source, err := ParseSlot(wo.SourceLocation)
	if err != nil {
		warnings = append(warnings, Warning{
			Code:     CodeUnparseableSlot,
			Field:    "source_location",
			Detail:   err.Error(),
			Severity: SeverityError,
		})
		return nil, warnings, fmt.Errorf("translate: source_location: %w", err)
	}
	dest, err := ParseSlot(wo.DestLocation)
	if err != nil {
		warnings = append(warnings, Warning{
			Code:     CodeUnparseableSlot,
			Field:    "dest_location",
			Detail:   err.Error(),
			Severity: SeverityError,
		})
		return nil, warnings, fmt.Errorf("translate: dest_location: %w", err)
	}

	// Zone affinity follows the work: picks and moves happen in the source
	// zone, while putaway happens in the destination zone.
	zone := source.Zone
	if taskType == taskpb.TaskType_PUT {
		zone = dest.Zone
	}

	if wo.DueBy == nil {
		warnings = append(warnings, Warning{
			Code:     CodeMissingDueBy,
			Field:    "due_by",
			Detail:   "no due_by supplied; task is scheduled purely by priority band",
			Severity: SeverityInfo,
		})
	}
	if len(wo.ClientMeta) > 0 {
		warnings = append(warnings, Warning{
			Code:     CodeDroppedClientMeta,
			Field:    "client_meta",
			Detail:   fmt.Sprintf("%d client metadata entries have no internal field and were dropped", len(wo.ClientMeta)),
			Severity: SeverityInfo,
		})
	}

	now := time.Now().UTC()
	task := &taskpb.Task{
		Id:             uuid.NewString(),
		OrderRef:       wo.OrderRef,
		TaskType:       taskType,
		Priority:       priority,
		Zone:           zone,
		SourceSlot:     source.String(),
		DestSlot:       dest.String(),
		Quantity:       int32(wo.Qty),
		State:          taskpb.TaskState_PENDING,
		CreatedAt:      timestamppb.New(now),
		UpdatedAt:      timestamppb.New(now),
		IdempotencyKey: wo.IdempotencyKey,
	}
	return task, warnings, nil
}

// stateMapping is the internal-to-external state translation table.
var stateMapping = map[taskpb.TaskState]string{
	taskpb.TaskState_PENDING:  wms.ExternalStateReceived,
	taskpb.TaskState_ASSIGNED: wms.ExternalStateInProgress,
	taskpb.TaskState_RUNNING:  wms.ExternalStateInProgress,
	taskpb.TaskState_DONE:     wms.ExternalStateComplete,
	taskpb.TaskState_FAILED:   wms.ExternalStateRejected,
}

// ToExternal translates an internal state event into a WMS status update.
// ASSIGNED and RUNNING both collapse to IN_PROGRESS, which is lossy and always
// emits the LOSSY_STATE_COLLAPSE warning.
func ToExternal(ev *taskpb.TaskStateEvent) (wms.OrderStatusUpdate, []Warning) {
	var warnings []Warning
	if ev == nil {
		return wms.OrderStatusUpdate{}, []Warning{{
			Code:     CodeLossyStateCollapse,
			Field:    "state",
			Detail:   "nil event produced no external update",
			Severity: SeverityError,
		}}
	}

	externalState, ok := stateMapping[ev.ToState]
	if !ok {
		externalState = wms.ExternalStateReceived
		warnings = append(warnings, Warning{
			Code:     CodeLossyStateCollapse,
			Field:    "to_state",
			Detail:   fmt.Sprintf("internal state %s has no external mapping; sent as RECEIVED", ev.ToState),
			Severity: SeverityWarning,
		})
	}
	if ev.ToState == taskpb.TaskState_ASSIGNED || ev.ToState == taskpb.TaskState_RUNNING {
		warnings = append(warnings, Warning{
			Code:     CodeLossyStateCollapse,
			Field:    "to_state",
			Detail:   fmt.Sprintf("internal state %s collapsed to IN_PROGRESS for the WMS", ev.ToState),
			Severity: SeverityWarning,
		})
	}

	updatedAt := time.Now().UTC()
	if ev.EmittedAt != nil {
		updatedAt = ev.EmittedAt.AsTime().UTC()
	}
	detail := ev.Reason
	if len(detail) > 500 {
		detail = detail[:500]
		warnings = append(warnings, Warning{
			Code:     CodeLengthyDetail,
			Field:    "reason",
			Detail:   "reason exceeded 500 characters and was truncated for the external update",
			Severity: SeverityInfo,
		})
	}
	return wms.OrderStatusUpdate{
		OrderRef:      ev.OrderRef,
		ExternalState: externalState,
		StateSeq:      ev.Seq,
		Detail:        detail,
		UpdatedAt:     updatedAt,
	}, warnings
}

// ReconciliationReport aggregates mapping warnings by code. It is safe for
// concurrent use by ingest handlers.
type ReconciliationReport struct {
	mu         sync.Mutex
	counts     map[string]int
	severities map[string]string
	samples    map[string]string
	total      int
	generated  time.Time
}

// NewReconciliationReport returns an empty report.
func NewReconciliationReport() *ReconciliationReport {
	return &ReconciliationReport{
		counts:     make(map[string]int),
		severities: make(map[string]string),
		samples:    make(map[string]string),
		generated:  time.Now().UTC(),
	}
}

// Add records warnings in the report.
func (r *ReconciliationReport) Add(warnings ...Warning) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts == nil {
		r.counts = make(map[string]int)
		r.severities = make(map[string]string)
		r.samples = make(map[string]string)
	}
	for _, w := range warnings {
		r.counts[w.Code]++
		r.total++
		if _, ok := r.severities[w.Code]; !ok {
			r.severities[w.Code] = w.Severity
		}
		if _, ok := r.samples[w.Code]; !ok {
			r.samples[w.Code] = w.Detail
		}
	}
}

// Merge folds another report's counts into this one.
func (r *ReconciliationReport) Merge(other *ReconciliationReport) {
	if r == nil || other == nil || r == other {
		return
	}
	other.mu.Lock()
	defer other.mu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts == nil {
		r.counts = make(map[string]int)
		r.severities = make(map[string]string)
		r.samples = make(map[string]string)
	}
	for code, n := range other.counts {
		r.counts[code] += n
	}
	for code, sev := range other.severities {
		if _, ok := r.severities[code]; !ok {
			r.severities[code] = sev
		}
	}
	for code, s := range other.samples {
		if _, ok := r.samples[code]; !ok {
			r.samples[code] = s
		}
	}
	r.total += other.total
}

// ReconciliationEntry is one row of the aggregated report.
type ReconciliationEntry struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Count    int    `json:"count"`
	Sample   string `json:"sample"`
}

// Snapshot is the JSON-friendly aggregate returned by the REST endpoint.
type Snapshot struct {
	Total       int                   `json:"total"`
	GeneratedAt time.Time             `json:"generated_at"`
	Warnings    []ReconciliationEntry `json:"warnings"`
}

// Snapshot returns a sorted, copy-safe aggregate.
func (r *ReconciliationReport) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{GeneratedAt: time.Now().UTC()}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := Snapshot{Total: r.total, GeneratedAt: r.generated}
	for code, n := range r.counts {
		out.Warnings = append(out.Warnings, ReconciliationEntry{
			Code:     code,
			Severity: r.severities[code],
			Count:    n,
			Sample:   r.samples[code],
		})
	}
	sort.Slice(out.Warnings, func(i, j int) bool { return out.Warnings[i].Code < out.Warnings[j].Code })
	return out
}

// Markdown renders the aggregate as a human-readable table. It returns a valid
// table with a "none" row when no warnings have been recorded.
func (r *ReconciliationReport) Markdown() string {
	snap := r.Snapshot()
	var b strings.Builder
	b.WriteString("| Code | Severity | Count | Sample |\n")
	b.WriteString("| --- | --- | ---: | --- |\n")
	if len(snap.Warnings) == 0 {
		b.WriteString("| _none_ | - | 0 | No mapping gaps recorded. |\n")
		return b.String()
	}
	for _, e := range snap.Warnings {
		sample := strings.ReplaceAll(e.Sample, "|", "\\|")
		fmt.Fprintf(&b, "| %s | %s | %d | %s |\n", e.Code, e.Severity, e.Count, sample)
	}
	fmt.Fprintf(&b, "| **Total** | | **%d** | |\n", snap.Total)
	return b.String()
}
