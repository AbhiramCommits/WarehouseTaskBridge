// Package wms defines the external Warehouse Management System wire schema.
//
// These types are intentionally shaped like a customer integration contract:
// flat, string-heavy, with coarse status vocabulary. They are not the internal
// domain model and must be translated before use.
package wms

import "time"

// SupportedSchemaVersions lists the external schema versions this bridge
// understands. Anything else is quarantined rather than guessed at.
var SupportedSchemaVersions = map[string]bool{
	"2024-06-01": true,
	"2025-01-15": true,
}

// WorkOrder is a unit of work submitted by the external WMS.
type WorkOrder struct {
	OrderRef       string            `json:"order_ref"`
	Kind           string            `json:"kind"`
	PriorityBand   string            `json:"priority_band"`
	SourceLocation string            `json:"source_location"`
	DestLocation   string            `json:"dest_location"`
	Qty            int               `json:"qty"`
	DueBy          *time.Time        `json:"due_by,omitempty"`
	ClientMeta     map[string]string `json:"client_meta,omitempty"`
	SchemaVersion  string            `json:"schema_version"`
	IdempotencyKey string            `json:"idempotency_key"`
}

// Priority bands accepted from the external system.
const (
	PriorityExpedite = "EXPEDITE"
	PriorityStandard = "STANDARD"
	PriorityBulk     = "BULK"
)

// External order states. This vocabulary is coarser than the internal task
// lifecycle: ASSIGNED and RUNNING both surface as IN_PROGRESS.
const (
	ExternalStateReceived   = "RECEIVED"
	ExternalStateInProgress = "IN_PROGRESS"
	ExternalStateStaged     = "STAGED"
	ExternalStateComplete   = "COMPLETE"
	ExternalStateRejected   = "REJECTED"
)

// OrderStatusUpdate is a lifecycle update sent back to the WMS.
type OrderStatusUpdate struct {
	OrderRef      string    `json:"order_ref"`
	ExternalState string    `json:"external_state"`
	StateSeq      uint64    `json:"state_seq"`
	Detail        string    `json:"detail,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}
