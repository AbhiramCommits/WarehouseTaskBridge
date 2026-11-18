package store

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// timestamppbNew converts a time.Time to a protobuf timestamp, tolerating zero.
func timestamppbNew(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// timestampOf converts a protobuf timestamp to time.Time, tolerating nil.
func timestampOf(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
