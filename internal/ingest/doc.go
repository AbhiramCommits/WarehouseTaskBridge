// Package ingest accepts external work orders over NATS JetStream and REST,
// applies schema validation and translation, enforces idempotency, and
// quarantines payloads that cannot be accepted.
package ingest
