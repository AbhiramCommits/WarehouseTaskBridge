package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// NATS ingest topology.
const (
	// StreamName is the JetStream stream carrying inbound external orders.
	StreamName = "WMS_IN"
	// OrderSubject is the wildcard subscribed by the stream.
	OrderSubject = "wms.orders.>"
	// IngestDurable is the durable pull-consumer name.
	IngestDurable = "bridge-ingest"
	// QuarantineSubject receives raw copies of rejected payloads.
	QuarantineSubject = "wms.quarantine"
)

// NATSIngester consumes external orders from JetStream with explicit acks.
type NATSIngester struct {
	conn       *nats.Conn
	js         nats.JetStreamContext
	acceptor   *Acceptor
	logger     *slog.Logger
	nakBackoff time.Duration
}

// NewNATSIngester builds a JetStream ingester.
func NewNATSIngester(conn *nats.Conn, js nats.JetStreamContext, acceptor *Acceptor, logger *slog.Logger) (*NATSIngester, error) {
	if conn == nil {
		return nil, errors.New("ingest: nats ingester requires a connection")
	}
	if js == nil {
		return nil, errors.New("ingest: nats ingester requires a JetStream context")
	}
	if acceptor == nil {
		return nil, errors.New("ingest: nats ingester requires an acceptor")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &NATSIngester{
		conn:       conn,
		js:         js,
		acceptor:   acceptor,
		logger:     logger,
		nakBackoff: 2 * time.Second,
	}, nil
}

// EnsureStream creates the WMS_IN stream and durable pull consumer when absent.
// It is safe to call on every start.
func (n *NATSIngester) EnsureStream() error {
	_, err := n.js.StreamInfo(StreamName)
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = n.js.AddStream(&nats.StreamConfig{
			Name:       StreamName,
			Subjects:   []string{OrderSubject},
			Storage:    nats.FileStorage,
			Retention:  nats.LimitsPolicy,
			MaxAge:     24 * time.Hour,
			Duplicates: 2 * time.Minute,
		})
		if err != nil {
			return fmt.Errorf("ingest: create stream %s: %w", StreamName, err)
		}
		n.logger.Info("jetstream stream created", "stream", StreamName, "subjects", OrderSubject)
	} else if err != nil {
		return fmt.Errorf("ingest: inspect stream %s: %w", StreamName, err)
	}

	if _, err := n.js.ConsumerInfo(StreamName, IngestDurable); errors.Is(err, nats.ErrConsumerNotFound) {
		_, err = n.js.AddConsumer(StreamName, &nats.ConsumerConfig{
			Durable:       IngestDurable,
			AckPolicy:     nats.AckExplicitPolicy,
			MaxDeliver:    10,
			AckWait:       30 * time.Second,
			MaxAckPending: 256,
			FilterSubject: OrderSubject,
		})
		if err != nil {
			return fmt.Errorf("ingest: create consumer %s: %w", IngestDurable, err)
		}
		n.logger.Info("jetstream consumer created", "stream", StreamName, "consumer", IngestDurable)
	} else if err != nil {
		return fmt.Errorf("ingest: inspect consumer %s: %w", IngestDurable, err)
	}
	return nil
}

// Run pulls and processes orders until ctx is cancelled.
func (n *NATSIngester) Run(ctx context.Context) error {
	sub, err := n.js.PullSubscribe(OrderSubject, IngestDurable,
		nats.Bind(StreamName, IngestDurable))
	if err != nil {
		return fmt.Errorf("ingest: pull subscribe: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := sub.Fetch(16, nats.Context(ctx))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || ctx.Err() != nil {
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			n.logger.Error("jetstream fetch failed", "error", err)
			continue
		}
		for _, msg := range msgs {
			n.handle(ctx, msg)
		}
	}
}

// handle processes one message. Permanent rejections are acked after
// quarantine; transient failures are NAKed with a delay for safe redelivery.
func (n *NATSIngester) handle(ctx context.Context, msg *nats.Msg) {
	result, err := n.acceptor.AcceptOrder(ctx, msg.Subject, msg.Data)
	if err != nil {
		var rejection *Rejection
		if errors.As(err, &rejection) {
			if ackErr := msg.Ack(); ackErr != nil {
				n.logger.Error("ack rejected message", "subject", msg.Subject, "error", ackErr)
			}
			n.logger.Warn("rejected message acknowledged after quarantine",
				"subject", msg.Subject, "reason", rejection.Reason)
			return
		}
		if nakErr := msg.NakWithDelay(n.nakBackoff); nakErr != nil {
			n.logger.Error("nak transient message", "subject", msg.Subject, "error", nakErr)
			return
		}
		n.logger.Warn("transient ingest failure; message will be redelivered",
			"subject", msg.Subject, "error", err, "backoff", n.nakBackoff.String())
		return
	}
	if err := msg.Ack(); err != nil {
		n.logger.Error("ack accepted message", "subject", msg.Subject, "error", err)
		return
	}
	n.logger.Debug("order accepted",
		"subject", msg.Subject, "task_id", result.TaskID,
		"created", result.Created, "warnings", len(result.Warnings))
}

// PublishQuarantine mirrors a raw rejected payload onto wms.quarantine using
// core NATS. The durable record is the quarantine table.
func (n *NATSIngester) PublishQuarantine(_ string, raw []byte, reason string) {
	msg := nats.NewMsg(QuarantineSubject)
	msg.Data = raw
	msg.Header.Set("X-Quarantine-Reason", reason)
	if err := n.conn.PublishMsg(msg); err != nil {
		n.logger.Error("publish quarantine mirror", "error", err)
	}
}

// QuarantineMirror returns a sink suitable for Acceptor.SetQuarantineSink.
func (n *NATSIngester) QuarantineMirror() func(string, []byte, string) {
	return n.PublishQuarantine
}
