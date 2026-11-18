//go:build integration

// Package integration proves the bridge end to end against real NATS
// JetStream and PostgreSQL containers.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/local/warehousetaskbridge/internal/app"
	"github.com/local/warehousetaskbridge/internal/config"
	"github.com/local/warehousetaskbridge/internal/ingest"
	"github.com/local/warehousetaskbridge/internal/store"
	"github.com/local/warehousetaskbridge/internal/taskpb"
	"github.com/local/warehousetaskbridge/internal/wms"
)

type infra struct {
	dsn     string
	natsURL string
}

func startInfra(t *testing.T) infra {
	t.Helper()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("Docker is not reachable (%v); skipping integration suite", err)
	}
	defer provider.Close()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pg, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER": "bridge", "POSTGRES_PASSWORD": "bridge", "POSTGRES_DB": "bridge",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })
	pgEndpoint, err := pg.Endpoint(ctx, "tcp")
	if err != nil {
		t.Fatalf("postgres endpoint: %v", err)
	}
	pgEndpoint = strings.TrimPrefix(pgEndpoint, "tcp://")

	nc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.10-alpine",
			ExposedPorts: []string{"4222/tcp", "8222/tcp"},
			Cmd:          []string{"-js", "-m", "8222"},
			WaitingFor:   wait.ForLog("Server is ready").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	t.Cleanup(func() { _ = nc.Terminate(context.Background()) })
	natsEndpoint, err := nc.Endpoint(ctx, "tcp")
	if err != nil {
		t.Fatalf("nats endpoint: %v", err)
	}
	natsEndpoint = strings.TrimPrefix(natsEndpoint, "tcp://")

	return infra{
		dsn:     "postgres://bridge:bridge@" + pgEndpoint + "/bridge?sslmode=disable",
		natsURL: "nats://" + natsEndpoint,
	}
}

func testConfig(in infra) config.Config {
	return config.Config{
		HTTPAddr:           "127.0.0.1:0",
		NATSURL:            in.natsURL,
		PostgresDSN:        in.dsn,
		HeartbeatInterval:  100 * time.Millisecond,
		PeerSilenceTimeout: 60 * time.Second,
		BotCount:           6,
		SimSpeedFactor:     1000,
		SimFailureRate:     0,
		SimSeed:            99,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func startApp(t *testing.T, in infra) *app.App {
	t.Helper()
	return startAppWithConfig(t, in, testConfig(in))
}

func startAppWithConfig(t *testing.T, in infra, cfg config.Config) *app.App {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	service, err := app.Start(context.Background(), cfg, app.Options{
		Logger:          discardLogger(),
		Listener:        listener,
		SimTickInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start app: %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service
}

func connectNATS(t *testing.T, in infra) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(in.natsURL, nats.Timeout(10*time.Second))
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func jetstream(t *testing.T, nc *nats.Conn) nats.JetStreamContext {
	t.Helper()
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

type collector struct {
	mu      sync.Mutex
	nc      *nats.Conn
	sub     *nats.Subscription
	updates map[string][]wms.OrderStatusUpdate
}

func newCollector(t *testing.T, in infra) *collector {
	t.Helper()
	c := &collector{nc: connectNATS(t, in), updates: make(map[string][]wms.OrderStatusUpdate)}
	if err := c.start(); err != nil {
		t.Fatalf("subscribe statuses: %v", err)
	}
	t.Cleanup(c.stop)
	return c
}

func (c *collector) start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sub != nil {
		return nil
	}
	sub, err := c.nc.Subscribe("wms.status.>", func(msg *nats.Msg) {
		var update wms.OrderStatusUpdate
		if err := json.Unmarshal(msg.Data, &update); err != nil {
			return
		}
		c.mu.Lock()
		c.updates[update.OrderRef] = append(c.updates[update.OrderRef], update)
		c.mu.Unlock()
	})
	if err != nil {
		return err
	}
	c.sub = sub
	return c.nc.Flush()
}

func (c *collector) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sub != nil {
		_ = c.sub.Unsubscribe()
		c.sub = nil
	}
}

func (c *collector) forOrder(orderRef string) []wms.OrderStatusUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]wms.OrderStatusUpdate(nil), c.updates[orderRef]...)
}

func (c *collector) completedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, updates := range c.updates {
		if len(updates) > 0 && updates[len(updates)-1].ExternalState == wms.ExternalStateComplete {
			n++
		}
	}
	return n
}

func (c *collector) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updates = make(map[string][]wms.OrderStatusUpdate)
}

func sampleOrder(i int, key string) wms.WorkOrder {
	due := time.Now().Add(2 * time.Hour)
	return wms.WorkOrder{
		OrderRef:       fmt.Sprintf("IT-ORD-%05d", i),
		Kind:           "pick",
		PriorityBand:   wms.PriorityStandard,
		SourceLocation: "A-2-3",
		DestLocation:   "B-1-1",
		Qty:            2,
		DueBy:          &due,
		SchemaVersion:  "2025-01-15",
		IdempotencyKey: key,
	}
}

func postOrder(t *testing.T, baseURL string, order wms.WorkOrder) (int, []byte) {
	t.Helper()
	body, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("marshal order: %v", err)
	}
	resp, err := http.Post(baseURL+"/v1/orders", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post order: %v", err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func publishOrder(t *testing.T, js nats.JetStreamContext, order wms.WorkOrder) {
	t.Helper()
	body, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("marshal order: %v", err)
	}
	if _, err := js.Publish("wms.orders.test", body); err != nil {
		t.Fatalf("publish order: %v", err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, description string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, description)
}

func getTaskCount(t *testing.T, db *store.Store, where string, args ...any) int {
	t.Helper()
	var n int
	query := "SELECT count(*) FROM tasks"
	if where != "" {
		query += " WHERE " + where
	}
	if err := db.Pool().QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	return n
}

func assertIncreasing(t *testing.T, updates []wms.OrderStatusUpdate) {
	t.Helper()
	var last uint64
	for i, update := range updates {
		if i > 0 && update.StateSeq <= last {
			t.Fatalf("status seq not strictly increasing at %d: %d after %d", i, update.StateSeq, last)
		}
		last = update.StateSeq
	}
}

func metricValue(t *testing.T, baseURL, name string) int64 {
	t.Helper()
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			var n int64
			_, _ = fmt.Sscanf(fields[1], "%d", &n)
			return n
		}
	}
	return 0
}

func TestHappyPath200Orders(t *testing.T) {
	in := startInfra(t)
	service := startApp(t, in)
	collector := newCollector(t, in)
	nc := connectNATS(t, in)
	js := jetstream(t, nc)

	const total = 200
	for i := 0; i < total; i++ {
		order := sampleOrder(i, fmt.Sprintf("it-happy-%d", i))
		if i%2 == 0 {
			status, body := postOrder(t, service.URL(), order)
			if status != http.StatusCreated {
				t.Fatalf("POST %s status = %d body = %s", order.OrderRef, status, body)
			}
		} else {
			publishOrder(t, js, order)
		}
	}

	waitFor(t, 90*time.Second, "200 COMPLETE updates", func() bool {
		return collector.completedCount() >= total
	})
	if got := getTaskCount(t, service.DB(), "state = 'DONE'"); got != total {
		t.Fatalf("DONE tasks = %d, want %d", got, total)
	}
	for i := 0; i < total; i++ {
		orderRef := fmt.Sprintf("IT-ORD-%05d", i)
		updates := collector.forOrder(orderRef)
		if len(updates) < 3 {
			t.Fatalf("%s received %d updates, want at least 3: %+v", orderRef, len(updates), updates)
		}
		assertIncreasing(t, updates)
		if updates[len(updates)-1].ExternalState != wms.ExternalStateComplete {
			t.Fatalf("%s final external state = %s, want COMPLETE", orderRef, updates[len(updates)-1].ExternalState)
		}
	}
}

func TestDuplicateStormAcrossTransports(t *testing.T) {
	in := startInfra(t)
	service := startApp(t, in)
	collector := newCollector(t, in)
	js := jetstream(t, connectNATS(t, in))

	order := sampleOrder(1, "it-dup-storm")
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			status, body := postOrder(t, service.URL(), order)
			if status != http.StatusCreated && status != http.StatusOK {
				t.Fatalf("duplicate POST status = %d body = %s", status, body)
			}
		} else {
			publishOrder(t, js, order)
		}
	}

	waitFor(t, 10*time.Second, "exactly one task row", func() bool {
		return getTaskCount(t, service.DB(), "idempotency_key = $1", order.IdempotencyKey) == 1
	})
	waitFor(t, 30*time.Second, "duplicate storm completion", func() bool {
		updates := collector.forOrder(order.OrderRef)
		return len(updates) > 0 && updates[len(updates)-1].ExternalState == wms.ExternalStateComplete
	})
	updates := collector.forOrder(order.OrderRef)
	assertIncreasing(t, updates)
	seen := make(map[uint64]bool)
	for _, update := range updates {
		if seen[update.StateSeq] {
			t.Fatalf("duplicate StateSeq %d delivered to the WMS", update.StateSeq)
		}
		seen[update.StateSeq] = true
	}
	if got := getTaskCount(t, service.DB(), ""); got != 1 {
		t.Fatalf("total task rows = %d, want 1", got)
	}
}

func TestMalformedAndUnknownSchemaQuarantine(t *testing.T) {
	in := startInfra(t)
	service := startApp(t, in)
	collector := newCollector(t, in)
	js := jetstream(t, connectNATS(t, in))

	badPayloads := [][]byte{
		[]byte(`{"order_ref":`),
		[]byte(`{"order_ref":"BAD-2","kind":"pick","schema_version":"1990-01-01"}`),
		[]byte(`not-json-at-all`),
	}
	for i, payload := range badPayloads {
		if i%2 == 0 {
			resp, err := http.Post(service.URL()+"/v1/orders", "application/json", bytes.NewReader(payload))
			if err != nil {
				t.Fatalf("post malformed: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode < 400 {
				t.Fatalf("malformed REST payload accepted with %d", resp.StatusCode)
			}
		} else {
			if _, err := js.Publish("wms.orders.test", payload); err != nil {
				t.Fatalf("publish malformed: %v", err)
			}
		}
	}

	waitFor(t, 15*time.Second, "quarantine rows", func() bool {
		var n int
		if err := service.DB().Pool().QueryRow(context.Background(), "SELECT count(*) FROM quarantine").Scan(&n); err != nil {
			return false
		}
		return n >= len(badPayloads)
	})
	resp, err := http.Get(service.URL() + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health after quarantine = %d", resp.StatusCode)
	}

	good := sampleOrder(42, "it-after-malformed")
	status, body := postOrder(t, service.URL(), good)
	if status != http.StatusCreated {
		t.Fatalf("good order after malformed = %d body %s", status, body)
	}
	waitFor(t, 30*time.Second, "good order completes after malformed traffic", func() bool {
		updates := collector.forOrder(good.OrderRef)
		return len(updates) > 0 && updates[len(updates)-1].ExternalState == wms.ExternalStateComplete
	})
}

func TestRedeliveryIsIdempotent(t *testing.T) {
	in := startInfra(t)
	service := startApp(t, in)
	collector := newCollector(t, in)
	nc := connectNATS(t, in)
	js := jetstream(t, nc)

	// Force a transient DB failure on the first delivery by hiding the tasks
	// table, then restore it so the NAKed message is redelivered successfully.
	if _, err := service.DB().Pool().Exec(context.Background(), "ALTER TABLE tasks RENAME TO tasks_hidden"); err != nil {
		t.Fatalf("hide tasks table: %v", err)
	}
	order := sampleOrder(77, "it-redelivery")
	publishOrder(t, js, order)

	waitFor(t, 10*time.Second, "first delivery attempt", func() bool {
		info, err := js.ConsumerInfo(ingest.StreamName, ingest.IngestDurable)
		return err == nil && info.Delivered.Consumer >= 1
	})
	if _, err := service.DB().Pool().Exec(context.Background(), "ALTER TABLE tasks_hidden RENAME TO tasks"); err != nil {
		t.Fatalf("restore tasks table: %v", err)
	}

	waitFor(t, 30*time.Second, "redelivered order accepted", func() bool {
		return getTaskCount(t, service.DB(), "idempotency_key = $1", order.IdempotencyKey) == 1
	})
	waitFor(t, 30*time.Second, "redelivered order completes", func() bool {
		updates := collector.forOrder(order.OrderRef)
		return len(updates) > 0 && updates[len(updates)-1].ExternalState == wms.ExternalStateComplete
	})
	if got := getTaskCount(t, service.DB(), "idempotency_key = $1", order.IdempotencyKey); got != 1 {
		t.Fatalf("task rows after redelivery = %d, want 1", got)
	}
	updates := collector.forOrder(order.OrderRef)
	assertIncreasing(t, updates)
	seen := make(map[uint64]bool)
	for _, update := range updates {
		if seen[update.StateSeq] {
			t.Fatalf("redelivery produced duplicate StateSeq %d", update.StateSeq)
		}
		seen[update.StateSeq] = true
	}
}

func TestPeerSilenceBuffersAndDrains(t *testing.T) {
	in := startInfra(t)
	peerCfg := testConfig(in)
	peerCfg.PeerSilenceTimeout = 1 * time.Second
	service := startAppWithConfig(t, in, peerCfg)
	collector := newCollector(t, in)
	nc := connectNATS(t, in)

	var online atomicBool
	online.set(true)
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				if online.get() {
					payload, _ := json.Marshal(map[string]any{"peer_id": "wms", "sent_at": time.Now().UTC()})
					_ = nc.Publish("wms.heartbeat", payload)
				}
			}
		}
	}()
	t.Cleanup(func() {
		close(stopHeartbeat)
		<-heartbeatDone
	})

	// Baseline traffic completes while the peer is live.
	baseline := 10
	for i := 0; i < baseline; i++ {
		if _, body := postOrder(t, service.URL(), sampleOrder(1000+i, fmt.Sprintf("it-silence-base-%d", i))); len(body) == 0 {
			t.Fatal("empty response")
		}
	}
	waitFor(t, 30*time.Second, "baseline completion", func() bool {
		return collector.completedCount() >= baseline
	})

	// Go silent: heartbeats stop and the status subscription detaches.
	online.set(false)
	waitFor(t, 10*time.Second, "silence detection", func() bool {
		return metricValue(t, service.URL(), "peer_silence_events") >= 1
	})
	collector.stop()
	collector.reset()

	const duringSilence = 20
	for i := 0; i < duringSilence; i++ {
		order := sampleOrder(2000+i, fmt.Sprintf("it-silence-%d", i))
		if status, body := postOrder(t, service.URL(), order); status != http.StatusCreated {
			t.Fatalf("silence order status = %d body %s", status, body)
		}
	}
	waitFor(t, 20*time.Second, "events buffered while silent", func() bool {
		return metricValue(t, service.URL(), "events_buffered") >= 10
	})
	buffered := metricValue(t, service.URL(), "events_buffered")
	if buffered == 0 {
		t.Fatal("no events buffered during peer silence")
	}
	collector.reset()
	if err := collector.start(); err != nil {
		t.Fatalf("reattach status subscription: %v", err)
	}

	// Heartbeat resumes; the publisher must drain the buffer in order.
	online.set(true)
	payload, _ := json.Marshal(map[string]any{"peer_id": "wms", "sent_at": time.Now().UTC()})
	if err := nc.Publish("wms.heartbeat", payload); err != nil {
		t.Fatalf("resume heartbeat: %v", err)
	}
	waitFor(t, 45*time.Second, "all silent-window orders complete", func() bool {
		completed := 0
		for i := 0; i < duringSilence; i++ {
			orderRef := fmt.Sprintf("IT-ORD-%05d", 2000+i)
			updates := collector.forOrder(orderRef)
			if len(updates) > 0 && updates[len(updates)-1].ExternalState == wms.ExternalStateComplete {
				completed++
			}
		}
		return completed == duringSilence
	})
	for i := 0; i < duringSilence; i++ {
		orderRef := fmt.Sprintf("IT-ORD-%05d", 2000+i)
		updates := collector.forOrder(orderRef)
		if len(updates) != 3 {
			t.Fatalf("%s received %d updates, want exactly 3 (no gaps): %+v", orderRef, len(updates), updates)
		}
		assertIncreasing(t, updates)
	}
	waitFor(t, 20*time.Second, "buffer drained", func() bool {
		return metricValue(t, service.URL(), "events_drained") >= buffered
	})
	if gaps := metricValue(t, service.URL(), "order_gaps_detected"); gaps != 0 {
		t.Fatalf("order gaps detected = %d, want 0", gaps)
	}
}

type atomicBool struct {
	mu sync.Mutex
	v  bool
}

func (a *atomicBool) set(v bool) {
	a.mu.Lock()
	a.v = v
	a.mu.Unlock()
}

func (a *atomicBool) get() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.v
}

func TestCrashRecoveryDrainsUnpublishedEvents(t *testing.T) {
	in := startInfra(t)
	service := startApp(t, in)
	collector := newCollector(t, in)

	ctx := context.Background()
	db, err := store.Open(ctx, in.dsn)
	if err != nil {
		t.Fatalf("open independent store: %v", err)
	}
	t.Cleanup(db.Close)

	for i := 0; i < 80; i++ {
		if status, body := postOrder(t, service.URL(), sampleOrder(3000+i, fmt.Sprintf("it-crash-%d", i))); status != http.StatusCreated {
			t.Fatalf("crash order status = %d body %s", status, body)
		}
	}
	// Close mid-run, like a process crash: some tasks will be in flight and some
	// outbox events will not yet be acknowledged.
	if err := service.Close(); err != nil {
		t.Fatalf("close app: %v", err)
	}

	// Guarantee unpublished work exists: drive a few PENDING tasks to ASSIGNED
	// while the bridge is down so the outbox must survive the restart.
	rows, err := db.Pool().Query(ctx, `SELECT id FROM tasks WHERE state = 'PENDING' LIMIT 5`)
	if err != nil {
		t.Fatalf("select pending: %v", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan pending id: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if _, err := db.TransitionTask(ctx, id, taskpb.TaskState_PENDING, taskpb.TaskState_ASSIGNED,
			"bot-99", "crash recovery test: encoded while bridge is down", time.Now().UTC()); err != nil {
			t.Fatalf("create unpublished event: %v", err)
		}
	}

	rows, err = db.Pool().Query(ctx, `SELECT seq FROM task_events WHERE published = false ORDER BY seq`)
	if err != nil {
		t.Fatalf("select unpublished: %v", err)
	}
	var snapshot []uint64
	for rows.Next() {
		var seq uint64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("scan seq: %v", err)
		}
		snapshot = append(snapshot, seq)
	}
	rows.Close()
	if len(snapshot) == 0 {
		t.Fatal("expected unpublished outbox events after crash")
	}

	collector.reset()
	if err := collector.start(); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}

	restarted, err := app.Start(context.Background(), testConfig(in), app.Options{
		Logger: discardLogger(), SimTickInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("restart app: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })

	waitFor(t, 60*time.Second, "outbox fully drained", func() bool {
		n, err := restarted.DB().CountUnpublished(ctx)
		return err == nil && n == 0
	})
	for _, seq := range snapshot {
		var published bool
		if err := restarted.DB().Pool().QueryRow(ctx,
			`SELECT published FROM task_events WHERE seq = $1`, seq).Scan(&published); err != nil {
			t.Fatalf("inspect seq %d: %v", seq, err)
		}
		if !published {
			t.Fatalf("seq %d still unpublished after restart", seq)
		}
	}

	// Every snapshot seq must have reached the WMS exactly once.
	waitFor(t, 30*time.Second, "snapshot events delivered", func() bool {
		seen := make(map[uint64]int)
		for _, updates := range collectorSnapshot(collector) {
			for _, update := range updates {
				seen[update.StateSeq]++
			}
		}
		for _, seq := range snapshot {
			if seen[seq] == 0 {
				return false
			}
		}
		return true
	})
	seen := make(map[uint64]int)
	for _, updates := range collectorSnapshot(collector) {
		for _, update := range updates {
			seen[update.StateSeq]++
		}
	}
	for _, seq := range snapshot {
		if seen[seq] != 1 {
			t.Fatalf("seq %d delivered %d times, want exactly 1", seq, seen[seq])
		}
	}
}

func collectorSnapshot(c *collector) map[string][]wms.OrderStatusUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][]wms.OrderStatusUpdate, len(c.updates))
	for k, v := range c.updates {
		out[k] = append([]wms.OrderStatusUpdate(nil), v...)
	}
	return out
}
