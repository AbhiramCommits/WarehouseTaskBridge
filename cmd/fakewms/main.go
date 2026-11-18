// Command fakewms impersonates a customer Warehouse Management System. It
// publishes a reproducible mix of work orders over NATS and REST, subscribes to
// the bridge's status stream, optionally goes silent, and prints a measured
// end-to-end report.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/local/warehousetaskbridge/internal/feedback"
	"github.com/local/warehousetaskbridge/internal/ingest"
	"github.com/local/warehousetaskbridge/internal/wms"
)

type options struct {
	orders        int
	dupRate       float64
	malformedRate float64
	unmappedRate  float64
	goSilentAfter time.Duration
	silentFor     time.Duration
	seed          int64
	transport     string
	bridgeURL     string
	natsURL       string
	sendInterval  time.Duration
	heartbeat     time.Duration
	timeout       time.Duration
	resultsOut    string
}

func main() {
	var opts options
	flag.IntVar(&opts.orders, "orders", 1000, "number of unique work orders")
	flag.Float64Var(&opts.dupRate, "dup-rate", 0.15, "fraction of orders sent an extra time with the same idempotency key")
	flag.Float64Var(&opts.malformedRate, "malformed-rate", 0.05, "fraction of orders additionally sent as malformed payloads")
	flag.Float64Var(&opts.unmappedRate, "unmapped-kind-rate", 0.08, "fraction of orders using an unmappable kind")
	flag.DurationVar(&opts.goSilentAfter, "go-silent-after", 0, "go silent (heartbeat + status consumption) after this duration; 0 disables")
	flag.DurationVar(&opts.silentFor, "silent-for", 12*time.Second, "how long to stay silent before recovering")
	flag.Int64Var(&opts.seed, "seed", 424242, "random seed for reproducible workloads")
	flag.StringVar(&opts.transport, "transport", "mixed", "order transport: nats, rest, or mixed")
	flag.StringVar(&opts.bridgeURL, "bridge-url", "http://127.0.0.1:8080", "bridge REST base URL")
	flag.StringVar(&opts.natsURL, "nats-url", nats.DefaultURL, "NATS URL")
	flag.DurationVar(&opts.sendInterval, "send-interval", 20*time.Millisecond, "delay between unique orders")
	flag.DurationVar(&opts.heartbeat, "heartbeat-interval", 500*time.Millisecond, "WMS heartbeat period")
	flag.DurationVar(&opts.timeout, "timeout", 120*time.Second, "maximum wait for every order to reach COMPLETE")
	flag.StringVar(&opts.resultsOut, "results-out", "", "optional path for a JSON results file")
	flag.Parse()

	if err := run(opts); err != nil {
		fmt.Fprintf(os.Stderr, "fakewms: %v\n", err)
		os.Exit(1)
	}
}

func run(opts options) error {
	if opts.orders <= 0 {
		return errors.New("orders must be positive")
	}
	if opts.transport != "nats" && opts.transport != "rest" && opts.transport != "mixed" {
		return fmt.Errorf("transport must be nats, rest, or mixed; got %q", opts.transport)
	}
	for name, v := range map[string]float64{
		"dup-rate": opts.dupRate, "malformed-rate": opts.malformedRate,
		"unmapped-kind-rate": opts.unmappedRate,
	} {
		if v < 0 || v > 1 {
			return fmt.Errorf("%s must be within [0,1], got %f", name, v)
		}
	}
	if opts.goSilentAfter > 0 && opts.silentFor <= 0 {
		return errors.New("silent-for must be positive when -go-silent-after is set")
	}

	nc, err := nats.Connect(opts.natsURL, nats.Name("fakewms"), nats.Timeout(10*time.Second))
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	if err := ingest.EnsureOrderStream(js); err != nil {
		return err
	}

	rec := newRecorder()
	pump, err := newStatusPump(js, rec)
	if err != nil {
		return err
	}
	defer pump.stop()

	start := time.Now()
	var online atomic.Bool
	online.Store(true)
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(opts.heartbeat)
		defer ticker.Stop()
		_ = publishHeartbeat(nc)
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				if online.Load() {
					_ = publishHeartbeat(nc)
				}
			}
		}
	}()

	var silenceDone chan struct{}
	if opts.goSilentAfter > 0 {
		silenceDone = make(chan struct{})
		go func() {
			defer close(silenceDone)
			time.Sleep(opts.goSilentAfter)
			fmt.Printf("[fakewms] going silent at t=%.1fs (heartbeats paused, status subscription detached)\n", time.Since(start).Seconds())
			online.Store(false)
			pump.pause()
			time.Sleep(opts.silentFor)
			if err := pump.resume(); err != nil {
				fmt.Printf("[fakewms] status resume failed: %v\n", err)
			}
			online.Store(true)
			_ = publishHeartbeat(nc)
			fmt.Printf("[fakewms] recovered at t=%.1fs (status subscription reattached)\n", time.Since(start).Seconds())
		}()
	}

	client := &http.Client{Timeout: 10 * time.Second}
	rng := rand.New(rand.NewSource(opts.seed))
	malformedTotal := int(math.Round(opts.malformedRate * float64(opts.orders)))
	malformedEvery := 0
	if malformedTotal > 0 {
		malformedEvery = max(1, opts.orders/malformedTotal)
	}

	var (
		sent, duplicates, malformedSent int
		sendErrors                      int
	)
	expected := make([]string, 0, opts.orders)
	malformedSentSoFar := 0

	for i := 0; i < opts.orders; i++ {
		order := makeOrder(rng, opts, i)
		raw, err := json.Marshal(order)
		if err != nil {
			return err
		}
		ref := order.OrderRef
		expected = append(expected, ref)
		rec.markSent(ref)
		primary := chooseTransport(opts.transport, rng)
		if err := sendOrder(client, js, opts, primary, raw); err != nil {
			sendErrors++
			fmt.Printf("[fakewms] send %s via %s failed: %v\n", ref, primary, err)
		} else {
			sent++
		}

		if rng.Float64() < opts.dupRate {
			dupTransport := oppositeTransport(opts.transport, primary)
			if err := sendOrder(client, js, opts, dupTransport, raw); err != nil {
				sendErrors++
			} else {
				sent++
				duplicates++
			}
		}

		if malformedEvery > 0 && malformedSentSoFar < malformedTotal && i%malformedEvery == 0 && i > 0 {
			bad := []byte(fmt.Sprintf(`{"order_ref":"MAL-%06d","kind":`, i))
			badTransport := chooseTransport(opts.transport, rng)
			if err := sendRaw(client, js, opts, badTransport, bad); err == nil {
				malformedSent++
				malformedSentSoFar++
			} else {
				sendErrors++
			}
		}

		if opts.sendInterval > 0 {
			time.Sleep(opts.sendInterval)
		}
	}

	// Flush any scheduled malformed payloads that were not interleaved.
	for malformedSentSoFar < malformedTotal {
		bad := []byte(fmt.Sprintf(`{"order_ref":"MAL-EXTRA-%06d","kind":`, malformedSentSoFar))
		if err := sendRaw(client, js, opts, chooseTransport(opts.transport, rng), bad); err != nil {
			sendErrors++
			break
		}
		malformedSent++
		malformedSentSoFar++
	}

	fmt.Printf("[fakewms] sent %d messages (%d unique, %d duplicates, %d malformed) in %.1fs; waiting for completion\n",
		sent+malformedSent, opts.orders, duplicates, malformedSent, time.Since(start).Seconds())

	deadline := time.Now().Add(opts.timeout)
	for rec.completedCount() < opts.orders && time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
	}
	if silenceDone != nil {
		<-silenceDone
	}
	// Give the resumed status consumer a moment to drain any final buffered ack.
	drainDeadline := time.Now().Add(5 * time.Second)
	for pump.pendingBacklog() > 0 && time.Now().Before(drainDeadline) {
		time.Sleep(100 * time.Millisecond)
	}
	close(stopHeartbeat)
	<-heartbeatDone

	counterMap, metricsErr := fetchMetrics(opts.bridgeURL)
	if metricsErr != nil {
		fmt.Printf("[fakewms] warning: metrics unavailable: %v\n", metricsErr)
		counterMap = map[string]int64{}
	}
	recon, reconErr := fetchReconciliation(opts.bridgeURL)
	if reconErr != nil {
		fmt.Printf("[fakewms] warning: reconciliation unavailable: %v\n", reconErr)
	}

	report := buildReport(opts, rec, expected, sent, duplicates, malformedSent, sendErrors,
		counterMap, recon, time.Since(start))
	printReport(report)

	if opts.resultsOut != "" {
		body, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		body = append(body, '\n')
		if err := os.WriteFile(opts.resultsOut, body, 0o644); err != nil {
			return fmt.Errorf("write results: %w", err)
		}
		fmt.Printf("[fakewms] results written to %s\n", opts.resultsOut)
	}
	return nil
}

func publishHeartbeat(nc *nats.Conn) error {
	payload, _ := json.Marshal(map[string]any{"peer_id": "wms", "sent_at": time.Now().UTC()})
	return nc.Publish(wmsHeartbeatSubject, payload)
}

const wmsHeartbeatSubject = "wms.heartbeat"

func chooseTransport(mode string, rng *rand.Rand) string {
	switch mode {
	case "nats", "rest":
		return mode
	default:
		if rng.Intn(2) == 0 {
			return "nats"
		}
		return "rest"
	}
}

func oppositeTransport(mode, primary string) string {
	if mode == "mixed" {
		if primary == "nats" {
			return "rest"
		}
		return "nats"
	}
	return mode
}

func sendOrder(client *http.Client, js nats.JetStreamContext, opts options, transport string, body []byte) error {
	if transport == "nats" {
		_, err := js.Publish("wms.orders.orders", body)
		return err
	}
	resp, err := client.Post(opts.bridgeURL+"/v1/orders", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 500 {
		return nil
	}
	return fmt.Errorf("rest status %d", resp.StatusCode)
}

func sendRaw(client *http.Client, js nats.JetStreamContext, opts options, transport string, body []byte) error {
	return sendOrder(client, js, opts, transport, body)
}

func makeOrder(rng *rand.Rand, opts options, i int) wms.WorkOrder {
	kinds := []string{"pick", "putaway", "relocate"}
	kind := kinds[rng.Intn(len(kinds))]
	if rng.Float64() < opts.unmappedRate {
		kind = "kitting"
	}
	bands := []string{wms.PriorityExpedite, wms.PriorityStandard, wms.PriorityStandard, wms.PriorityStandard, wms.PriorityBulk}
	band := bands[rng.Intn(len(bands))]
	source := randomSlot(rng)
	dest := randomSlot(rng)
	for dest == source {
		dest = randomSlot(rng)
	}
	order := wms.WorkOrder{
		OrderRef:       fmt.Sprintf("ORD-%06d", i+1),
		Kind:           kind,
		PriorityBand:   band,
		SourceLocation: source,
		DestLocation:   dest,
		Qty:            1 + rng.Intn(20),
		SchemaVersion:  "2025-01-15",
		IdempotencyKey: fmt.Sprintf("idem-%d-%06d", opts.seed, i+1),
	}
	if i%4 != 0 {
		due := time.Now().Add(time.Duration(1+rng.Intn(72)) * time.Hour)
		order.DueBy = &due
	}
	if i%3 == 0 {
		order.ClientMeta = map[string]string{"purchase_order": fmt.Sprintf("PO-%06d", i+1)}
	}
	return order
}

func randomSlot(rng *rand.Rand) string {
	aisle := string(rune('A' + rng.Intn(20)))
	return fmt.Sprintf("%s-%d-%d", aisle, 1+rng.Intn(20), 1+rng.Intn(5))
}

type recorder struct {
	mu          sync.Mutex
	statuses    map[string][]wms.OrderStatusUpdate
	seen        map[string]map[uint64]bool
	lastSeq     map[string]uint64
	sentAt      map[string]time.Time
	completedAt map[string]time.Time
	received    int
	outOfOrder  int
	duplicates  int
}

func newRecorder() *recorder {
	return &recorder{
		statuses:    make(map[string][]wms.OrderStatusUpdate),
		seen:        make(map[string]map[uint64]bool),
		lastSeq:     make(map[string]uint64),
		sentAt:      make(map[string]time.Time),
		completedAt: make(map[string]time.Time),
	}
}

func (r *recorder) markSent(orderRef string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sentAt[orderRef]; !ok {
		r.sentAt[orderRef] = time.Now()
	}
}

func (r *recorder) record(update wms.OrderStatusUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := r.seen[update.OrderRef]
	if seen == nil {
		seen = make(map[uint64]bool)
		r.seen[update.OrderRef] = seen
	}
	if seen[update.StateSeq] {
		r.duplicates++
		return
	}
	if last, ok := r.lastSeq[update.OrderRef]; ok && update.StateSeq < last {
		r.outOfOrder++
		return
	}
	seen[update.StateSeq] = true
	r.lastSeq[update.OrderRef] = update.StateSeq
	r.statuses[update.OrderRef] = append(r.statuses[update.OrderRef], update)
	r.received++
	if update.ExternalState == wms.ExternalStateComplete {
		if _, ok := r.completedAt[update.OrderRef]; !ok {
			r.completedAt[update.OrderRef] = time.Now()
		}
	}
}

func (r *recorder) completedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.completedAt)
}

func (r *recorder) finalStates() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]string, len(r.statuses))
	for order, updates := range r.statuses {
		if len(updates) > 0 {
			out[order] = updates[len(updates)-1].ExternalState
		}
	}
	return out
}

type latencySamples []float64

func (l latencySamples) percentile(p float64) float64 {
	if len(l) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(l)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(l) {
		idx = len(l) - 1
	}
	return l[idx]
}

type statusPump struct {
	mu      sync.Mutex
	js      nats.JetStreamContext
	durable string
	sub     *nats.Subscription
	rec     *recorder
}

func newStatusPump(js nats.JetStreamContext, rec *recorder) (*statusPump, error) {
	if err := feedback.EnsureStatusStream(js); err != nil {
		return nil, err
	}
	p := &statusPump{js: js, durable: "fakewms-status", rec: rec}
	_ = js.DeleteConsumer(feedback.StatusStream, p.durable)
	if _, err := js.AddConsumer(feedback.StatusStream, &nats.ConsumerConfig{
		Durable:       p.durable,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverNewPolicy,
		FilterSubject: feedback.StatusSubjectPrefix + ">",
		MaxAckPending: 4096,
	}); err != nil {
		return nil, fmt.Errorf("create status consumer: %w", err)
	}
	if err := p.subscribe(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *statusPump) subscribe() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.subscribeLocked()
}

func (p *statusPump) subscribeLocked() error {
	if p.sub != nil {
		return nil
	}
	sub, err := p.js.PullSubscribe(feedback.StatusSubjectPrefix+">", p.durable,
		nats.Bind(feedback.StatusStream, p.durable))
	if err != nil {
		return fmt.Errorf("pull subscribe status: %w", err)
	}
	p.sub = sub
	go p.loop(sub)
	return nil
}

func (p *statusPump) loop(sub *nats.Subscription) {
	for {
		msgs, err := sub.Fetch(32, nats.MaxWait(250*time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return
		}
		for _, msg := range msgs {
			var update wms.OrderStatusUpdate
			if err := json.Unmarshal(msg.Data, &update); err != nil {
				log.Printf("fakewms: bad status payload: %v", err)
				_ = msg.Ack()
				continue
			}
			p.rec.record(update)
			_ = msg.Ack()
		}
	}
}

func (p *statusPump) pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sub != nil {
		_ = p.sub.Unsubscribe()
		p.sub = nil
	}
}

func (p *statusPump) resume() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.subscribeLocked()
}

func (p *statusPump) pendingBacklog() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sub == nil {
		return 0
	}
	info, err := p.sub.ConsumerInfo()
	if err != nil {
		return 0
	}
	return int(info.NumPending) + int(info.NumAckPending)
}

func (p *statusPump) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sub != nil {
		_ = p.sub.Unsubscribe()
		p.sub = nil
	}
}

func fetchMetrics(baseURL string) (map[string]int64, error) {
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64)
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		var v int64
		if _, err := fmt.Sscanf(fields[1], "%d", &v); err != nil {
			continue
		}
		out[fields[0]] = v
	}
	return out, nil
}

type reconWarning struct {
	Code  string `json:"code"`
	Count int    `json:"count"`
}

type reconSnapshot struct {
	Total    int            `json:"total"`
	Warnings []reconWarning `json:"warnings"`
}

func fetchReconciliation(baseURL string) (reconSnapshot, error) {
	resp, err := http.Get(baseURL + "/v1/reconciliation")
	if err != nil {
		return reconSnapshot{}, err
	}
	defer resp.Body.Close()
	var snap reconSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return reconSnapshot{}, err
	}
	return snap, nil
}

type latencyReport struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	Max float64 `json:"max"`
}

type peerSilenceReport struct {
	SilenceEvents int64 `json:"silence_events"`
	Buffered      int64 `json:"buffered"`
	Drained       int64 `json:"drained"`
	Gaps          int64 `json:"gaps"`
}

type report struct {
	OrdersSent            int               `json:"orders_sent"`
	UniqueOrders          int               `json:"unique_orders"`
	DuplicatesSent        int               `json:"duplicates_sent"`
	TasksCreated          int64             `json:"tasks_created"`
	MalformedSent         int               `json:"malformed_sent"`
	MalformedQuarantined  int64             `json:"malformed_quarantined"`
	UnmappedKindWarnings  int               `json:"unmapped_kind_warnings"`
	StatusUpdatesReceived int               `json:"status_updates_received"`
	OutOfOrderDetections  int               `json:"out_of_order_detections"`
	DuplicateDeliveries   int               `json:"duplicate_deliveries"`
	EventsPublished       int64             `json:"events_published"`
	EventsLost            int64             `json:"events_lost"`
	SendErrors            int               `json:"send_errors"`
	LatencyMS             latencyReport     `json:"latency_ms"`
	PeerSilence           peerSilenceReport `json:"peer_silence"`
	FinalStates           map[string]int    `json:"final_states"`
	PerOrderFinalState    map[string]string `json:"per_order_final_state"`
	Reconciliation        reconSnapshot     `json:"reconciliation"`
	DurationSeconds       float64           `json:"duration_seconds"`
	OrdersReceivedCounter int64             `json:"orders_received_counter"`
	OrdersDedupedCounter  int64             `json:"orders_deduped_counter"`
	Seed                  int64             `json:"seed"`
}

func buildReport(opts options, rec *recorder, expected []string, sent, duplicates, malformed, sendErrors int,
	counters map[string]int64, recon reconSnapshot, elapsed time.Duration) report {
	finalStates := rec.finalStates()
	finalCounts := make(map[string]int)
	orderFinal := make(map[string]string, len(expected))
	for _, order := range expected {
		state, ok := finalStates[order]
		if !ok {
			state = "UNKNOWN"
		}
		orderFinal[order] = state
		finalCounts[state]++
	}

	rec.mu.Lock()
	latencies := make(latencySamples, 0, len(rec.completedAt))
	for order, completed := range rec.completedAt {
		if sentAt, ok := rec.sentAt[order]; ok {
			latencies = append(latencies, float64(completed.Sub(sentAt).Milliseconds()))
		}
	}
	received := rec.received
	outOfOrder := rec.outOfOrder
	duplicatesDelivered := rec.duplicates
	rec.mu.Unlock()
	sort.Float64s(latencies)

	late := latencyReport{
		P50: latencies.percentile(50),
		P95: latencies.percentile(95),
	}
	if len(latencies) > 0 {
		late.Max = latencies[len(latencies)-1]
	}

	unmapped := 0
	for _, warning := range recon.Warnings {
		if warning.Code == "UNMAPPED_KIND" {
			unmapped = warning.Count
		}
	}
	eventsPublished := counters["events_published"]
	lost := eventsPublished - int64(received)
	if lost < 0 {
		lost = 0
	}
	return report{
		OrdersSent:            sent + malformed,
		UniqueOrders:          opts.orders,
		DuplicatesSent:        duplicates,
		TasksCreated:          counters["tasks_created"],
		MalformedSent:         malformed,
		MalformedQuarantined:  counters["quarantined"],
		UnmappedKindWarnings:  unmapped,
		StatusUpdatesReceived: received,
		OutOfOrderDetections:  outOfOrder,
		DuplicateDeliveries:   duplicatesDelivered,
		EventsPublished:       eventsPublished,
		EventsLost:            lost,
		SendErrors:            sendErrors,
		LatencyMS:             late,
		PeerSilence: peerSilenceReport{
			SilenceEvents: counters["peer_silence_events"],
			Buffered:      counters["events_buffered"],
			Drained:       counters["events_drained"],
			Gaps:          counters["order_gaps_detected"],
		},
		FinalStates:           finalCounts,
		PerOrderFinalState:    orderFinal,
		Reconciliation:        recon,
		DurationSeconds:       math.Round(elapsed.Seconds()*100) / 100,
		OrdersReceivedCounter: counters["orders_received"],
		OrdersDedupedCounter:  counters["orders_deduped"],
		Seed:                  opts.seed,
	}
}

func printReport(r report) {
	fmt.Println()
	fmt.Println("=== WarehouseTaskBridge fake-WMS report ===")
	fmt.Printf("orders sent .................. %d\n", r.OrdersSent)
	fmt.Printf("unique orders ................ %d\n", r.UniqueOrders)
	fmt.Printf("duplicates sent .............. %d\n", r.DuplicatesSent)
	fmt.Printf("tasks created ................ %d\n", r.TasksCreated)
	fmt.Printf("malformed sent / quarantined . %d / %d\n", r.MalformedSent, r.MalformedQuarantined)
	fmt.Printf("unmapped-kind warnings ....... %d\n", r.UnmappedKindWarnings)
	fmt.Printf("status updates received ...... %d\n", r.StatusUpdatesReceived)
	fmt.Printf("out-of-order detections ...... %d\n", r.OutOfOrderDetections)
	fmt.Printf("duplicate deliveries ......... %d\n", r.DuplicateDeliveries)
	fmt.Printf("events published / lost ...... %d / %d\n", r.EventsPublished, r.EventsLost)
	fmt.Printf("end-to-end latency p50/p95/max %.1f / %.1f / %.1f ms\n", r.LatencyMS.P50, r.LatencyMS.P95, r.LatencyMS.Max)
	fmt.Printf("peer silence events .......... %d (buffered %d, drained %d, gaps %d)\n",
		r.PeerSilence.SilenceEvents, r.PeerSilence.Buffered, r.PeerSilence.Drained, r.PeerSilence.Gaps)
	fmt.Printf("final states ................. %v\n", r.FinalStates)
	fmt.Printf("run duration ................. %.2fs\n", r.DurationSeconds)
	fmt.Println()
	fmt.Println("reconciliation warning counts:")
	if len(r.Reconciliation.Warnings) == 0 {
		fmt.Println("  (none)")
	}
	for _, warning := range r.Reconciliation.Warnings {
		fmt.Printf("  %-24s %d\n", warning.Code, warning.Count)
	}
	fmt.Println()
	fmt.Println("per-order final state:")
	orders := make([]string, 0, len(r.PerOrderFinalState))
	for order := range r.PerOrderFinalState {
		orders = append(orders, order)
	}
	sort.Strings(orders)
	for _, order := range orders {
		fmt.Printf("  %s %s\n", order, r.PerOrderFinalState[order])
	}
}
