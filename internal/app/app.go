// Package app wires the bridge's config, storage, NATS transports, HTTP API,
// scheduler, simulator, and feedback publisher into one runnable service.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/local/warehousetaskbridge/internal/config"
	"github.com/local/warehousetaskbridge/internal/feedback"
	"github.com/local/warehousetaskbridge/internal/ingest"
	"github.com/local/warehousetaskbridge/internal/metrics"
	"github.com/local/warehousetaskbridge/internal/peer"
	"github.com/local/warehousetaskbridge/internal/sim"
	"github.com/local/warehousetaskbridge/internal/store"
	"github.com/local/warehousetaskbridge/internal/translate"
)

// Options customizes app startup, primarily for tests.
type Options struct {
	// Logger is optional and defaults to slog.Default.
	Logger *slog.Logger
	// Listener lets a test bind :0 and learn the real address. When nil the
	// configured HTTP address is used.
	Listener net.Listener
	// SimSeed overrides the configured simulator seed when non-zero.
	SimSeed int64
	// SimTickInterval overrides the default 100ms simulator tick.
	SimTickInterval time.Duration
}

// App is a fully wired, running bridge instance.
type App struct {
	cfg       config.Config
	logger    *slog.Logger
	counters  *metrics.Counters
	report    *translate.ReconciliationReport
	db        *store.Store
	nc        *nats.Conn
	publisher *feedback.Publisher
	srv       *http.Server
	listener  net.Listener

	cancel  context.CancelFunc
	group   sync.WaitGroup
	closeMu sync.Once
	errCh   chan error
}

// Start connects dependencies, applies migrations, starts every background
// component, and begins serving HTTP.
func Start(ctx context.Context, cfg config.Config, opts Options) (*App, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	startCtx, startCancel := context.WithTimeout(ctx, 60*time.Second)
	defer startCancel()

	db, err := store.Open(startCtx, cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}
	if err := db.Migrate(startCtx); err != nil {
		db.Close()
		return nil, err
	}

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("warehousetaskbridge"), nats.Timeout(10*time.Second))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("app: connect nats: %w", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		nc.Close()
		db.Close()
		return nil, fmt.Errorf("app: jetstream context: %w", err)
	}

	counters := metrics.New()
	report := translate.NewReconciliationReport()

	acceptor := ingest.NewAcceptor(db, counters, report, logger)
	natsIngest, err := ingest.NewNATSIngester(nc, js, acceptor, logger)
	if err != nil {
		nc.Close()
		db.Close()
		return nil, err
	}
	if err := natsIngest.EnsureStream(); err != nil {
		nc.Close()
		db.Close()
		return nil, err
	}
	acceptor.SetQuarantineSink(natsIngest.QuarantineMirror())

	watcher := peer.NewWatcher(peer.WatcherConfig{
		PeerID:   peer.DefaultPeerID,
		Timeout:  cfg.PeerSilenceTimeout,
		Counters: counters,
		Logger:   logger,
	})
	heartbeat, err := peer.NewHeartbeat(peer.HeartbeatConfig{
		Conn:     nc,
		Store:    db,
		Watcher:  watcher,
		Interval: cfg.HeartbeatInterval,
		PeerID:   peer.DefaultPeerID,
		Counters: counters,
		Logger:   logger,
	})
	if err != nil {
		nc.Close()
		db.Close()
		return nil, err
	}

	if err := feedback.EnsureStatusStream(js); err != nil {
		nc.Close()
		db.Close()
		return nil, err
	}
	sender, err := feedback.NewNATSSender(js)
	if err != nil {
		nc.Close()
		db.Close()
		return nil, err
	}
	publisher, err := feedback.NewPublisher(feedback.Config{
		Outbox:       db,
		Sender:       sender,
		Gate:         watcher,
		Metrics:      counters,
		Logger:       logger,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		nc.Close()
		db.Close()
		return nil, err
	}

	seed := cfg.SimSeed
	if opts.SimSeed != 0 {
		seed = opts.SimSeed
	}
	tick := opts.SimTickInterval
	if tick <= 0 {
		tick = 100 * time.Millisecond
	}
	engine, err := sim.NewEngine(db, sim.Config{
		BotCount:     cfg.BotCount,
		SpeedFactor:  cfg.SimSpeedFactor,
		FailureRate:  cfg.SimFailureRate,
		Seed:         seed,
		TickInterval: tick,
		Metrics:      counters,
		Logger:       logger,
	})
	if err != nil {
		nc.Close()
		db.Close()
		return nil, err
	}
	if err := engine.EnsureFleet(startCtx); err != nil {
		nc.Close()
		db.Close()
		return nil, err
	}
	if _, err := engine.Recover(startCtx); err != nil {
		nc.Close()
		db.Close()
		return nil, err
	}

	listener := opts.Listener
	if listener == nil {
		listener, err = net.Listen("tcp", cfg.HTTPAddr)
		if err != nil {
			nc.Close()
			db.Close()
			return nil, fmt.Errorf("app: listen %s: %w", cfg.HTTPAddr, err)
		}
	}

	handler := ingest.NewHTTPHandler(ingest.HTTPConfig{
		Store:    db,
		Acceptor: acceptor,
		Report:   report,
		Metrics:  counters,
		Logger:   logger,
	})
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	runCtx, cancel := context.WithCancel(ctx)
	a := &App{
		cfg: cfg, logger: logger, counters: counters, report: report,
		db: db, nc: nc, publisher: publisher, srv: srv, listener: listener,
		cancel: cancel, errCh: make(chan error, 8),
	}

	a.run("http", func() error {
		logger.Info("http listening", "addr", listener.Addr().String())
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	a.run("nats-ingest", func() error { return natsIngest.Run(runCtx) })
	a.run("heartbeat", func() error { return heartbeat.Run(runCtx) })
	a.run("publisher", func() error { return publisher.Run(runCtx) })
	a.run("simulator", func() error { return engine.Run(runCtx) })
	a.run("peer-watcher", func() error {
		watcher.Run(runCtx, cfg.PeerSilenceTimeout/4)
		return nil
	})
	return a, nil
}

func (a *App) run(name string, fn func() error) {
	a.group.Add(1)
	go func() {
		defer a.group.Done()
		if err := fn(); err != nil {
			select {
			case a.errCh <- fmt.Errorf("%s: %w", name, err):
			default:
			}
			a.logger.Error("component stopped with error", "component", name, "error", err)
		}
	}()
}

// Addr returns the bound HTTP address, for example 127.0.0.1:53211.
func (a *App) Addr() string {
	return a.listener.Addr().String()
}

// URL returns the base HTTP URL.
func (a *App) URL() string {
	return "http://" + a.Addr()
}

// DB exposes the store for inspection in integration tests.
func (a *App) DB() *store.Store { return a.db }

// Metrics exposes the counter set.
func (a *App) Metrics() *metrics.Counters { return a.counters }

// Publisher exposes the feedback publisher for its buffer statistics.
func (a *App) Publisher() *feedback.Publisher { return a.publisher }

// Close performs a graceful shutdown and releases every dependency.
func (a *App) Close() error {
	var closeErr error
	a.closeMu.Do(func() {
		a.cancel()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := a.srv.Shutdown(shutdownCtx); err != nil {
			closeErr = err
		}
		done := make(chan struct{})
		go func() {
			a.group.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			if closeErr == nil {
				closeErr = shutdownCtx.Err()
			}
		}
		a.nc.Close()
		a.db.Close()
	})
	return closeErr
}

// Errors returns a channel of fatal component errors observed so far.
func (a *App) Errors() <-chan error { return a.errCh }
