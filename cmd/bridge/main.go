// Command bridge runs the WarehouseTaskBridge service: an integration and
// orchestration middleware layer between a customer WMS and a robotic fleet.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"

	"github.com/local/warehousetaskbridge/internal/config"
	"github.com/local/warehousetaskbridge/internal/ingest"
	"github.com/local/warehousetaskbridge/internal/metrics"
	"github.com/local/warehousetaskbridge/internal/peer"
	"github.com/local/warehousetaskbridge/internal/store"
	"github.com/local/warehousetaskbridge/internal/translate"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("bridge exited", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	db, err := store.Open(startCtx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(startCtx); err != nil {
		return err
	}
	logger.Info("database ready", "dsn", cfg.PostgresDSN)

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("warehousetaskbridge"), nats.Timeout(10*time.Second))
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		return fmt.Errorf("jetstream context: %w", err)
	}
	logger.Info("nats connected", "url", cfg.NATSURL)

	counters := metrics.New()
	report := translate.NewReconciliationReport()

	acceptor := ingest.NewAcceptor(db, counters, report, logger)
	natsIngest, err := ingest.NewNATSIngester(nc, js, acceptor, logger)
	if err != nil {
		return err
	}
	if err := natsIngest.EnsureStream(); err != nil {
		return err
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
		return err
	}

	handler := ingest.NewHTTPHandler(ingest.HTTPConfig{
		Store:    db,
		Acceptor: acceptor,
		Report:   report,
		Metrics:  counters,
		Logger:   logger,
	})
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	group, gctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		logger.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	group.Go(func() error {
		<-gctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		return srv.Shutdown(shutdownCtx)
	})
	group.Go(func() error { return natsIngest.Run(gctx) })
	group.Go(func() error { return heartbeat.Run(gctx) })
	group.Go(func() error {
		watcher.Run(gctx, cfg.PeerSilenceTimeout/4)
		return nil
	})

	err = group.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("bridge stopped")
	return nil
}
