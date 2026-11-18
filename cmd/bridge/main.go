// Command bridge runs the WarehouseTaskBridge service: an integration and
// orchestration middleware layer between a customer WMS and a robotic fleet.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/local/warehousetaskbridge/internal/app"
	"github.com/local/warehousetaskbridge/internal/config"
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

	service, err := app.Start(ctx, cfg, app.Options{Logger: logger})
	if err != nil {
		return err
	}
	logger.Info("bridge started",
		"http", service.Addr(),
		"bots", cfg.BotCount,
		"heartbeat_interval", cfg.HeartbeatInterval.String(),
		"peer_silence_timeout", cfg.PeerSilenceTimeout.String())

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-service.Errors():
		logger.Error("component failure", "error", err)
	}
	if err := service.Close(); err != nil {
		return err
	}
	logger.Info("bridge stopped")
	return nil
}
