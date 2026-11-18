// Package config loads and validates WarehouseTaskBridge configuration from
// environment variables, applying documented defaults.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Default values used when the corresponding environment variable is unset.
const (
	DefaultHTTPAddr           = ":8080"
	DefaultNATSURL            = "nats://127.0.0.1:4222"
	DefaultPostgresDSN        = "postgres://bridge:bridge@127.0.0.1:55432/bridge?sslmode=disable"
	DefaultHeartbeatInterval  = 2 * time.Second
	DefaultPeerSilenceTimeout = 6 * time.Second
	DefaultBotCount           = 6
)

// Config is the fully resolved runtime configuration.
type Config struct {
	// HTTPAddr is the listen address for the REST API, for example ":8080".
	HTTPAddr string
	// NATSURL is the NATS server URL.
	NATSURL string
	// PostgresDSN is the pgx connection string.
	PostgresDSN string
	// HeartbeatInterval is how often the bridge publishes bridge.heartbeat.
	HeartbeatInterval time.Duration
	// PeerSilenceTimeout is how long the WMS may be quiet before it is
	// considered SILENT and outbound status publishing is paused.
	PeerSilenceTimeout time.Duration
	// BotCount is the size of the simulated bot fleet.
	BotCount int
}

// Load reads configuration from the environment. It returns an error for
// malformed durations, non-positive bot counts, or missing required values.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:           envString("BRIDGE_HTTP_ADDR", DefaultHTTPAddr),
		NATSURL:            envString("NATS_URL", DefaultNATSURL),
		PostgresDSN:        envString("POSTGRES_DSN", DefaultPostgresDSN),
		HeartbeatInterval:  DefaultHeartbeatInterval,
		PeerSilenceTimeout: DefaultPeerSilenceTimeout,
		BotCount:           DefaultBotCount,
	}

	var err error
	if cfg.HeartbeatInterval, err = envDuration("HEARTBEAT_INTERVAL", DefaultHeartbeatInterval); err != nil {
		return Config{}, err
	}
	if cfg.PeerSilenceTimeout, err = envDuration("PEER_SILENCE_TIMEOUT", DefaultPeerSilenceTimeout); err != nil {
		return Config{}, err
	}
	if cfg.BotCount, err = envInt("BOT_COUNT", DefaultBotCount); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks that the configuration is internally consistent.
func (c Config) Validate() error {
	if c.HTTPAddr == "" {
		return fmt.Errorf("config: BRIDGE_HTTP_ADDR must not be empty")
	}
	if c.NATSURL == "" {
		return fmt.Errorf("config: NATS_URL must not be empty")
	}
	if c.PostgresDSN == "" {
		return fmt.Errorf("config: POSTGRES_DSN must not be empty")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("config: HEARTBEAT_INTERVAL must be positive, got %s", c.HeartbeatInterval)
	}
	if c.PeerSilenceTimeout <= 0 {
		return fmt.Errorf("config: PEER_SILENCE_TIMEOUT must be positive, got %s", c.PeerSilenceTimeout)
	}
	if c.PeerSilenceTimeout <= c.HeartbeatInterval {
		return fmt.Errorf("config: PEER_SILENCE_TIMEOUT (%s) must exceed HEARTBEAT_INTERVAL (%s)", c.PeerSilenceTimeout, c.HeartbeatInterval)
	}
	if c.BotCount <= 0 {
		return fmt.Errorf("config: BOT_COUNT must be positive, got %d", c.BotCount)
	}
	return nil
}

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config: malformed duration %s=%q: %w", key, raw, err)
	}
	return d, nil
}

func envInt(key string, def int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config: malformed integer %s=%q: %w", key, raw, err)
	}
	return n, nil
}
