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
	DefaultSimSpeedFactor     = 20.0
	DefaultSimFailureRate     = 0.05
	DefaultSimSeed            = 42
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
	// SimSpeedFactor divides simulated handling time; higher is faster.
	SimSpeedFactor float64
	// SimFailureRate is the probability a simulated task fails and requeues.
	SimFailureRate float64
	// SimSeed makes simulated failure decisions reproducible.
	SimSeed int64
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
		SimSpeedFactor:     DefaultSimSpeedFactor,
		SimFailureRate:     DefaultSimFailureRate,
		SimSeed:            DefaultSimSeed,
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
	if cfg.SimSpeedFactor, err = envFloat("SIM_SPEED_FACTOR", DefaultSimSpeedFactor); err != nil {
		return Config{}, err
	}
	if cfg.SimFailureRate, err = envFloat("SIM_FAILURE_RATE", DefaultSimFailureRate); err != nil {
		return Config{}, err
	}
	seed, err := envInt("SIM_SEED", DefaultSimSeed)
	if err != nil {
		return Config{}, err
	}
	cfg.SimSeed = int64(seed)
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
	if c.SimSpeedFactor <= 0 {
		return fmt.Errorf("config: SIM_SPEED_FACTOR must be positive, got %f", c.SimSpeedFactor)
	}
	if c.SimFailureRate < 0 || c.SimFailureRate > 1 {
		return fmt.Errorf("config: SIM_FAILURE_RATE must be within [0,1], got %f", c.SimFailureRate)
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

func envFloat(key string, def float64) (float64, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("config: malformed float %s=%q: %w", key, raw, err)
	}
	return f, nil
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
