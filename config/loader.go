package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Default network and concurrency constants.
const (
	DefaultListenAddr        = "127.0.0.1:12701"
	DefaultMaxConcurrent     = 3
	DefaultChunksPerDownload = 4
	DefaultTimeoutSeconds    = 30
	DefaultUserAgent         = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

// DefaultConfig returns a Config populated with standard defaults.
func DefaultConfig() *Config {
	home, _ := os.UserHomeDir()
	downloadDir := "."
	taskDB := ".gofetch/daemon_tasks.db"
	if home != "" {
		downloadDir = filepath.Join(home, "Downloads")
		taskDB = filepath.Join(home, ".gofetch", "daemon_tasks.db")
	}

	return &Config{
		Concurrency: ConcurrencyConfig{
			MaxConcurrentDownloads: DefaultMaxConcurrent,
			ChunksPerDownload:      DefaultChunksPerDownload,
		},
		Storage: StorageConfig{
			DefaultDownloadDir: downloadDir,
			AutoClassify:       true,
			CategoryPaths:      make(map[string]string),
		},
		Network: NetworkConfig{
			UserAgent:         DefaultUserAgent,
			ConnectionTimeout: DefaultTimeoutSeconds,
		},
		Daemon: DaemonConfig{
			ListenAddr:   DefaultListenAddr,
			EnableWebGUI: true,
			TaskDBPath:   taskDB,
		},
	}
}

// LoadConfig loads configuration from path or default search paths.
// If explicitPath is provided, it must exist.
// If explicitPath is empty, it attempts $GOFETCH_CONFIG, ~/.gofetch/config.yaml, ./config.yaml,
// falling back to DefaultConfig() if no file is found.
func LoadConfig(explicitPath string) (*Config, error) {
	cfg := DefaultConfig()

	targetPath := explicitPath
	if targetPath == "" {
		targetPath = os.Getenv("GOFETCH_CONFIG")
	}
	if targetPath == "" {
		home, _ := os.UserHomeDir()
		if home != "" {
			standardPath := filepath.Join(home, ".gofetch", "config.yaml")
			if _, err := os.Stat(standardPath); err == nil {
				targetPath = standardPath
			}
		}
	}
	if targetPath == "" {
		if _, err := os.Stat("./config.yaml"); err == nil {
			targetPath = "./config.yaml"
		}
	}

	if targetPath == "" {
		return cfg, nil // No config file found; use defaults
	}

	data, err := os.ReadFile(targetPath)
	if err != nil {
		if explicitPath != "" {
			return nil, fmt.Errorf("failed reading specified config file %q: %w", explicitPath, err)
		}
		return cfg, nil
	}

	// Strict decoding rejects unknown or misspelled keys instead of silently
	// ignoring them, so a typo in the config file cannot disable a setting.
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("invalid config file %q: %w", targetPath, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config in %q: %w", targetPath, err)
	}

	return cfg, nil
}

// Validate checks sanity of config values and applies minimums.
func (c *Config) Validate() error {
	if c.Concurrency.MaxConcurrentDownloads <= 0 {
		c.Concurrency.MaxConcurrentDownloads = DefaultMaxConcurrent
	}
	if c.Concurrency.ChunksPerDownload <= 0 {
		c.Concurrency.ChunksPerDownload = DefaultChunksPerDownload
	}
	if c.Network.ConnectionTimeout <= 0 {
		c.Network.ConnectionTimeout = DefaultTimeoutSeconds
	}
	if c.Network.UserAgent == "" {
		c.Network.UserAgent = DefaultUserAgent
	}
	if c.Daemon.ListenAddr == "" {
		c.Daemon.ListenAddr = DefaultListenAddr
	}
	if c.Network.GlobalRateLimitBps < 0 {
		return errors.New("global_rate_limit_bps cannot be negative")
	}
	return nil
}

// Save writes configuration as formatted YAML to path.
func Save(cfg *Config, path string) error {
	if cfg == nil {
		return errors.New("cannot save nil config")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("failed creating directory %q: %w", dir, err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed serializing config: %w", err)
	}

	return os.WriteFile(path, data, 0600)
}
