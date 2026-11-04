package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("expected default config, got nil")
	}

	if cfg.Concurrency.MaxConcurrentDownloads != DefaultMaxConcurrent {
		t.Errorf("expected max concurrent %d, got %d", DefaultMaxConcurrent, cfg.Concurrency.MaxConcurrentDownloads)
	}
	if cfg.Concurrency.ChunksPerDownload != DefaultChunksPerDownload {
		t.Errorf("expected chunks %d, got %d", DefaultChunksPerDownload, cfg.Concurrency.ChunksPerDownload)
	}
	if !cfg.Storage.AutoClassify {
		t.Error("expected auto classify to default to true")
	}
	if cfg.Daemon.ListenAddr != DefaultListenAddr {
		t.Errorf("expected listen addr %s, got %s", DefaultListenAddr, cfg.Daemon.ListenAddr)
	}
}

func TestLoadConfig_Roundtrip(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	original := DefaultConfig()
	original.Concurrency.MaxConcurrentDownloads = 8
	original.Concurrency.ChunksPerDownload = 16
	original.Network.GlobalRateLimitBps = 10 * 1024 * 1024
	original.Storage.CategoryPaths["Videos"] = "/custom/videos"

	if err := Save(original, configPath); err != nil {
		t.Fatalf("failed saving config: %v", err)
	}

	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed loading saved config: %v", err)
	}

	if loaded.Concurrency.MaxConcurrentDownloads != 8 {
		t.Errorf("expected max concurrent 8, got %d", loaded.Concurrency.MaxConcurrentDownloads)
	}
	if loaded.Concurrency.ChunksPerDownload != 16 {
		t.Errorf("expected chunks 16, got %d", loaded.Concurrency.ChunksPerDownload)
	}
	if loaded.Network.GlobalRateLimitBps != 10*1024*1024 {
		t.Errorf("expected rate limit 10MB, got %d", loaded.Network.GlobalRateLimitBps)
	}
	if loaded.Storage.CategoryPaths["Videos"] != "/custom/videos" {
		t.Errorf("expected custom video path, got %q", loaded.Storage.CategoryPaths["Videos"])
	}
}

func TestLoadConfig_Validation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Network.GlobalRateLimitBps = -100

	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error on negative rate limit, got nil")
	}
}

func TestLoadConfig_ExplicitFileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/to/config.yaml")
	if err == nil {
		t.Error("expected error when explicit config does not exist, got nil")
	}
}

func TestLoadConfig_EnvVar(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "env_config.yaml")

	cfg := DefaultConfig()
	cfg.Concurrency.MaxConcurrentDownloads = 12
	_ = Save(cfg, configPath)

	_ = os.Setenv("GOFETCH_CONFIG", configPath)
	defer func() { _ = os.Unsetenv("GOFETCH_CONFIG") }()

	loaded, err := LoadConfig("")
	if err != nil {
		t.Fatalf("failed loading config from env: %v", err)
	}

	if loaded.Concurrency.MaxConcurrentDownloads != 12 {
		t.Errorf("expected 12 from env config, got %d", loaded.Concurrency.MaxConcurrentDownloads)
	}
}
