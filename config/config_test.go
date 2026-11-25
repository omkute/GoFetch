package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestLoadConfig_UnknownKeysRejected(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")

	cases := []struct {
		name       string
		yaml       string
		unknownKey string
	}{
		{
			name:       "top-level section",
			yaml:       "general:\n  download_dir: /tmp\n",
			unknownKey: "general",
		},
		{
			name:       "nested key",
			yaml:       "concurrency:\n  chunks_per_download: 4\n  default_chunks_per_task: 8\n",
			unknownKey: "default_chunks_per_task",
		},
		{
			name:       "storage key",
			yaml:       "storage:\n  fsync_on_checkpoint: true\n",
			unknownKey: "fsync_on_checkpoint",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(configPath, []byte(tc.yaml), 0600); err != nil {
				t.Fatalf("failed writing config: %v", err)
			}
			_, err := LoadConfig(configPath)
			if err == nil {
				t.Fatal("expected an error for an unknown config key, got nil")
			}
			if !strings.Contains(err.Error(), configPath) {
				t.Errorf("error %q does not name the config file %q", err, configPath)
			}
			if !strings.Contains(err.Error(), tc.unknownKey) {
				t.Errorf("error %q does not name the unknown key %q", err, tc.unknownKey)
			}
		})
	}
}

func TestLoadConfig_FullSchemaRoundTrip(t *testing.T) {
	const fullSchema = `
concurrency:
  max_concurrent_downloads: 6
  chunks_per_download: 12
storage:
  default_download_dir: "/tmp/gofetch-downloads"
  auto_classify: false
  category_paths:
    Videos: "Media/Videos"
    Documents: "Papers"
network:
  user_agent: "GoFetch-Test/1.0"
  connection_timeout_sec: 45
  global_rate_limit_bps: 1048576
daemon:
  listen_addr: "127.0.0.1:12799"
  enable_web_gui: false
  task_db_path: "/tmp/gofetch-tasks.db"
`

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(fullSchema), 0600); err != nil {
		t.Fatalf("failed writing config: %v", err)
	}

	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("failed loading full schema: %v", err)
	}
	if loaded.Concurrency.MaxConcurrentDownloads != 6 || loaded.Concurrency.ChunksPerDownload != 12 {
		t.Errorf("unexpected concurrency: %+v", loaded.Concurrency)
	}
	if loaded.Storage.DefaultDownloadDir != "/tmp/gofetch-downloads" || loaded.Storage.AutoClassify {
		t.Errorf("unexpected storage: %+v", loaded.Storage)
	}
	if loaded.Storage.CategoryPaths["Videos"] != "Media/Videos" || loaded.Storage.CategoryPaths["Documents"] != "Papers" {
		t.Errorf("unexpected category paths: %+v", loaded.Storage.CategoryPaths)
	}
	if loaded.Network.UserAgent != "GoFetch-Test/1.0" || loaded.Network.ConnectionTimeout != 45 || loaded.Network.GlobalRateLimitBps != 1048576 {
		t.Errorf("unexpected network: %+v", loaded.Network)
	}
	if loaded.Daemon.ListenAddr != "127.0.0.1:12799" || loaded.Daemon.EnableWebGUI || loaded.Daemon.TaskDBPath != "/tmp/gofetch-tasks.db" {
		t.Errorf("unexpected daemon: %+v", loaded.Daemon)
	}

	savedPath := filepath.Join(t.TempDir(), "saved.yaml")
	if err := Save(loaded, savedPath); err != nil {
		t.Fatalf("failed saving config: %v", err)
	}
	reloaded, err := LoadConfig(savedPath)
	if err != nil {
		t.Fatalf("failed reloading saved config: %v", err)
	}
	if !reflect.DeepEqual(loaded, reloaded) {
		t.Fatalf("round-trip mismatch:\nfirst:  %+v\nsecond: %+v", loaded, reloaded)
	}
}

func TestLoadConfig_EmptyFileUsesDefaults(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("# intentionally empty\n"), 0600); err != nil {
		t.Fatalf("failed writing config: %v", err)
	}

	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("empty config should fall back to defaults, got: %v", err)
	}
	if !reflect.DeepEqual(loaded, DefaultConfig()) {
		t.Fatalf("empty config did not preserve defaults: %+v", loaded)
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
