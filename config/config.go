package config

// Config defines the user and daemon runtime configuration.
type Config struct {
	Concurrency ConcurrencyConfig `yaml:"concurrency"`
	Storage     StorageConfig     `yaml:"storage"`
	Network     NetworkConfig     `yaml:"network"`
	Daemon      DaemonConfig      `yaml:"daemon"`
}

// ConcurrencyConfig controls worker parallelism and simultaneous downloads.
type ConcurrencyConfig struct {
	MaxConcurrentDownloads int `yaml:"max_concurrent_downloads"`
	ChunksPerDownload      int `yaml:"chunks_per_download"`
}

// StorageConfig controls download paths and media classification routing.
type StorageConfig struct {
	DefaultDownloadDir string            `yaml:"default_download_dir"`
	AutoClassify       bool              `yaml:"auto_classify"`
	CategoryPaths      map[string]string `yaml:"category_paths"`
}

// NetworkConfig controls HTTP transport and throughput throttling.
type NetworkConfig struct {
	UserAgent          string `yaml:"user_agent"`
	ConnectionTimeout  int    `yaml:"connection_timeout_sec"`
	GlobalRateLimitBps int64  `yaml:"global_rate_limit_bps"`
}

// DaemonConfig controls loopback IPC, embedded GUI, and task persistence.
type DaemonConfig struct {
	ListenAddr   string `yaml:"listen_addr"`
	EnableWebGUI bool   `yaml:"enable_web_gui"`
	TaskDBPath   string `yaml:"task_db_path"`
}
