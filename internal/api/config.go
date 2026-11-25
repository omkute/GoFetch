package api

import (
	"fmt"
	"time"

	"github.com/omkute/gofetch/internal/classifier"
)

// DaemonConfig holds the configuration defaults applied to daemon downloads
// when a request leaves the corresponding field unset. A zero value keeps the
// built-in engine defaults, so callers that do not configure the daemon see the
// same behavior as before configuration was wired in.
type DaemonConfig struct {
	DefaultMaxChunks     int
	DefaultDestDir       string
	DefaultAutoClassify  *bool
	CategoryPaths        map[string]string
	UserAgent            string
	ConnectionTimeoutSec int
	EnableWebGUI         bool
}

// SetDaemonConfig installs the daemon's configuration defaults. Category path
// overrides are validated up front; an unsafe mapping is rejected with a clear
// error and never reaches the classifier (and therefore cannot create a
// directory outside the download root). A positive connection timeout is
// applied as the TCP dial timeout of a fresh streaming HTTP client via
// NewStreamingClientWithDialTimeout; Client.Timeout stays at zero.
func (s *Server) SetDaemonConfig(cfg DaemonConfig) error {
	if err := classifier.ValidateCategoryPaths(cfg.CategoryPaths); err != nil {
		return fmt.Errorf("invalid category_paths: %w", err)
	}

	stored := cfg
	if len(cfg.CategoryPaths) > 0 {
		stored.CategoryPaths = make(map[string]string, len(cfg.CategoryPaths))
		for category, folder := range cfg.CategoryPaths {
			stored.CategoryPaths[category] = folder
		}
	}
	if cfg.DefaultAutoClassify != nil {
		value := *cfg.DefaultAutoClassify
		stored.DefaultAutoClassify = &value
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = stored
	if cfg.ConnectionTimeoutSec > 0 {
		s.httpClient = NewStreamingClientWithDialTimeout(time.Duration(cfg.ConnectionTimeoutSec) * time.Second)
	}
	return nil
}

// daemonConfig snapshots the installed configuration defaults. The returned
// map is owned by the server and must not be modified by callers.
func (s *Server) daemonConfig() DaemonConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config
}
