package engine

import (
	"context"
	"strings"
)

// ConcurrencyLimiter bounds maximum concurrent operations across tasks and chunk workers.
type ConcurrencyLimiter interface {
	Acquire(ctx context.Context) error
	Release()
}

// DownloadOptions configures the execution parameters of a download coordinator.
type DownloadOptions struct {
	DestDir      string
	JobID        string
	MaxWorkers   int
	Sequential   bool
	Semaphore    ConcurrencyLimiter
	RateLimiter  *RateLimiter
	ProgressFn   func(chunkIndex int, bytesRead int)
	Checksum     string
	ChecksumAlgo string
	// UserAgent overrides the User-Agent header for probe and chunk requests.
	// An empty value falls back to DefaultUserAgent.
	UserAgent string
}

// ProbeOptions configures optional behavior of a resource probe.
type ProbeOptions struct {
	// UserAgent overrides the User-Agent header; empty means DefaultUserAgent.
	UserAgent string
}

// resolveUserAgent returns the configured user agent or the engine default.
func resolveUserAgent(userAgent string) string {
	if strings.TrimSpace(userAgent) == "" {
		return DefaultUserAgent
	}
	return userAgent
}
