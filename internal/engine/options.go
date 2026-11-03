package engine

import (
	"context"
)

// ConcurrencyLimiter bounds maximum concurrent operations across tasks and chunk workers.
type ConcurrencyLimiter interface {
	Acquire(ctx context.Context) error
	Release()
}

// DownloadOptions configures the execution parameters of a download coordinator.
type DownloadOptions struct {
	DestDir    string
	JobID      string
	MaxWorkers int
	Sequential bool
	Semaphore  ConcurrencyLimiter
	ProgressFn func(chunkIndex int, bytesRead int)
}
