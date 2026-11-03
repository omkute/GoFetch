package ui

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"
)

// ProgressTracker maintains lock-free, atomic progress and throughput counters.
type ProgressTracker struct {
	totalBytes      int64
	numChunks       int
	downloadedTotal atomic.Int64
	chunkBytes      []atomic.Int64
	startTime       time.Time
}

// NewProgressTracker initializes a new progress tracker for totalBytes across numChunks.
func NewProgressTracker(totalBytes int64, numChunks int) *ProgressTracker {
	if numChunks <= 0 {
		numChunks = 1
	}

	return &ProgressTracker{
		totalBytes: totalBytes,
		numChunks:  numChunks,
		chunkBytes: make([]atomic.Int64, numChunks),
		startTime:  time.Now(),
	}
}

// AddChunkBytes atomically increments the downloaded byte count for a specific chunk.
func (t *ProgressTracker) AddChunkBytes(chunkIdx int, n int64) {
	if n <= 0 {
		return
	}
	t.downloadedTotal.Add(n)
	if chunkIdx >= 0 && chunkIdx < len(t.chunkBytes) {
		t.chunkBytes[chunkIdx].Add(n)
	}
}

// TotalDownloaded returns the cumulative count of bytes downloaded so far.
func (t *ProgressTracker) TotalDownloaded() int64 {
	return t.downloadedTotal.Load()
}

// TotalBytes returns the total target payload size in bytes.
func (t *ProgressTracker) TotalBytes() int64 {
	return t.totalBytes
}

// ChunkBytes returns the byte count downloaded for chunkIdx.
func (t *ProgressTracker) ChunkBytes(chunkIdx int) int64 {
	if chunkIdx < 0 || chunkIdx >= len(t.chunkBytes) {
		return 0
	}
	return t.chunkBytes[chunkIdx].Load()
}

// NumChunks returns the total partitioned chunk count.
func (t *ProgressTracker) NumChunks() int {
	return t.numChunks
}

// Percentage computes the overall download completion percentage (0.0 to 100.0).
func (t *ProgressTracker) Percentage() float64 {
	if t.totalBytes <= 0 {
		return 0.0
	}
	pct := (float64(t.TotalDownloaded()) / float64(t.totalBytes)) * 100.0
	if pct > 100.0 {
		return 100.0
	}
	return pct
}

// SpeedBytesPerSec computes current transfer throughput in bytes per second.
func (t *ProgressTracker) SpeedBytesPerSec() float64 {
	elapsed := time.Since(t.startTime).Seconds()
	if elapsed <= 0.001 {
		return 0.0
	}
	return float64(t.TotalDownloaded()) / elapsed
}

// ETA calculates remaining time until download completion.
func (t *ProgressTracker) ETA() time.Duration {
	remaining := t.totalBytes - t.TotalDownloaded()
	if remaining <= 0 {
		return 0
	}

	speed := t.SpeedBytesPerSec()
	if speed <= 10.0 {
		return 0
	}

	seconds := float64(remaining) / speed
	if seconds > 86400*365 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0
	}

	return time.Duration(seconds) * time.Second
}

// FormatBytes formats a byte count into a readable string (B, KB, MB, GB).
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// FormatDuration formats duration into concise human-readable time strings.
func FormatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%01dh %01dm", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%01dm %01ds", m, s)
	}
	return fmt.Sprintf("%01ds", s)
}

// RenderProgressBar draws an ASCII progress bar of specified width.
func RenderProgressBar(percent float64, width int) string {
	if width <= 0 {
		width = 20
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	filled := int((percent / 100.0) * float64(width))
	if filled > width {
		filled = width
	}
	empty := width - filled

	return strings.Repeat("█", filled) + strings.Repeat("░", empty)
}
