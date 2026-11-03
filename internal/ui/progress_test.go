package ui_test

import (
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/ui"
)

func TestProgressTracker_AtomicCounting(t *testing.T) {
	const (
		totalBytes = int64(10000)
		numChunks  = 4
		perChunk   = 2500
	)

	tracker := ui.NewProgressTracker(totalBytes, numChunks)

	var wg sync.WaitGroup
	for c := 0; c < numChunks; c++ {
		wg.Add(1)
		chunkIdx := c
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				tracker.AddChunkBytes(chunkIdx, 100)
			}
		}()
	}
	wg.Wait()

	if tracker.TotalDownloaded() != totalBytes {
		t.Fatalf("expected total downloaded %d, got %d", totalBytes, tracker.TotalDownloaded())
	}

	if tracker.Percentage() != 100.0 {
		t.Fatalf("expected 100%%, got %.2f%%", tracker.Percentage())
	}

	for i := 0; i < numChunks; i++ {
		if tracker.ChunkBytes(i) != int64(perChunk) {
			t.Errorf("chunk %d expected %d bytes, got %d", i, perChunk, tracker.ChunkBytes(i))
		}
	}
}

func TestProgressTracker_ThroughputAndETA(t *testing.T) {
	const total = int64(10 * 1024 * 1024) // 10 MB
	tracker := ui.NewProgressTracker(total, 2)

	// Simulate half downloaded
	tracker.AddChunkBytes(0, 5*1024*1024)

	pct := tracker.Percentage()
	if pct < 49.9 || pct > 50.1 {
		t.Fatalf("expected ~50%%, got %.2f%%", pct)
	}

	speed := tracker.SpeedBytesPerSec()
	// Initial speed check (non-negative)
	if speed < 0 {
		t.Fatalf("speed should not be negative: %f", speed)
	}

	eta := tracker.ETA()
	if eta < 0 {
		t.Fatalf("ETA should not be negative: %v", eta)
	}

	// Complete transfer
	tracker.AddChunkBytes(1, 5*1024*1024)
	if tracker.Percentage() != 100.0 {
		t.Fatalf("expected 100%%, got %.2f%%", tracker.Percentage())
	}
	if tracker.ETA() != 0 {
		t.Fatalf("expected 0s ETA when completed, got %v", tracker.ETA())
	}
}

func TestFormatBytes(t *testing.T) {
	testCases := []struct {
		bytes    int64
		expected string
	}{
		{500, "500 B"},
		{1024, "1.00 KB"},
		{1048576, "1.00 MB"},
		{1073741824, "1.00 GB"},
		{1572864, "1.50 MB"},
	}

	for _, tc := range testCases {
		actual := ui.FormatBytes(tc.bytes)
		if actual != tc.expected {
			t.Errorf("FormatBytes(%d) = %q; expected %q", tc.bytes, actual, tc.expected)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	testCases := []struct {
		d        time.Duration
		expected string
	}{
		{5 * time.Second, "5s"},
		{65 * time.Second, "1m 5s"},
		{3665 * time.Second, "1h 1m"},
		{0, "0s"},
	}

	for _, tc := range testCases {
		actual := ui.FormatDuration(tc.d)
		if actual != tc.expected {
			t.Errorf("FormatDuration(%v) = %q; expected %q", tc.d, actual, tc.expected)
		}
	}
}
