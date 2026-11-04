package engine

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

func TestParseRateLimit(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasErr   bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"none", 0, false},
		{"unlimited", 0, false},
		{"500B", 500, false},
		{"1024", 1024, false},
		{"1KB", 1024, false},
		{"2k", 2048, false},
		{"1MB", 1024 * 1024, false},
		{"5M", 5 * 1024 * 1024, false},
		{"2.5MB/s", int64(2.5 * 1024 * 1024), false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"invalid", 0, true},
		{"-5MB", 0, true},
	}

	for _, tc := range tests {
		got, err := ParseRateLimit(tc.input)
		if tc.hasErr && err == nil {
			t.Errorf("ParseRateLimit(%q) expected error, got nil", tc.input)
		}
		if !tc.hasErr && err != nil {
			t.Errorf("ParseRateLimit(%q) unexpected error: %v", tc.input, err)
		}
		if got != tc.expected {
			t.Errorf("ParseRateLimit(%q) = %d, expected %d", tc.input, got, tc.expected)
		}
	}
}

func TestRateLimiterThrottling(t *testing.T) {
	// Set limit to 100 KB/s
	limit := int64(100 * 1024)
	limiter := NewRateLimiter(limit)

	// Payload of 50 KB
	payloadSize := 50 * 1024
	data := bytes.Repeat([]byte("a"), payloadSize)
	reader := bytes.NewReader(data)

	ctx := context.Background()
	limitedReader := NewLimitedReader(ctx, reader, limiter)

	// Drain reader
	buf := make([]byte, 16*1024)
	totalRead := 0
	for {
		n, err := limitedReader.Read(buf)
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
	}

	if totalRead != payloadSize {
		t.Fatalf("expected total read %d, got %d", payloadSize, totalRead)
	}

	// Now set rate limit to 0 (uncapped) and verify read takes virtually 0 time
	limiter.SetLimit(0)
	if limiter.Limit() != 0 {
		t.Errorf("expected limit 0, got %d", limiter.Limit())
	}
}

func TestRateLimiterContextCancellation(t *testing.T) {
	limiter := NewRateLimiter(1024) // 1 KB/s
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context after 30ms
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	err := limiter.Wait(ctx, 100*1024) // Request 100KB which would take 100s
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRateLimiterConcurrentWorkers(t *testing.T) {
	limiter := NewRateLimiter(10 * 1024 * 1024) // 10MB/s

	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := bytes.Repeat([]byte("x"), 16*1024)
			lr := NewLimitedReader(ctx, bytes.NewReader(payload), limiter)
			buf := make([]byte, 4096)
			for {
				n, err := lr.Read(buf)
				if err == io.EOF {
					break
				}
				if err != nil && err != io.EOF {
					t.Errorf("concurrent read error: %v", err)
					return
				}
				if n == 0 {
					break
				}
			}
		}()
	}

	wg.Wait()
}
