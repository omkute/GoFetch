package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RateLimiter controls throughput in bytes per second using a token-bucket algorithm.
// A limit of <= 0 represents an uncapped (unlimited) transfer rate.
type RateLimiter struct {
	mu         sync.Mutex
	limit      int64 // bytes per second
	tokens     float64
	capacity   float64
	lastRefill time.Time
}

// NewRateLimiter creates a new RateLimiter with the specified bytes-per-second limit.
func NewRateLimiter(limitBps int64) *RateLimiter {
	rl := &RateLimiter{
		limit:      limitBps,
		lastRefill: time.Now(),
	}
	rl.updateCapacityLocked()
	rl.tokens = rl.capacity
	return rl
}

func (rl *RateLimiter) updateCapacityLocked() {
	if rl.limit <= 0 {
		rl.capacity = 0
		rl.tokens = 0
		return
	}
	// Capacity allows small burstiness (at least 64KB or 1 second worth of bandwidth)
	capVal := float64(rl.limit)
	if capVal < 64*1024 {
		capVal = 64 * 1024
	}
	rl.capacity = capVal
	if rl.tokens > rl.capacity {
		rl.tokens = rl.capacity
	}
}

// SetLimit dynamically updates the allowed throughput in bytes per second.
func (rl *RateLimiter) SetLimit(limitBps int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refillLocked(time.Now())
	rl.limit = limitBps
	rl.updateCapacityLocked()
}

// Limit returns the current rate limit in bytes per second.
func (rl *RateLimiter) Limit() int64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.limit
}

func (rl *RateLimiter) refillLocked(now time.Time) {
	if rl.limit <= 0 {
		rl.lastRefill = now
		return
	}
	elapsed := now.Sub(rl.lastRefill).Seconds()
	if elapsed > 0 {
		rl.tokens += elapsed * float64(rl.limit)
		if rl.tokens > rl.capacity {
			rl.tokens = rl.capacity
		}
		rl.lastRefill = now
	}
}

// Wait consumes tokens for n bytes, sleeping if necessary until sufficient tokens
// have been replenished. If ctx is cancelled, Wait returns ctx.Err().
func (rl *RateLimiter) Wait(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}

	for {
		rl.mu.Lock()
		if rl.limit <= 0 {
			rl.mu.Unlock()
			return nil
		}

		now := time.Now()
		rl.refillLocked(now)

		needed := float64(n)
		if rl.tokens >= needed {
			rl.tokens -= needed
			rl.mu.Unlock()
			return nil
		}

		// Calculate sleep time needed to accrue missing tokens
		deficit := needed - rl.tokens
		waitSec := deficit / float64(rl.limit)
		rl.mu.Unlock()

		sleepDur := time.Duration(waitSec * float64(time.Second))
		if sleepDur <= 0 {
			continue
		}

		// Cap individual sleep intervals to 250ms to ensure responsiveness to dynamic SetLimit
		if sleepDur > 250*time.Millisecond {
			sleepDur = 250 * time.Millisecond
		}

		timer := time.NewTimer(sleepDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// LimitedReader wraps an io.Reader and throttles read throughput across one or more RateLimiters.
type LimitedReader struct {
	ctx      context.Context
	r        io.Reader
	limiters []*RateLimiter
}

// NewLimitedReader wraps an io.Reader and throttles it according to the provided limiters.
func NewLimitedReader(ctx context.Context, r io.Reader, limiters ...*RateLimiter) *LimitedReader {
	validLimiters := make([]*RateLimiter, 0, len(limiters))
	for _, l := range limiters {
		if l != nil {
			validLimiters = append(validLimiters, l)
		}
	}
	return &LimitedReader{
		ctx:      ctx,
		r:        r,
		limiters: validLimiters,
	}
}

// Read reads from the underlying reader and throttles subsequent execution against all active rate limiters.
func (lr *LimitedReader) Read(p []byte) (int, error) {
	if lr.ctx.Err() != nil {
		return 0, lr.ctx.Err()
	}

	n, err := lr.r.Read(p)
	if n > 0 {
		for _, limiter := range lr.limiters {
			if limiter != nil && limiter.Limit() > 0 {
				if waitErr := limiter.Wait(lr.ctx, n); waitErr != nil {
					return n, waitErr
				}
			}
		}
	}
	return n, err
}

// ParseRateLimit parses human-readable strings like "5MB", "500KB", "10M", "1GB", "2MB/s"
// into bytes per second. Returns 0 for "0", "none", or "unlimited".
func ParseRateLimit(s string) (int64, error) {
	clean := strings.TrimSpace(strings.ToLower(s))
	if clean == "" || clean == "0" || clean == "none" || clean == "unlimited" {
		return 0, nil
	}

	clean = strings.TrimSuffix(clean, "/s")
	clean = strings.TrimSuffix(clean, "ps")

	var multiplier int64 = 1
	var numStr string

	if strings.HasSuffix(clean, "gb") || strings.HasSuffix(clean, "g") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(clean, "gb"), "g")
	} else if strings.HasSuffix(clean, "mb") || strings.HasSuffix(clean, "m") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(clean, "mb"), "m")
	} else if strings.HasSuffix(clean, "kb") || strings.HasSuffix(clean, "k") {
		multiplier = 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(clean, "kb"), "k")
	} else if strings.HasSuffix(clean, "b") {
		multiplier = 1
		numStr = strings.TrimSuffix(clean, "b")
	} else {
		numStr = clean
	}

	val, err := strconv.ParseFloat(strings.TrimSpace(numStr), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid rate limit format %q: %w", s, err)
	}
	if val < 0 {
		return 0, errors.New("rate limit cannot be negative")
	}

	return int64(val * float64(multiplier)), nil
}
