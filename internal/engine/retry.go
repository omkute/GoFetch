package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"
)

// RetryConfig specifies parameters for exponential backoff and retry behavior.
type RetryConfig struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

// DefaultRetryConfig returns standard backoff retry defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:      3,
		InitialInterval: 200 * time.Millisecond,
		MaxInterval:     3 * time.Second,
		Multiplier:      2.0,
	}
}

// HTTPStatusError carries the status returned by a failed HTTP request so retry
// decisions use the real response code instead of parsing an error string.
// It may wrap an underlying cause while preserving errors.Is/errors.As chains.
type HTTPStatusError struct {
	StatusCode int
	Status     string
	Err        error
}

func (e *HTTPStatusError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("HTTP status %d (%s): %v", e.StatusCode, e.Status, e.Err)
	}
	return fmt.Sprintf("HTTP status %d (%s)", e.StatusCode, e.Status)
}

func (e *HTTPStatusError) Unwrap() error {
	return e.Err
}

// isTransientStatus reports whether an HTTP status code is safe to retry.
func isTransientStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// IsTransient evaluates whether an HTTP status code or Go error represents a transient failure
// that is safe to retry.
func IsTransient(err error, statusCode int) bool {
	if isTransientStatus(statusCode) {
		return true
	}

	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// The typed error carries the authoritative status code; ExecuteWithRetry
	// passes 0 for statusCode, so detect it before falling back to string heuristics.
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) && statusErr != nil {
		return isTransientStatus(statusErr.StatusCode)
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	errStr := strings.ToLower(err.Error())
	transientPhrases := []string{
		"connection reset",
		"broken pipe",
		"connection refused",
		"connection error",
		"connection",
		"transient",
		"temporary failure",
		"server closed idle connection",
		"tls handshake timeout",
		"eof",
		"timeout",
	}

	for _, phrase := range transientPhrases {
		if strings.Contains(errStr, phrase) {
			return true
		}
	}

	return false
}

// ExecuteWithRetry executes fn, retrying on transient errors with exponential backoff and jitter.
func ExecuteWithRetry(ctx context.Context, config RetryConfig, fn func() error) error {
	if config.MaxRetries < 0 {
		config.MaxRetries = 0
	}
	if config.InitialInterval <= 0 {
		config.InitialInterval = 100 * time.Millisecond
	}
	if config.MaxInterval <= 0 {
		config.MaxInterval = 2 * time.Second
	}
	if config.Multiplier < 1.0 {
		config.Multiplier = 2.0
	}

	var lastErr error
	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		if attempt == config.MaxRetries || !IsTransient(err, 0) {
			return err
		}

		// Calculate exponential backoff with full jitter
		factor := math.Pow(config.Multiplier, float64(attempt))
		backoff := float64(config.InitialInterval) * factor
		if backoff > float64(config.MaxInterval) {
			backoff = float64(config.MaxInterval)
		}

		// Jitter between [0.5 * backoff, 1.0 * backoff]
		jitteredBackoff := time.Duration(backoff * (0.5 + rand.Float64()*0.5)) //nolint:gosec // jitter random

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jitteredBackoff):
		}
	}

	return lastErr
}
