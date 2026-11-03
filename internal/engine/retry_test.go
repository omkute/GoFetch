package engine_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/engine"
)

func TestIsTransient(t *testing.T) {
	testCases := []struct {
		name       string
		err        error
		statusCode int
		expected   bool
	}{
		{"429 Too Many Requests", nil, http.StatusTooManyRequests, true},
		{"500 Internal Server Error", nil, http.StatusInternalServerError, true},
		{"502 Bad Gateway", nil, http.StatusBadGateway, true},
		{"503 Service Unavailable", nil, http.StatusServiceUnavailable, true},
		{"504 Gateway Timeout", nil, http.StatusGatewayTimeout, true},
		{"200 OK", nil, http.StatusOK, false},
		{"206 Partial Content", nil, http.StatusPartialContent, false},
		{"404 Not Found", nil, http.StatusNotFound, false},
		{"401 Unauthorized", nil, http.StatusUnauthorized, false},
		{"context.Canceled", context.Canceled, 0, false},
		{"context.DeadlineExceeded", context.DeadlineExceeded, 0, false},
		{"generic transient error", errors.New("connection reset by peer"), 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := engine.IsTransient(tc.err, tc.statusCode)
			if actual != tc.expected {
				t.Errorf("IsTransient(%v, %d) = %v; expected %v", tc.err, tc.statusCode, actual, tc.expected)
			}
		})
	}
}

func TestExecuteWithRetry_SuccessAfterTransientErrors(t *testing.T) {
	ctx := context.Background()
	config := engine.RetryConfig{
		MaxRetries:      3,
		InitialInterval: 5 * time.Millisecond,
		MaxInterval:     20 * time.Millisecond,
		Multiplier:      2.0,
	}

	attempts := 0
	err := engine.ExecuteWithRetry(ctx, config, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("connection reset by peer")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestExecuteWithRetry_ExhaustRetries(t *testing.T) {
	ctx := context.Background()
	config := engine.RetryConfig{
		MaxRetries:      2,
		InitialInterval: 5 * time.Millisecond,
		MaxInterval:     10 * time.Millisecond,
		Multiplier:      2.0,
	}

	attempts := 0
	err := engine.ExecuteWithRetry(ctx, config, func() error {
		attempts++
		return errors.New("persistent connection error")
	})

	if err == nil {
		t.Fatal("expected error after exhausted retries, got nil")
	}
	// Initial attempt + 2 retries = 3 total attempts
	if attempts != 3 {
		t.Errorf("expected 3 attempts (1 initial + 2 retries), got %d", attempts)
	}
}

func TestExecuteWithRetry_ContextCancelledStopsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	config := engine.RetryConfig{
		MaxRetries:      5,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     500 * time.Millisecond,
		Multiplier:      2.0,
	}

	attempts := 0
	errCh := make(chan error, 1)

	go func() {
		errCh <- engine.ExecuteWithRetry(ctx, config, func() error {
			attempts++
			return errors.New("transient error")
		})
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for retry loop to abort on cancelled context")
	}
}
