package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
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
		{"typed HTTPStatusError 503", &engine.HTTPStatusError{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable"}, 0, true},
		{"typed HTTPStatusError 404", &engine.HTTPStatusError{StatusCode: http.StatusNotFound, Status: "404 Not Found"}, 0, false},
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

// A real httptest server returning 503 twice before a 206 proves that the
// status code rides through HTTPStatusError into ExecuteWithRetry's backoff.
func TestDownloadWithOptions_RetriesTransientHTTPStatus(t *testing.T) {
	const payload = "transient status retry payload"
	var rangeRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "" {
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(payload))
			return
		}
		if rangeRequests.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte(payload))
	}))
	defer server.Close()

	url := server.URL + "/retry.bin"
	probe := &engine.ResourceInfo{
		URL:           url,
		FinalURL:      url,
		Filename:      "retry.bin",
		ContentLength: int64(len(payload)),
		AcceptRanges:  true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	coordinator := engine.NewCoordinator(server.Client())
	finalPath, err := coordinator.DownloadWithOptions(ctx, probe, engine.DownloadOptions{
		DestDir:    t.TempDir(),
		MaxWorkers: 1,
	})
	if err != nil {
		t.Fatalf("expected success after two 503 responses, got: %v", err)
	}

	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed to read committed file: %v", err)
	}
	if !bytes.Equal(data, []byte(payload)) {
		t.Fatalf("file content mismatch: expected %q, got %q", payload, string(data))
	}
	if got := rangeRequests.Load(); got != 3 {
		t.Fatalf("expected exactly 3 Range requests (503, 503, 206), got %d", got)
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
