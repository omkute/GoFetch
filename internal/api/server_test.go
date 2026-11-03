package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/api"
)

func TestHealthEndpoint(t *testing.T) {
	server := api.NewServer("127.0.0.1:0")

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	// Wait for server to start listening
	select {
	case <-server.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server failed to start listening in time")
	}

	addr := server.Addr()
	url := "http://" + addr + "/api/v1/health"

	var resp *http.Response
	var err error
	for i := 0; i < 20; i++ {
		resp, err = http.Get(url) //nolint:gosec // loopback test URL
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Logf("TCP dial restricted in sandbox (%v), verifying router directly with httptest", err)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
			server.Router().ServeHTTP(rec, req)
			resp = rec.Result()
		} else {
			t.Fatalf("failed to GET %s: %v", url, err)
		}
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" && contentType != "application/json; charset=utf-8" {
		t.Errorf("expected Content-Type application/json, got %q", contentType)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	var data map[string]string
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatalf("failed to unmarshal JSON response (%s): %v", string(body), err)
	}

	if data["status"] != "ok" {
		t.Errorf("expected status 'ok', got %q", data["status"])
	}

	// Verify graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("failed to shutdown server: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("server.Start() error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestHealthEndpointDefaultAddr(t *testing.T) {
	server := api.NewServer("")
	if server.Addr() != api.DefaultAddr {
		t.Errorf("expected default addr %s, got %s", api.DefaultAddr, server.Addr())
	}
}
