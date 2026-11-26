package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/api"
)

const (
	daemonStartTimeout    = 5 * time.Second
	daemonShutdownTimeout = 5 * time.Second
	daemonRequestTimeout  = 10 * time.Second
	integrationPollLimit  = 10 * time.Second
	integrationPollEvery  = 10 * time.Millisecond
)

// daemonHarness owns a real loopback daemon and the client used to talk to it.
type daemonHarness struct {
	baseURL string
	client  *http.Client
}

// startTestDaemon starts an api.Server on a real listener and registers a
// bounded shutdown in t.Cleanup. The spec names 127.0.0.1:12701 as the daemon
// address, but tests bind 127.0.0.1:0 instead: a developer daemon (or a
// parallel test process) may already own 12701, whereas the OS assigns a free
// ephemeral port. Addr(), not the requested address, is the source of truth
// for client URLs. Start() also starts the scheduler, so enqueued downloads
// really execute during integration tests.
func startTestDaemon(t *testing.T, configure func(*api.Server)) *daemonHarness {
	t.Helper()

	server := api.NewServer("127.0.0.1:0")
	if configure != nil {
		configure(server)
	}

	started := make(chan error, 1)
	go func() {
		started <- server.Start()
	}()

	select {
	case <-server.Ready():
	case err := <-started:
		t.Fatalf("daemon Start() failed before Ready(): %v", err)
	case <-time.After(daemonStartTimeout):
		t.Fatalf("daemon did not become ready within %s", daemonStartTimeout)
	}

	h := &daemonHarness{
		baseURL: "http://" + server.Addr(),
		client:  &http.Client{Timeout: daemonRequestTimeout},
	}

	t.Cleanup(func() {
		// Close idle keep-alive connections first: otherwise the daemon's
		// http.Server.Shutdown can wait on a connection that became idle
		// after Shutdown took its active-connection snapshot.
		h.client.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), daemonShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("daemon shutdown failed: %v", err)
		}
		// Join the Start goroutine so no listener goroutine survives the test.
		select {
		case err := <-started:
			if err != nil {
				t.Errorf("daemon Start() returned error: %v", err)
			}
		case <-time.After(daemonShutdownTimeout):
			t.Error("daemon Start() goroutine did not exit after shutdown")
		}
	})

	return h
}

// request drives a real HTTP request against the daemon listener.
func (h *daemonHarness) request(t *testing.T, method, path string, body io.Reader, contentType string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(method, h.baseURL+path, body)
	if err != nil {
		t.Fatalf("build %s %s request: %v", method, path, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

// jsonRequest sends JSON (when payload is non-nil), optionally decodes the
// JSON response, and always closes the response body.
func (h *daemonHarness) jsonRequest(t *testing.T, method, path string, payload, out any) int {
	t.Helper()

	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal %s %s payload: %v", method, path, err)
		}
		body = bytes.NewReader(data)
	}

	resp := h.request(t, method, path, body, "application/json")
	defer func() { _ = resp.Body.Close() }()

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode %s %s response (%d): %v", method, path, resp.StatusCode, err)
		}
	}
	// Drain any remainder so the connection can return to the idle pool
	// instead of lingering as an active server-side connection.
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func (h *daemonHarness) getJSON(t *testing.T, path string, out any) int {
	t.Helper()
	return h.jsonRequest(t, http.MethodGet, path, nil, out)
}

func (h *daemonHarness) postJSON(t *testing.T, path string, payload, out any) int {
	t.Helper()
	return h.jsonRequest(t, http.MethodPost, path, payload, out)
}

// waitForDetail polls the detail endpoint until match accepts the snapshot.
func (h *daemonHarness) waitForDetail(t *testing.T, id string, match func(api.DownloadDetailResponse) bool) api.DownloadDetailResponse {
	t.Helper()

	path := "/api/v1/downloads/" + id
	deadline := time.Now().Add(integrationPollLimit)
	for {
		var detail api.DownloadDetailResponse
		code := h.getJSON(t, path, &detail)
		if code != http.StatusOK {
			t.Fatalf("GET %s: expected 200, got %d", path, code)
		}
		if match(detail) {
			return detail
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for detail condition on %s (last status=%q downloaded=%d chunks=%d error=%q)",
				id, detail.Status, detail.DownloadedBytes, len(detail.Chunks), detail.Error)
		}
		time.Sleep(integrationPollEvery)
	}
}

// waitForCondition polls a boolean condition with a bounded deadline.
func waitForCondition(t *testing.T, what string, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(integrationPollEvery)
	}
}

// newRangeOrigin serves real byte ranges via http.ServeContent: it advertises
// Accept-Ranges: bytes and answers Range requests with 206 + Content-Range, so
// daemon chunk transfers genuinely execute.
func newRangeOrigin(t *testing.T, name string, payload []byte) *httptest.Server {
	t.Helper()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/"+name {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Accept-Ranges", "bytes")
		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(payload))
	}))
	t.Cleanup(origin.Close)
	return origin
}

// gatedRangeOrigin is a range-capable origin that sends the first half of
// every byte range immediately and holds the remainder until released. This
// makes an in-flight, partially downloaded job deterministic to observe and
// pause, without relying on sleeps or scheduler timing.
type gatedRangeOrigin struct {
	*httptest.Server
	chunkRequested chan struct{}
	release        chan struct{}
	closeOnce      sync.Once
}

func newGatedRangeOrigin(t *testing.T, name string, payload []byte) *gatedRangeOrigin {
	t.Helper()

	origin := &gatedRangeOrigin{
		chunkRequested: make(chan struct{}, 16),
		release:        make(chan struct{}),
	}
	origin.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/"+name {
			http.NotFound(w, r)
			return
		}

		header := w.Header()
		header.Set("Accept-Ranges", "bytes")
		header.Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodHead {
			header.Set("Content-Length", strconv.Itoa(len(payload)))
			return
		}

		start, end := 0, len(payload)-1
		status := http.StatusOK
		if raw := r.Header.Get("Range"); raw != "" {
			if _, err := fmt.Sscanf(raw, "bytes=%d-%d", &start, &end); err != nil || start < 0 || start > end {
				http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= len(payload) {
				end = len(payload) - 1
			}
			status = http.StatusPartialContent
			header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
		}

		body := payload[start : end+1]
		header.Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(status)
		select {
		case origin.chunkRequested <- struct{}{}:
		default:
		}

		// The first read produces real progress; the rest stays blocked so the
		// job cannot complete before the test pauses it.
		split := len(body) / 2
		if split == 0 {
			_, _ = w.Write(body)
			return
		}
		if _, err := w.Write(body[:split]); err != nil {
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-origin.release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(body[split:])
	}))

	t.Cleanup(func() {
		origin.unblock()
		origin.Close()
	})
	return origin
}

func (o *gatedRangeOrigin) unblock() {
	o.closeOnce.Do(func() { close(o.release) })
}

// parseURLListFile mirrors the CLI --input-file ingestion: non-empty,
// non-comment lines become batch URLs.
func parseURLListFile(t *testing.T, path string) []string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read URL list %q: %v", path, err)
	}
	var urls []string
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			urls = append(urls, trimmed)
		}
	}
	return urls
}
