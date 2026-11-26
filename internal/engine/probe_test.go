package engine_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/engine"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newMockClient(fn roundTripFunc) *http.Client {
	return &http.Client{
		Transport: fn,
	}
}

func TestSanitizeFilename(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "regular filename",
			input:    "document.pdf",
			expected: "document.pdf",
		},
		{
			name:     "unix path traversal",
			input:    "../../../../etc/passwd",
			expected: "passwd",
		},
		{
			name:     "windows path traversal",
			input:    `..\..\..\Windows\System32\cmd.exe`,
			expected: "cmd.exe",
		},
		{
			name:     "null byte injection",
			input:    "photo\x00.jpg.exe",
			expected: "photo.jpg.exe",
		},
		{
			name:     "empty filename defaults to fallback",
			input:    "",
			expected: "download",
		},
		{
			name:     "slash only defaults to fallback",
			input:    "///",
			expected: "download",
		},
		{
			name:     "dots only defaults to fallback",
			input:    "..",
			expected: "download",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := engine.SanitizeFilename(tc.input)
			if actual != tc.expected {
				t.Errorf("SanitizeFilename(%q) = %q; expected %q", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestProbe_HeadSuccess_AcceptRanges(t *testing.T) {
	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Accept-Ranges", "bytes")
		header.Set("Content-Length", "2048")
		header.Set("Content-Type", "application/zip")
		header.Set("Content-Disposition", `attachment; filename="bundle.zip"`)
		header.Set("ETag", `"etag-12345"`)

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Request:    req,
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	info, err := engine.Probe(ctx, mockClient, "http://mock.local/files/download")
	if err != nil {
		t.Fatalf("unexpected probe error: %v", err)
	}

	if !info.AcceptRanges {
		t.Errorf("expected AcceptRanges to be true")
	}
	if info.ContentLength != 2048 {
		t.Errorf("expected ContentLength 2048, got %d", info.ContentLength)
	}
	if info.ContentType != "application/zip" {
		t.Errorf("expected ContentType application/zip, got %q", info.ContentType)
	}
	if info.Filename != "bundle.zip" {
		t.Errorf("expected Filename bundle.zip, got %q", info.Filename)
	}
	if info.ETag != `"etag-12345"` {
		t.Errorf("expected ETag \"etag-12345\", got %q", info.ETag)
	}
}

func TestProbe_LastModifiedHeaderPreserved(t *testing.T) {
	const lastModified = "Wed, 21 Oct 2015 07:28:00 GMT"

	for _, tc := range []struct {
		name       string
		rejectHEAD bool
	}{
		{name: "HEAD response"},
		{name: "GET fallback response", rejectHEAD: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
				if tc.rejectHEAD && req.Method == http.MethodHead {
					return &http.Response{
						StatusCode: http.StatusMethodNotAllowed,
						Header:     make(http.Header),
						Body:       io.NopCloser(bytes.NewReader(nil)),
						Request:    req,
					}, nil
				}
				header := make(http.Header)
				header.Set("Last-Modified", lastModified)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       io.NopCloser(bytes.NewReader(nil)),
					Request:    req,
				}, nil
			})

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			info, err := engine.Probe(ctx, mockClient, "http://mock.local/resource")
			if err != nil {
				t.Fatalf("unexpected probe error: %v", err)
			}
			if info.LastModified != lastModified {
				t.Errorf("LastModified = %q, want %q", info.LastModified, lastModified)
			}
		})
	}
}

func TestProbe_PathTraversalInContentDisposition(t *testing.T) {
	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Content-Disposition", `attachment; filename="../../../malicious.sh"`)
		header.Set("Content-Length", "512")

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Request:    req,
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	info, err := engine.Probe(ctx, mockClient, "http://mock.local/archive")
	if err != nil {
		t.Fatalf("unexpected probe error: %v", err)
	}

	if info.Filename != "malicious.sh" {
		t.Fatalf("expected sanitized filename 'malicious.sh', got %q", info.Filename)
	}
}

func TestProbe_FallbackToGetWhenHeadMethodNotAllowed(t *testing.T) {
	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodHead {
			return &http.Response{
				StatusCode: http.StatusMethodNotAllowed,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		if req.Method == http.MethodGet {
			header := make(http.Header)
			header.Set("Accept-Ranges", "bytes")
			header.Set("Content-Length", "4096")

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(make([]byte, 1))),
				Request:    req,
			}, nil
		}

		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Request:    req,
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	info, err := engine.Probe(ctx, mockClient, "http://mock.local/fallback")
	if err != nil {
		t.Fatalf("expected probe to fallback to GET, got error: %v", err)
	}

	if info.ContentLength != 4096 {
		t.Errorf("expected ContentLength 4096, got %d", info.ContentLength)
	}
	if !info.AcceptRanges {
		t.Errorf("expected AcceptRanges to be true from GET response")
	}
}

func TestUserAgentOverride_ProbeAndChunkRequests(t *testing.T) {
	payload := bytes.Repeat([]byte("user-agent payload "), 1024)
	var mu sync.Mutex
	var agents []string

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		agents = append(agents, r.Header.Get("User-Agent"))
		mu.Unlock()
		http.ServeContent(w, r, "payload.bin", time.Now(), bytes.NewReader(payload))
	}))
	defer origin.Close()

	const customAgent = "GoFetch-Custom/2.0"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := origin.Client()
	probe, err := engine.ProbeWithOptions(ctx, client, origin.URL+"/payload.bin", engine.ProbeOptions{UserAgent: customAgent})
	if err != nil {
		t.Fatalf("ProbeWithOptions failed: %v", err)
	}
	if _, err := engine.NewCoordinator(client).DownloadWithOptions(ctx, probe, engine.DownloadOptions{
		DestDir:    t.TempDir(),
		MaxWorkers: 2,
		UserAgent:  customAgent,
	}); err != nil {
		t.Fatalf("DownloadWithOptions failed: %v", err)
	}

	mu.Lock()
	seen := append([]string(nil), agents...)
	agents = nil
	mu.Unlock()
	if len(seen) < 2 {
		t.Fatalf("expected probe and chunk requests, got %d requests", len(seen))
	}
	for _, agent := range seen {
		if agent != customAgent {
			t.Errorf("User-Agent = %q, want %q", agent, customAgent)
		}
	}

	// Empty options must keep the historical default agent.
	if _, err := engine.ProbeWithOptions(ctx, client, origin.URL+"/payload.bin", engine.ProbeOptions{}); err != nil {
		t.Fatalf("ProbeWithOptions with default agent failed: %v", err)
	}
	mu.Lock()
	seen = append([]string(nil), agents...)
	mu.Unlock()
	for _, agent := range seen {
		if agent != engine.DefaultUserAgent {
			t.Errorf("default User-Agent = %q, want %q", agent, engine.DefaultUserAgent)
		}
	}
}
