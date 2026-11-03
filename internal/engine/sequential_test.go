package engine_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/queue"
	"github.com/omkute/gofetch/internal/storage"
)

func TestCoordinator_DownloadSequential_MonotonicOrder(t *testing.T) {
	tempDir := t.TempDir()
	const totalSize = int64(1024 * 512) // 512 KB
	const numChunks = 4

	sourcePayload := make([]byte, totalSize)
	if _, err := rand.Read(sourcePayload); err != nil {
		t.Fatalf("failed to generate payload: %v", err)
	}

	var requestedRanges []string
	var mu sync.Mutex

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Accept-Ranges", "bytes")
		header.Set("Content-Length", strconv.FormatInt(totalSize, 10))
		header.Set("Content-Type", "video/mp4")
		header.Set("Content-Disposition", `attachment; filename="stream.mp4"`)

		if req.Method == http.MethodHead {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		rangeHeader := req.Header.Get("Range")
		mu.Lock()
		requestedRanges = append(requestedRanges, rangeHeader)
		mu.Unlock()

		parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		slice := sourcePayload[start : end+1]
		rangeRespHeader := make(http.Header)
		rangeRespHeader.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		rangeRespHeader.Set("Content-Length", strconv.Itoa(len(slice)))
		rangeRespHeader.Set("Content-Type", "video/mp4")

		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Header:     rangeRespHeader,
			Body:       io.NopCloser(bytes.NewReader(slice)),
			Request:    req,
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, mockClient, "http://mock.local/stream.mp4")
	if err != nil {
		t.Fatalf("probe failed: %v", err)
	}

	coordinator := engine.NewCoordinator(mockClient)
	opts := engine.DownloadOptions{
		DestDir:     tempDir,
		MaxWorkers:  numChunks,
		Sequential:  true, // Head-to-tail in-order downloading
	}

	finalPath, err := coordinator.DownloadWithOptions(ctx, probeInfo, opts)
	if err != nil {
		t.Fatalf("DownloadWithOptions failed: %v", err)
	}

	// Invariant: In sequential mode, Range requests MUST arrive strictly in head-to-tail ascending order
	mu.Lock()
	capturedRanges := append([]string(nil), requestedRanges...)
	mu.Unlock()

	expectedRanges := []string{
		"bytes=0-131071",
		"bytes=131072-262143",
		"bytes=262144-393215",
		"bytes=393216-524287",
	}

	if len(capturedRanges) != len(expectedRanges) {
		t.Fatalf("expected %d range requests, got %d: %v", len(expectedRanges), len(capturedRanges), capturedRanges)
	}

	for i, exp := range expectedRanges {
		if capturedRanges[i] != exp {
			t.Fatalf("step %d: expected Range %q, got %q (non-sequential execution detected)",
				i, exp, capturedRanges[i])
		}
	}

	// Verify file was correctly assembled
	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed to read final file: %v", err)
	}
	if !bytes.Equal(data, sourcePayload) {
		t.Fatal("assembled sequential file content does not match source payload")
	}

	// Verify .part staging file was cleaned up
	if _, err := os.Stat(finalPath + storage.PartFileExtension); !os.IsNotExist(err) {
		t.Fatal("staging part file still exists")
	}
}

func TestCoordinator_DownloadWithOptions_WithSemaphoreLimiter(t *testing.T) {
	tempDir := t.TempDir()
	const totalSize = int64(1024 * 256) // 256 KB
	const numChunks = 4
	const maxConcurrentConns = 2

	sourcePayload := make([]byte, totalSize)
	if _, err := rand.Read(sourcePayload); err != nil {
		t.Fatalf("failed to generate payload: %v", err)
	}

	var activeConns int32
	var maxObservedConns int32

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Accept-Ranges", "bytes")
		header.Set("Content-Length", strconv.FormatInt(totalSize, 10))

		if req.Method == http.MethodHead {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		current := atomic.AddInt32(&activeConns, 1)
		for {
			prev := atomic.LoadInt32(&maxObservedConns)
			if current <= prev || atomic.CompareAndSwapInt32(&maxObservedConns, prev, current) {
				break
			}
		}

		// Simulate slight streaming delay
		time.Sleep(15 * time.Millisecond)
		atomic.AddInt32(&activeConns, -1)

		parts := strings.Split(strings.TrimPrefix(req.Header.Get("Range"), "bytes="), "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		slice := sourcePayload[start : end+1]
		rangeRespHeader := make(http.Header)
		rangeRespHeader.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		rangeRespHeader.Set("Content-Length", strconv.Itoa(len(slice)))

		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Header:     rangeRespHeader,
			Body:       io.NopCloser(bytes.NewReader(slice)),
			Request:    req,
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, mockClient, "http://mock.local/limited.bin")
	if err != nil {
		t.Fatalf("probe failed: %v", err)
	}

	connLimiter := queue.NewSemaphore(maxConcurrentConns)
	coordinator := engine.NewCoordinator(mockClient)

	opts := engine.DownloadOptions{
		DestDir:    tempDir,
		MaxWorkers: numChunks,
		Semaphore:  connLimiter,
	}

	finalPath, err := coordinator.DownloadWithOptions(ctx, probeInfo, opts)
	if err != nil {
		t.Fatalf("DownloadWithOptions failed: %v", err)
	}

	// Verify semaphore bounded the maximum concurrent connections
	observed := atomic.LoadInt32(&maxObservedConns)
	if observed > int32(maxConcurrentConns) {
		t.Fatalf("connection limiter invariant violated: observed %d concurrent requests, max allowed %d",
			observed, maxConcurrentConns)
	}

	// Verify file integrity
	destFile := filepath.Join(tempDir, probeInfo.Filename)
	if finalPath != destFile {
		t.Fatalf("expected finalPath %q, got %q", destFile, finalPath)
	}

	data, err := os.ReadFile(finalPath)
	if err != nil || !bytes.Equal(data, sourcePayload) {
		t.Fatal("downloaded file corrupted or missing")
	}
}
