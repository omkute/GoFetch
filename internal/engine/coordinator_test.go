package engine_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/storage"
)

func TestDownloadSingleStream_NonChunkableServer(t *testing.T) {
	tempDir := t.TempDir()
	const payloadSize = 256 * 1024 // 256 KB

	expectedPayload := make([]byte, payloadSize)
	if _, err := rand.Read(expectedPayload); err != nil {
		t.Fatalf("failed to generate random test payload: %v", err)
	}

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Accept-Ranges", "none")
		header.Set("Content-Length", "262144")
		header.Set("Content-Type", "application/octet-stream")
		header.Set("Content-Disposition", `attachment; filename="single_stream.bin"`)

		if req.Method == http.MethodHead {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(bytes.NewReader(expectedPayload)),
			Request:    req,
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, mockClient, "http://mock.local/single_stream")
	if err != nil {
		t.Fatalf("probe failed: %v", err)
	}

	if probeInfo.AcceptRanges {
		t.Fatalf("server should report Accept-Ranges: none")
	}

	coordinator := engine.NewCoordinator(mockClient)
	committedPath, err := coordinator.DownloadSingleStream(ctx, probeInfo, tempDir)
	if err != nil {
		t.Fatalf("DownloadSingleStream failed: %v", err)
	}

	expectedFinalPath := filepath.Join(tempDir, "single_stream.bin")
	if committedPath != expectedFinalPath {
		t.Fatalf("expected committedPath %q, got %q", expectedFinalPath, committedPath)
	}

	// Verify part file does not exist
	partPath := committedPath + storage.PartFileExtension
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("part file %q still exists after completion", partPath)
	}

	// Verify byte content matches bit for bit
	downloadedData, err := os.ReadFile(committedPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedPayload) {
		t.Fatalf("downloaded file content does not match expected payload (data corruption)")
	}
}

// blockingReader blocks on Read until context is cancelled or unblock channel is triggered.
type blockingReader struct {
	ctx     context.Context
	initial []byte
	sent    bool
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if !r.sent && len(r.initial) > 0 {
		n := copy(p, r.initial)
		r.sent = true
		return n, nil
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (r *blockingReader) Close() error {
	return nil
}

func TestDownloadSingleStream_ContextCancellationCleansUp(t *testing.T) {
	tempDir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Content-Disposition", `attachment; filename="cancelled.bin"`)
		header.Set("Content-Length", "1048576")

		if req.Method == http.MethodHead {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body: &blockingReader{
				ctx:     ctx,
				initial: []byte("initial chunk bytes"),
			},
			Request: req,
		}, nil
	})

	probeInfo, err := engine.Probe(ctx, mockClient, "http://mock.local/cancelled_stream")
	if err != nil {
		t.Fatalf("probe failed: %v", err)
	}

	coordinator := engine.NewCoordinator(mockClient)

	errCh := make(chan error, 1)
	go func() {
		_, dlErr := coordinator.DownloadSingleStream(ctx, probeInfo, tempDir)
		errCh <- dlErr
	}()

	// Allow reader to send initial data and then cancel context
	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case dlErr := <-errCh:
		if dlErr == nil || !errors.Is(dlErr, context.Canceled) {
			t.Fatalf("expected context.Canceled error, got %v", dlErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for download to abort upon cancellation")
	}

	// Incomplete part file and final file must be discarded
	finalPath := filepath.Join(tempDir, "cancelled.bin")
	partPath := finalPath + storage.PartFileExtension

	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Fatalf("final file %q should not exist after cancellation", finalPath)
	}
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("staging part file %q was not discarded after cancellation", partPath)
	}
}
