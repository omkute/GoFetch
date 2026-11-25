package integration_test

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
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/state"
	"github.com/omkute/gofetch/internal/storage"
)

// TestIntegration_ResumePreservesProgressAndRevalidates exercises the real
// coordinator + bbolt checkpoint store: a multi-chunk download is interrupted
// around 50%, resumed, and must fetch only the unfinished byte ranges. An
// If-Range/ETag conditional request must guard the resume, and the assembled
// file must be bit-for-bit identical to the source.
func TestIntegration_ResumePreservesProgressAndRevalidates(t *testing.T) {
	tempDir := t.TempDir()
	const totalSize = 512 * 1024
	const numChunks = 4

	payload := make([]byte, totalSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("failed to build payload: %v", err)
	}
	etag := `"resume-e2e-etag"`

	var mu sync.Mutex
	var recordedRanges []string
	var sawIfRange bool
	var servedBytes int64
	var interrupted bool

	cancelCh := make(chan struct{}, 1)

	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		base := make(http.Header)
		base.Set("Accept-Ranges", "bytes")
		base.Set("Content-Length", strconv.Itoa(totalSize))
		base.Set("ETag", etag)
		base.Set("Content-Disposition", `attachment; filename="resume_e2e.bin"`)

		if req.Method == http.MethodHead {
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        base,
				Body:          io.NopCloser(bytes.NewReader(nil)),
				ContentLength: 0,
				Request:       req,
			}, nil
		}

		if req.Header.Get("If-Range") != "" {
			mu.Lock()
			sawIfRange = true
			mu.Unlock()
		}

		rangeHeader := req.Header.Get("Range")
		if rangeHeader == "" {
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        base,
				Body:          io.NopCloser(bytes.NewReader(payload)),
				ContentLength: totalSize,
				Request:       req,
			}, nil
		}

		// Revalidation probe issued by the engine on resume. It is a single
		// byte conditional fetch and must not count as a chunk data fetch.
		if req.Header.Get("If-Range") != "" {
			if start, end, ok := singleByteRange(rangeHeader); ok {
				return partialResponse(req, base, start, end, payload[start:end+1], totalSize), nil
			}
		}

		var start, end int64
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
			return nil, err
		}
		if end >= totalSize {
			end = totalSize - 1
		}
		if start < 0 || start > end {
			return nil, fmt.Errorf("invalid range %q", rangeHeader)
		}

		mu.Lock()
		recordedRanges = append(recordedRanges, rangeHeader)
		servedBytes += end - start + 1
		shouldInterrupt := !interrupted && servedBytes >= int64(totalSize)/2
		if shouldInterrupt {
			interrupted = true
		}
		mu.Unlock()
		if shouldInterrupt {
			select {
			case cancelCh <- struct{}{}:
			default:
			}
		}

		return partialResponse(req, base, start, end, payload[start:end+1], totalSize), nil
	})}

	probeURL := "http://mock.internal/resume_e2e.bin"
	store, err := state.NewBboltStore(filepath.Join(tempDir, "checkpoints.db"))
	if err != nil {
		t.Fatalf("failed to open checkpoint store: %v", err)
	}
	defer func() { _ = store.Close() }()

	const jobID = "resume-e2e-job"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-cancelCh
		cancel()
	}()

	probe, err := engine.Probe(ctx, client, probeURL)
	if err != nil {
		t.Fatalf("probe failed: %v", err)
	}
	if probe.ContentLength != totalSize {
		t.Fatalf("probe content length = %d, want %d", probe.ContentLength, totalSize)
	}

	// First attempt: interrupt mid-transfer.
	first := engine.NewCoordinatorWithStore(client, store)
	if _, err := first.DownloadWithOptions(ctx, probe, engine.DownloadOptions{
		DestDir:    tempDir,
		JobID:      jobID,
		MaxWorkers: numChunks,
	}); err == nil {
		t.Fatal("expected the first download to be interrupted")
	}

	checkpoint, err := store.GetCheckpoint(jobID)
	if err != nil {
		t.Fatalf("failed to read checkpoint: %v", err)
	}
	if checkpoint == nil {
		t.Fatal("expected a checkpoint after interruption")
	}

	expectedRanges := make(map[string]bool)
	for _, ch := range checkpoint.Chunks {
		if ch.Remaining() > 0 {
			expectedRanges[ch.RangeHeader()] = true
		}
	}
	if len(expectedRanges) == 0 {
		t.Fatal("expected at least one incomplete chunk to resume")
	}

	partPath := filepath.Join(tempDir, probe.Filename) + storage.PartFileExtension
	if info, statErr := os.Stat(partPath); statErr != nil || info.Size() != totalSize {
		t.Fatalf("expected preserved full-size part file, stat: %v, size: %v", statErr, info)
	}

	// Reset recording, then resume with a fresh coordinator sharing the store.
	mu.Lock()
	recordedRanges = nil
	sawIfRange = false
	mu.Unlock()

	resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer resumeCancel()
	second := engine.NewCoordinatorWithStore(client, store)
	finalPath, err := second.DownloadWithOptions(resumeCtx, probe, engine.DownloadOptions{
		DestDir:    tempDir,
		JobID:      jobID,
		MaxWorkers: numChunks,
	})
	if err != nil {
		t.Fatalf("resumed download failed: %v", err)
	}

	mu.Lock()
	ranges := append([]string(nil), recordedRanges...)
	revalidated := sawIfRange
	mu.Unlock()

	if len(ranges) == 0 {
		t.Fatal("expected chunk range requests during resume")
	}
	for _, r := range ranges {
		if !expectedRanges[r] {
			t.Errorf("resume fetched unexpected range %q; only %v should be requested", r, expectedRanges)
		}
	}
	if !revalidated {
		t.Error("expected an If-Range/ETag revalidation request before resuming")
	}

	assembled, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed to read final file: %v", err)
	}
	if !bytes.Equal(assembled, payload) {
		t.Fatal("resumed file does not match the original payload bit-for-bit")
	}

	if _, statErr := os.Stat(partPath); !os.IsNotExist(statErr) {
		t.Errorf("part file %q should be removed after completion: %v", partPath, statErr)
	}
	after, err := store.GetCheckpoint(jobID)
	if err != nil {
		t.Fatalf("failed to check checkpoint after completion: %v", err)
	}
	if after != nil {
		t.Error("checkpoint should be deleted after a successful resume")
	}
}

func singleByteRange(header string) (int64, int64, bool) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}
	parts := strings.Split(strings.TrimPrefix(header, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	start, errStart := strconv.ParseInt(parts[0], 10, 64)
	end, errEnd := strconv.ParseInt(parts[1], 10, 64)
	if errStart != nil || errEnd != nil || start != end {
		return 0, 0, false
	}
	return start, end, true
}

func partialResponse(req *http.Request, base http.Header, start, end int64, data []byte, total int64) *http.Response {
	h := base.Clone()
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	h.Set("Content-Length", strconv.Itoa(len(data)))
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: int64(len(data)),
		Request:       req,
	}
}
