package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/omkute/gofetch/internal/storage"
)

// Coordinator manages single-stream and multi-chunk download lifecycles.
type Coordinator struct {
	client *http.Client
	store  CheckpointStore
}

// NewCoordinator creates a new download coordinator with the provided HTTP client.
func NewCoordinator(client *http.Client) *Coordinator {
	if client == nil {
		client = http.DefaultClient
	}
	return &Coordinator{client: client}
}

// NewCoordinatorWithStore creates a new coordinator configured with a persistent CheckpointStore.
func NewCoordinatorWithStore(client *http.Client, store CheckpointStore) *Coordinator {
	if client == nil {
		client = http.DefaultClient
	}
	return &Coordinator{
		client: client,
		store:  store,
	}
}

// DownloadSingleStream executes a sequential streaming download of a resource,
// writing into a staged `.gofetch.part` file and atomically committing on completion.
func (c *Coordinator) DownloadSingleStream(ctx context.Context, probe *ResourceInfo, destDir string) (string, error) {
	return c.DownloadSingleStreamWithOptions(ctx, probe, DownloadOptions{DestDir: destDir})
}

// DownloadSingleStreamWithOptions executes a single-stream download honoring DownloadOptions,
// ProgressFn, and resumable checkpoint state.
func (c *Coordinator) DownloadSingleStreamWithOptions(ctx context.Context, probe *ResourceInfo, opts DownloadOptions) (string, error) {
	if probe == nil {
		return "", errors.New("probe info cannot be nil")
	}
	userAgent := resolveUserAgent(opts.UserAgent)

	absDir, err := CleanDestinationPath(opts.DestDir)
	if err != nil {
		return "", err
	}

	finalPath := filepath.Join(absDir, probe.Filename)
	partPath := StagingPathFor(finalPath, opts.JobID)
	downloadURL := probe.FinalURL
	if downloadURL == "" {
		downloadURL = probe.URL
	}

	resumable := c.store != nil && opts.JobID != ""

	// Replay staged bytes only when the checkpoint size matches and the
	// resource still carries the same ETag (If-Range revalidation).
	var resumeOffset int64
	var resumeETag string
	if resumable {
		if cp, getErr := c.store.GetCheckpoint(opts.JobID); getErr == nil && cp != nil {
			if probe.ContentLength < 0 || cp.TotalSize == probe.ContentLength {
				if info, statErr := os.Stat(partPath); statErr == nil && info.Size() > 0 {
					offset := info.Size()
					if probe.ContentLength >= 0 && offset > probe.ContentLength {
						offset = probe.ContentLength
					}
					unchanged, valErr := revalidateResource(ctx, c.client, downloadURL, cp.ETag, userAgent, offset)
					if valErr == nil && unchanged {
						resumeOffset = offset
						resumeETag = cp.ETag
					}
				}
			}
		}
		if resumeOffset == 0 {
			if _, statErr := os.Stat(partPath); statErr == nil {
				_ = os.Remove(partPath)
				_ = c.store.DeleteCheckpoint(opts.JobID)
			}
		}
	}

	// Preallocate if content length is known (>= 0), otherwise create dynamic staging file
	writer, err := storage.NewFileWriterWithPartPath(finalPath, partPath, probe.ContentLength)
	if err != nil {
		return "", fmt.Errorf("failed to initialize staging file: %w", err)
	}

	// Progress offsets are handed to a background consumer; no DB work happens
	// in the streaming loop itself.
	var tracker *checkpointTracker
	if resumable {
		etag := probe.ETag
		if resumeETag != "" {
			etag = resumeETag
		}
		cp := &Checkpoint{
			ID:        opts.JobID,
			URL:       probe.FinalURL,
			FinalPath: finalPath,
			TotalSize: probe.ContentLength,
			ETag:      etag,
			Chunks:    []Chunk{{Index: 0, Start: 0, End: probe.ContentLength - 1, Current: resumeOffset}},
			UpdatedAt: time.Now(),
		}
		tracker = newCheckpointTracker(c.store, cp, cp.Chunks, 0)
	}

	committed := false
	defer func() {
		if !committed {
			if resumable {
				_ = writer.Close() // Preserve staged bytes for the next resume attempt
			} else {
				_ = writer.Discard()
			}
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build download request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	if resumeOffset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", resumeOffset))
		if resumeETag != "" {
			req.Header.Set("If-Range", resumeETag)
		}
	}

	resp, err := c.client.Do(req)
	if err != nil {
		if tracker != nil {
			tracker.stop()
		}
		return "", fmt.Errorf("download request failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusOK && resumeOffset > 0 {
		// Server ignored the conditional Range request; staged bytes cannot be
		// safely combined with a fresh full-body response.
		if tracker != nil {
			tracker.stop()
		}
		if resumable {
			_ = os.Remove(partPath)
			_ = c.store.DeleteCheckpoint(opts.JobID)
		}
		return "", ErrResourceChanged
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if tracker != nil {
			tracker.stop()
		}
		return "", fmt.Errorf("server responded with status %d: %s", resp.StatusCode, resp.Status)
	}

	buf := make([]byte, 32*1024) // 32 KB streaming buffer
	offset := resumeOffset

	var src io.Reader = resp.Body
	if opts.RateLimiter != nil && opts.RateLimiter.Limit() > 0 {
		src = NewLimitedReader(ctx, resp.Body, opts.RateLimiter)
	}

	for {
		select {
		case <-ctx.Done():
			if tracker != nil {
				tracker.stop()
				c.saveSingleStreamCheckpoint(opts, probe, finalPath, offset, resumeETag)
			}
			return "", ctx.Err()
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			_, writeErr := writer.WriteAt(buf[:n], offset)
			if writeErr != nil {
				if tracker != nil {
					tracker.stop()
					c.saveSingleStreamCheckpoint(opts, probe, finalPath, offset, resumeETag)
				}
				return "", fmt.Errorf("failed to write data at offset %d: %w", offset, writeErr)
			}
			offset += int64(n)
			if tracker != nil {
				tracker.record(0, offset, false)
				tracker.maybePersist(time.Now())
			}
			if opts.ProgressFn != nil {
				opts.ProgressFn(0, n)
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			if tracker != nil {
				tracker.stop()
				c.saveSingleStreamCheckpoint(opts, probe, finalPath, offset, resumeETag)
			}
			return "", fmt.Errorf("error reading stream body: %w", readErr)
		}
	}

	if tracker != nil {
		tracker.stop()
	}

	// Verify integrity on the staged file before it is ever given its final name.
	if err := verifyChecksumBeforeCommit(writer, opts); err != nil {
		if tracker != nil {
			c.saveSingleStreamCheckpoint(opts, probe, finalPath, offset, resumeETag)
		}
		return "", err
	}

	// Commit staged file atomically to final destination
	if err := writer.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit downloaded file: %w", err)
	}

	committed = true
	if resumable {
		_ = c.store.DeleteCheckpoint(opts.JobID)
	}
	return finalPath, nil
}

// saveSingleStreamCheckpoint records the exact byte offset of a single-stream
// transfer. It is called only from the coordinator goroutine after the async
// consumer has been stopped, never from a worker hot path.
func (c *Coordinator) saveSingleStreamCheckpoint(opts DownloadOptions, probe *ResourceInfo, finalPath string, offset int64, etag string) {
	if c.store == nil || opts.JobID == "" {
		return
	}
	end := probe.ContentLength - 1
	if end < offset {
		end = offset
	}
	_ = c.store.SaveCheckpoint(&Checkpoint{
		ID:        opts.JobID,
		URL:       probe.FinalURL,
		FinalPath: finalPath,
		TotalSize: probe.ContentLength,
		ETag:      etag,
		Chunks:    []Chunk{{Index: 0, Start: 0, End: end, Current: offset}},
		UpdatedAt: time.Now(),
	})
}

// verifyChecksumBeforeCommit hashes the staged .gofetch.part file and compares
// it with the expected digest before the file is atomically promoted to its
// final name. On mismatch the part file is left intact and the final file is
// never created or replaced. It is a no-op when no checksum was requested.
func verifyChecksumBeforeCommit(writer *storage.FileWriter, opts DownloadOptions) error {
	if opts.Checksum == "" {
		return nil
	}

	algo := storage.AlgorithmSHA256
	if strings.EqualFold(strings.TrimSpace(opts.ChecksumAlgo), "md5") {
		algo = storage.AlgorithmMD5
	}

	if err := writer.Sync(); err != nil {
		return fmt.Errorf("failed to sync part file before checksum verification: %w", err)
	}

	valid, err := storage.VerifyFile(writer.PartPath(), opts.Checksum, algo)
	if err != nil {
		return fmt.Errorf("checksum verification failed: %w", err)
	}
	if !valid {
		return fmt.Errorf("checksum mismatch: expected %s (%s)", strings.TrimSpace(opts.Checksum), algo)
	}
	return nil
}
