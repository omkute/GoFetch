package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/omkute/gofetch/internal/storage"
	"golang.org/x/sync/errgroup"
)

// DefaultConcurrency is the fallback worker count when not explicitly specified.
const DefaultConcurrency = 4

// ErrResourceChanged indicates an If-Range revalidation determined that the
// remote resource changed after a download was interrupted. Staged bytes must
// not be reused; the caller should retry from scratch.
var ErrResourceChanged = errors.New("resource changed during download; partial data cannot be resumed")

// ErrRangeUnsupported indicates the server ignored a Range request and returned
// the whole entity with 200 OK. The parallel transfer must be discarded and the
// download retried as a single stream.
var ErrRangeUnsupported = errors.New("server ignored Range request; download must fall back to single stream")

// DownloadParallel executes a concurrent, multi-chunk partitioned download.
// If the target server lacks byte Range support, it automatically downgrades to
// single-stream download without breaking invariant safety.
func (c *Coordinator) DownloadParallel(ctx context.Context, probe *ResourceInfo, destDir string, numWorkers int) (string, error) {
	return c.DownloadWithOptions(ctx, probe, DownloadOptions{
		DestDir:    destDir,
		MaxWorkers: numWorkers,
	})
}

// DownloadResumable executes or resumes a multi-chunk partitioned download using
// persisted checkpoints and transient error retries.
func (c *Coordinator) DownloadResumable(ctx context.Context, jobID string, probe *ResourceInfo, destDir string, numWorkers int) (string, error) {
	return c.DownloadWithOptions(ctx, probe, DownloadOptions{
		JobID:      jobID,
		DestDir:    destDir,
		MaxWorkers: numWorkers,
	})
}

// revalidateResource sends an If-Range/ETag conditional request before a resume.
// Status 206 means the entity is unchanged and staged offsets remain valid;
// status 200 means it changed and the staged bytes must be discarded.
func revalidateResource(ctx context.Context, client *http.Client, url, etag, userAgent string, offset int64) (bool, error) {
	if etag == "" {
		return true, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset))
	req.Header.Set("If-Range", etag)
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		return true, nil
	case http.StatusOK:
		return false, nil
	default:
		return false, fmt.Errorf("revalidation returned status %d", resp.StatusCode)
	}
}

// DownloadWithOptions executes a download according to the provided DownloadOptions,
// honoring sequential streaming mode, concurrency limiters, and resumable checkpoints.
func (c *Coordinator) DownloadWithOptions(ctx context.Context, probe *ResourceInfo, opts DownloadOptions) (string, error) {
	if probe == nil {
		return "", errors.New("probe info cannot be nil")
	}
	userAgent := resolveUserAgent(opts.UserAgent)

	// Architectural Invariant: Safely downgrade if Range headers are not supported or size is unknown
	if !probe.AcceptRanges || probe.ContentLength <= 0 {
		return c.DownloadSingleStreamWithOptions(ctx, probe, opts)
	}

	numWorkers := opts.MaxWorkers
	if numWorkers <= 0 {
		numWorkers = DefaultConcurrency
	}

	absDir, err := CleanDestinationPath(opts.DestDir)
	if err != nil {
		return "", err
	}

	finalPath := filepath.Join(absDir, probe.Filename)
	partPath := finalPath + storage.PartFileExtension
	downloadURL := probe.FinalURL
	if downloadURL == "" {
		downloadURL = probe.URL
	}

	resumable := c.store != nil && opts.JobID != ""

	// 1. Replay a valid checkpoint: part file must be complete in size and the
	// entity must still match the stored ETag (If-Range revalidation).
	var chunks []Chunk
	var resumeETag string
	if resumable {
		if cp, getErr := c.store.GetCheckpoint(opts.JobID); getErr == nil && cp != nil {
			valid := cp.TotalSize == probe.ContentLength
			if valid {
				if info, statErr := os.Stat(partPath); statErr == nil && info.Size() == probe.ContentLength {
					probeOffset := int64(0)
					for i := range cp.Chunks {
						if cp.Chunks[i].Remaining() > 0 {
							probeOffset = cp.Chunks[i].Current
							break
						}
					}
					unchanged, valErr := revalidateResource(ctx, c.client, downloadURL, cp.ETag, userAgent, probeOffset)
					if valErr == nil && unchanged {
						chunks = cp.Chunks
						resumeETag = cp.ETag
					} else {
						valid = false
					}
				} else {
					valid = false
				}
			}
			if !valid {
				_ = os.Remove(partPath)
				_ = c.store.DeleteCheckpoint(opts.JobID)
			}
		}
	}

	// 2. If no valid checkpoint exists, partition from scratch and save initial state
	if len(chunks) == 0 {
		var partErr error
		chunks, partErr = PartitionFile(probe.ContentLength, numWorkers)
		if partErr != nil {
			return "", fmt.Errorf("failed to partition download: %w", partErr)
		}

		if resumable {
			_ = c.store.SaveCheckpoint(&Checkpoint{
				ID:        opts.JobID,
				URL:       probe.FinalURL,
				FinalPath: finalPath,
				TotalSize: probe.ContentLength,
				ETag:      probe.ETag,
				Chunks:    chunks,
				UpdatedAt: time.Now(),
			})
		}
	}

	// 3. Preallocate target disk space upfront
	writer, err := storage.NewFileWriter(finalPath, probe.ContentLength)
	if err != nil {
		return "", fmt.Errorf("failed to preallocate staging file: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = writer.Close() // Keep .part file intact on disk for future resumption
		}
	}()

	// Progress offsets are published through a throttled, non-blocking
	// handoff. The background consumer is the only goroutine that writes to
	// the checkpoint store; worker goroutines never touch the DB.
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
			Chunks:    chunks,
			UpdatedAt: time.Now(),
		}
		tracker = newCheckpointTracker(c.store, cp, chunks, 0)
	}

	retryCfg := DefaultRetryConfig()
	var downloadErr error

	// 4. Execute either in sequential or parallel mode
	if opts.Sequential {
		for i := range chunks {
			chunk := &chunks[i]
			if chunk.Completed || chunk.Remaining() <= 0 {
				continue
			}

			if opts.Semaphore != nil {
				if semErr := opts.Semaphore.Acquire(ctx); semErr != nil {
					downloadErr = semErr
					break
				}
			}

			dlErr := ExecuteWithRetry(ctx, retryCfg, func() error {
				return downloadChunk(ctx, c.client, downloadURL, chunk, writer, opts.RateLimiter, opts.ProgressFn, resumeETag, userAgent, tracker)
			})

			if opts.Semaphore != nil {
				opts.Semaphore.Release()
			}

			if dlErr != nil {
				downloadErr = fmt.Errorf("sequential download error on chunk %d: %w", chunk.Index, dlErr)
				break
			}
		}
	} else {
		g, gCtx := errgroup.WithContext(ctx)
		for i := range chunks {
			chunk := &chunks[i]
			if chunk.Completed || chunk.Remaining() <= 0 {
				continue // Skip already completed chunks
			}

			g.Go(func() error {
				if opts.Semaphore != nil {
					if semErr := opts.Semaphore.Acquire(gCtx); semErr != nil {
						return semErr
					}
					defer opts.Semaphore.Release()
				}

				return ExecuteWithRetry(gCtx, retryCfg, func() error {
					return downloadChunk(gCtx, c.client, downloadURL, chunk, writer, opts.RateLimiter, opts.ProgressFn, resumeETag, userAgent, tracker)
				})
			})
		}

		if err := g.Wait(); err != nil {
			downloadErr = fmt.Errorf("parallel download error: %w", err)
		}
	}

	// Wait for all async checkpoint writes before the authoritative final save.
	if tracker != nil {
		tracker.stop()
	}

	// Late downgrade: a server that advertised Range support may still answer a
	// chunk request with 200 OK. Discard the staged bytes and checkpoint, then
	// restart as a single stream before the parallel path can commit anything.
	if errors.Is(downloadErr, ErrRangeUnsupported) {
		_ = writer.Discard()
		if resumable {
			_ = c.store.DeleteCheckpoint(opts.JobID)
		}
		if opts.OnSingleStreamFallback != nil {
			opts.OnSingleStreamFallback()
		}
		return c.DownloadSingleStreamWithOptions(ctx, probe, opts)
	}

	if downloadErr != nil {
		if resumable {
			if errors.Is(downloadErr, ErrResourceChanged) {
				_ = os.Remove(partPath)
				_ = c.store.DeleteCheckpoint(opts.JobID)
			} else {
				_ = c.store.SaveCheckpoint(&Checkpoint{
					ID:        opts.JobID,
					URL:       probe.FinalURL,
					FinalPath: finalPath,
					TotalSize: probe.ContentLength,
					ETag:      probe.ETag,
					Chunks:    chunks,
					UpdatedAt: time.Now(),
				})
			}
		}
		return "", downloadErr
	}

	// 5. Verify integrity on the staged file, then flush and commit it.
	if err := verifyChecksumBeforeCommit(writer, opts); err != nil {
		return "", err
	}
	if err := writer.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit download: %w", err)
	}

	committed = true

	// 6. Clean up completed checkpoint
	if resumable {
		_ = c.store.DeleteCheckpoint(opts.JobID)
	}

	return finalPath, nil
}

// downloadChunk streams an individual byte range [Current, End] directly into disk via WriteAt.
func downloadChunk(ctx context.Context, client *http.Client, url string, chunk *Chunk, writer *storage.FileWriter, limiter *RateLimiter, progressFn func(chunkIndex int, bytesRead int), ifRange, userAgent string, tracker *checkpointTracker) error {
	if chunk.Completed || chunk.Remaining() <= 0 {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("chunk %d: request creation error: %w", chunk.Index, err)
	}

	req.Header.Set("Range", chunk.RangeHeader())
	req.Header.Set("User-Agent", userAgent)
	if ifRange != "" {
		req.Header.Set("If-Range", ifRange)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("chunk %d: network error: %w", chunk.Index, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// Invariant: Range request must return 206 Partial Content (or 200 if whole file requested as 1 chunk)
	if resp.StatusCode != http.StatusPartialContent {
		// If-Range revalidation takes precedence: a 200 means the entity changed,
		// so staged bytes must never be reused.
		if ifRange != "" && resp.StatusCode == http.StatusOK {
			return ErrResourceChanged
		}
		if resp.StatusCode == http.StatusOK && chunk.Start == 0 && chunk.Remaining() == resp.ContentLength {
			// Acceptable single chunk covering whole file
		} else if resp.StatusCode == http.StatusOK && ifRange == "" {
			// The server advertised Range support but ignored this request.
			// Downgrade to a single-stream download from scratch.
			return ErrRangeUnsupported
		} else {
			return &HTTPStatusError{StatusCode: resp.StatusCode, Status: resp.Status}
		}
	}

	var src io.Reader = resp.Body
	if limiter != nil && limiter.Limit() > 0 {
		src = NewLimitedReader(ctx, resp.Body, limiter)
	}

	buf := make([]byte, 32*1024) // 32 KB chunk buffer
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			// Enforce non-overlapping boundary bounds
			if chunk.Current+int64(n)-1 > chunk.End {
				n = int(chunk.End - chunk.Current + 1)
			}

			_, writeErr := writer.WriteAt(buf[:n], chunk.Current)
			if writeErr != nil {
				return fmt.Errorf("chunk %d: write error at offset %d: %w", chunk.Index, chunk.Current, writeErr)
			}
			chunk.Current += int64(n)
			if tracker != nil {
				tracker.record(chunk.Index, chunk.Current, chunk.Current > chunk.End)
				tracker.maybePersist(time.Now())
			}
			if progressFn != nil {
				progressFn(chunk.Index, n)
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("chunk %d: read error: %w", chunk.Index, readErr)
		}
	}

	if chunk.Current > chunk.End {
		chunk.Completed = true
		if tracker != nil {
			tracker.record(chunk.Index, chunk.Current, true)
		}
	}

	return nil
}
