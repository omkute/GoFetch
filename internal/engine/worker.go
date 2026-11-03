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

// DownloadParallel executes a concurrent, multi-chunk partitioned download.
// If the target server lacks byte Range support, it automatically downgrades to
// single-stream download without breaking invariant safety.
func (c *Coordinator) DownloadParallel(ctx context.Context, probe *ResourceInfo, destDir string, numWorkers int) (string, error) {
	return c.DownloadResumable(ctx, "", probe, destDir, numWorkers)
}

// DownloadResumable executes or resumes a multi-chunk partitioned download using
// persisted checkpoints and transient error retries.
func (c *Coordinator) DownloadResumable(ctx context.Context, jobID string, probe *ResourceInfo, destDir string, numWorkers int) (string, error) {
	if probe == nil {
		return "", errors.New("probe info cannot be nil")
	}

	// Architectural Invariant: Safely downgrade if Range headers are not supported or size is unknown
	if !probe.AcceptRanges || probe.ContentLength <= 0 {
		return c.DownloadSingleStream(ctx, probe, destDir)
	}

	if numWorkers <= 0 {
		numWorkers = DefaultConcurrency
	}

	absDir, err := CleanDestinationPath(destDir)
	if err != nil {
		return "", err
	}

	finalPath := filepath.Join(absDir, probe.Filename)
	partPath := finalPath + storage.PartFileExtension

	var chunks []Chunk

	// 1. Check if an existing checkpoint can be resumed
	if c.store != nil && jobID != "" {
		if cp, getErr := c.store.GetCheckpoint(jobID); getErr == nil && cp != nil {
			if info, statErr := os.Stat(partPath); statErr == nil && info.Size() == probe.ContentLength {
				chunks = cp.Chunks
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

		if c.store != nil && jobID != "" {
			_ = c.store.SaveCheckpoint(&Checkpoint{
				ID:        jobID,
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

	downloadURL := probe.FinalURL
	if downloadURL == "" {
		downloadURL = probe.URL
	}

	retryCfg := DefaultRetryConfig()
	g, gCtx := errgroup.WithContext(ctx)

	for i := range chunks {
		chunk := &chunks[i]
		if chunk.Completed || chunk.Remaining() <= 0 {
			continue // Skip already completed chunks
		}

		g.Go(func() error {
			return ExecuteWithRetry(gCtx, retryCfg, func() error {
				return downloadChunk(gCtx, c.client, downloadURL, chunk, writer)
			})
		})
	}

	if err := g.Wait(); err != nil {
		// Update checkpoint on error or pause before returning
		if c.store != nil && jobID != "" {
			_ = c.store.SaveCheckpoint(&Checkpoint{
				ID:        jobID,
				URL:       probe.FinalURL,
				FinalPath: finalPath,
				TotalSize: probe.ContentLength,
				ETag:      probe.ETag,
				Chunks:    chunks,
				UpdatedAt: time.Now(),
			})
		}
		return "", fmt.Errorf("parallel download error: %w", err)
	}

	// 4. Flush dirty page buffers and commit staging .part file
	if err := writer.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit parallel download: %w", err)
	}

	committed = true

	// 5. Clean up completed checkpoint
	if c.store != nil && jobID != "" {
		_ = c.store.DeleteCheckpoint(jobID)
	}

	return finalPath, nil
}

// downloadChunk streams an individual byte range [Current, End] directly into disk via WriteAt.
func downloadChunk(ctx context.Context, client *http.Client, url string, chunk *Chunk, writer *storage.FileWriter) error {
	if chunk.Completed || chunk.Remaining() <= 0 {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("chunk %d: request creation error: %w", chunk.Index, err)
	}

	req.Header.Set("Range", chunk.RangeHeader())
	req.Header.Set("User-Agent", "GoFetch/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("chunk %d: network error: %w", chunk.Index, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// Invariant: Range request must return 206 Partial Content (or 200 if whole file requested as 1 chunk)
	if resp.StatusCode != http.StatusPartialContent {
		if resp.StatusCode == http.StatusOK && chunk.Start == 0 && chunk.Remaining() == resp.ContentLength {
			// Acceptable single chunk covering whole file
		} else {
			return fmt.Errorf("chunk %d: server returned status %d instead of 206 Partial Content", chunk.Index, resp.StatusCode)
		}
	}

	buf := make([]byte, 32*1024) // 32 KB chunk buffer
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
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
	}

	return nil
}
