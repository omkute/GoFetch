package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"

	"github.com/omkute/gofetch/internal/storage"
	"golang.org/x/sync/errgroup"
)

// DefaultConcurrency is the fallback worker count when not explicitly specified.
const DefaultConcurrency = 4

// DownloadParallel executes a concurrent, multi-chunk partitioned download.
// If the target server lacks byte Range support, it automatically downgrades to
// single-stream download without breaking invariant safety.
func (c *Coordinator) DownloadParallel(ctx context.Context, probe *ResourceInfo, destDir string, numWorkers int) (string, error) {
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

	chunks, err := PartitionFile(probe.ContentLength, numWorkers)
	if err != nil {
		return "", fmt.Errorf("failed to partition download: %w", err)
	}

	absDir, err := CleanDestinationPath(destDir)
	if err != nil {
		return "", err
	}

	finalPath := filepath.Join(absDir, probe.Filename)

	// Preallocate target disk space upfront
	writer, err := storage.NewFileWriter(finalPath, probe.ContentLength)
	if err != nil {
		return "", fmt.Errorf("failed to preallocate staging file: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = writer.Discard()
		}
	}()

	downloadURL := probe.FinalURL
	if downloadURL == "" {
		downloadURL = probe.URL
	}

	g, gCtx := errgroup.WithContext(ctx)
	for i := range chunks {
		chunk := &chunks[i]
		g.Go(func() error {
			return downloadChunk(gCtx, c.client, downloadURL, chunk, writer)
		})
	}

	if err := g.Wait(); err != nil {
		return "", fmt.Errorf("parallel download error: %w", err)
	}

	// Flush dirty page buffers and commit staging .part file
	if err := writer.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit parallel download: %w", err)
	}

	committed = true
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
