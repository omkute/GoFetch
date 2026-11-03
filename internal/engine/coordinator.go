package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"

	"github.com/omkute/gofetch/internal/storage"
)

// Coordinator manages single-stream and multi-chunk download lifecycles.
type Coordinator struct {
	client *http.Client
}

// NewCoordinator creates a new download coordinator with the provided HTTP client.
func NewCoordinator(client *http.Client) *Coordinator {
	if client == nil {
		client = http.DefaultClient
	}
	return &Coordinator{client: client}
}

// DownloadSingleStream executes a sequential streaming download of a resource,
// writing into a staged `.gofetch.part` file and atomically committing on completion.
func (c *Coordinator) DownloadSingleStream(ctx context.Context, probe *ResourceInfo, destDir string) (string, error) {
	if probe == nil {
		return "", errors.New("probe info cannot be nil")
	}

	absDir, err := CleanDestinationPath(destDir)
	if err != nil {
		return "", err
	}

	finalPath := filepath.Join(absDir, probe.Filename)

	// Preallocate if content length is known (>= 0), otherwise create dynamic staging file
	writer, err := storage.NewFileWriter(finalPath, probe.ContentLength)
	if err != nil {
		return "", fmt.Errorf("failed to initialize staging file: %w", err)
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build download request: %w", err)
	}
	req.Header.Set("User-Agent", "GoFetch/1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("download request failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("server responded with status %d: %s", resp.StatusCode, resp.Status)
	}

	buf := make([]byte, 32*1024) // 32 KB streaming buffer
	var offset int64

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			_, writeErr := writer.WriteAt(buf[:n], offset)
			if writeErr != nil {
				return "", fmt.Errorf("failed to write data at offset %d: %w", offset, writeErr)
			}
			offset += int64(n)
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return "", fmt.Errorf("error reading stream body: %w", readErr)
		}
	}

	// Commit staged file atomically to final destination
	if err := writer.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit downloaded file: %w", err)
	}

	committed = true
	return finalPath, nil
}
