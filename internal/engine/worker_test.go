package engine_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/storage"
)

func TestCoordinator_DownloadParallel_Integration(t *testing.T) {
	tempDir := t.TempDir()
	const totalSize = int64(1024 * 1024 * 2) // 2 MB
	const numWorkers = 4

	expectedPayload := make([]byte, totalSize)
	if _, err := rand.Read(expectedPayload); err != nil {
		t.Fatalf("failed to generate random payload: %v", err)
	}

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Accept-Ranges", "bytes")
		header.Set("Content-Length", strconv.FormatInt(totalSize, 10))
		header.Set("Content-Type", "application/octet-stream")
		header.Set("Content-Disposition", `attachment; filename="parallel_test.iso"`)

		if req.Method == http.MethodHead {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		rangeHeader := req.Header.Get("Range")
		if rangeHeader == "" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(bytes.NewReader(expectedPayload)),
				Request:    req,
			}, nil
		}

		// Parse Range: bytes=start-end
		if !strings.HasPrefix(rangeHeader, "bytes=") {
			return &http.Response{
				StatusCode: http.StatusRequestedRangeNotSatisfiable,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
		if len(parts) != 2 {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		if start < 0 || end >= totalSize || start > end {
			return &http.Response{
				StatusCode: http.StatusRequestedRangeNotSatisfiable,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Request:    req,
			}, nil
		}

		slice := expectedPayload[start : end+1]
		rangeRespHeader := make(http.Header)
		rangeRespHeader.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		rangeRespHeader.Set("Content-Length", strconv.Itoa(len(slice)))
		rangeRespHeader.Set("Content-Type", "application/octet-stream")

		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Header:     rangeRespHeader,
			Body:       io.NopCloser(bytes.NewReader(slice)),
			Request:    req,
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, mockClient, "http://mock.local/parallel_test.iso")
	if err != nil {
		t.Fatalf("Probe failed: %v", err)
	}

	if !probeInfo.AcceptRanges {
		t.Fatalf("expected server to support Accept-Ranges")
	}

	coordinator := engine.NewCoordinator(mockClient)
	finalPath, err := coordinator.DownloadParallel(ctx, probeInfo, tempDir, numWorkers)
	if err != nil {
		t.Fatalf("DownloadParallel failed: %v", err)
	}

	expectedFinalPath := filepath.Join(tempDir, "parallel_test.iso")
	if finalPath != expectedFinalPath {
		t.Fatalf("expected finalPath %q, got %q", expectedFinalPath, finalPath)
	}

	// Verify .part staging file was cleaned up
	partFile := finalPath + storage.PartFileExtension
	if _, err := os.Stat(partFile); !os.IsNotExist(err) {
		t.Fatalf("staging part file %q still exists after commit", partFile)
	}

	// Read and verify bit-for-bit file integrity
	downloadedData, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if int64(len(downloadedData)) != totalSize {
		t.Fatalf("expected size %d, got %d", totalSize, len(downloadedData))
	}

	if !bytes.Equal(downloadedData, expectedPayload) {
		t.Fatalf("downloaded file contents do not match expected payload (data corruption)")
	}
}

// A server can advertise Accept-Ranges: bytes yet answer every Range GET with
// a full 200 OK. The coordinator must discard the parallel staging file and
// fall back to a single-stream GET, without bypassing checksum verification.
func TestCoordinator_DownloadWithOptions_LateRangeDowngrade(t *testing.T) {
	payload := []byte("late 200 response to a range request must downgrade to a single stream")
	digest := sha256.Sum256(payload)
	goodChecksum := hex.EncodeToString(digest[:])

	testCases := []struct {
		name     string
		checksum string
		wantErr  bool
	}{
		{name: "valid checksum", checksum: goodChecksum},
		{name: "invalid checksum", checksum: strings.Repeat("0", 64), wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var rangeGETs, plainGETs atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Accept-Ranges", "bytes")
				w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
				if r.Header.Get("Range") != "" {
					rangeGETs.Add(1)
					// Ignore the Range header and return the whole entity with 200 OK.
				} else {
					plainGETs.Add(1)
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(payload)
			}))
			defer server.Close()

			url := server.URL + "/late_downgrade.bin"
			probe := &engine.ResourceInfo{
				URL:           url,
				FinalURL:      url,
				Filename:      "late_downgrade.bin",
				ContentLength: int64(len(payload)),
				AcceptRanges:  true,
			}
			tempDir := t.TempDir()
			finalPath := filepath.Join(tempDir, probe.Filename)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			coordinator := engine.NewCoordinator(server.Client())
			_, err := coordinator.DownloadWithOptions(ctx, probe, engine.DownloadOptions{
				DestDir:      tempDir,
				MaxWorkers:   4,
				Checksum:     tc.checksum,
				ChecksumAlgo: "sha256",
			})

			if rangeGETs.Load() == 0 {
				t.Fatal("expected the parallel path to issue at least one Range GET")
			}
			if plainGETs.Load() == 0 {
				t.Fatal("expected a GET without a Range header, proving single-stream fallback")
			}

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected a checksum mismatch error, got nil")
				}
				if _, statErr := os.Stat(finalPath); !os.IsNotExist(statErr) {
					t.Fatalf("corrupt file must not be committed at %q: %v", finalPath, statErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected single-stream fallback to succeed, got: %v", err)
			}
			data, readErr := os.ReadFile(finalPath)
			if readErr != nil {
				t.Fatalf("failed to read committed file: %v", readErr)
			}
			if !bytes.Equal(data, payload) {
				t.Fatalf("file content mismatch: expected %q, got %q", payload, string(data))
			}
		})
	}
}

func TestCoordinator_DownloadParallel_DowngradeWhenRangeUnsupported(t *testing.T) {
	tempDir := t.TempDir()
	payload := []byte("Fallback single stream content when Range is not supported")

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Accept-Ranges", "none")
		header.Set("Content-Length", strconv.Itoa(len(payload)))
		header.Set("Content-Disposition", `attachment; filename="downgrade.txt"`)

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(bytes.NewReader(payload)),
			Request:    req,
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, mockClient, "http://mock.local/downgrade")
	if err != nil {
		t.Fatalf("Probe failed: %v", err)
	}

	coordinator := engine.NewCoordinator(mockClient)
	// Calling DownloadParallel on a server with Accept-Ranges: none should gracefully fallback to single-stream
	finalPath, err := coordinator.DownloadParallel(ctx, probeInfo, tempDir, 4)
	if err != nil {
		t.Fatalf("DownloadParallel fallback failed: %v", err)
	}

	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed reading committed file: %v", err)
	}

	if !bytes.Equal(data, payload) {
		t.Fatalf("file content mismatch: expected %q, got %q", string(payload), string(data))
	}
}
