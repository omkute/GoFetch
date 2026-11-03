package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/storage"
)

func main() {
	destDir := "./downloads_demo"
	const totalSize = int64(10 * 1024 * 1024) // 10 MB payload
	const numWorkers = 4

	fmt.Println("==================================================")
	fmt.Println("🚀 GoFetch Multi-Part Parallel Chunk Downloader (T-004)")
	fmt.Println("==================================================")

	// Step 1: Generate deterministic test payload
	fmt.Printf("\n[Step 1] Generating %d MB in-memory test payload...\n", totalSize/(1024*1024))
	sourcePayload := make([]byte, totalSize)
	if _, err := rand.Read(sourcePayload); err != nil {
		log.Fatalf("❌ Failed to generate payload: %v", err)
	}

	sourceHasher, _ := storage.NewHasher(storage.AlgorithmSHA256)
	sourceHasher.Write(sourcePayload)
	expectedHash := fmt.Sprintf("%x", sourceHasher.Sum(nil))
	fmt.Printf("   └── Source SHA-256 Digest: %s\n", expectedHash)

	// Step 2: Spin up mock HTTP server with Range header support
	fmt.Println("\n[Step 2] Launching mock HTTP server supporting Accept-Ranges: bytes...")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="large_payload_10mb.bin"`)

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(sourcePayload)
			return
		}

		// Parse Range: bytes=start-end
		parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
		if len(parts) == 2 {
			start, _ := strconv.ParseInt(parts[0], 10, 64)
			end, _ := strconv.ParseInt(parts[1], 10, 64)
			if start >= 0 && end < totalSize && start <= end {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
				w.Header().Set("Content-Length", strconv.FormatInt((end-start)+1, 10))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.Copy(w, bytes.NewReader(sourcePayload[start:end+1]))
				return
			}
		}

		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}))
	defer server.Close()
	fmt.Printf("   └── Server listening at: %s\n", server.URL)

	// Step 3: Probe the server
	fmt.Println("\n[Step 3] Probing remote resource...")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, server.Client(), server.URL)
	if err != nil {
		log.Fatalf("❌ Probe failed: %v", err)
	}
	fmt.Printf("   ├── Accept-Ranges: %v\n", probeInfo.AcceptRanges)
	fmt.Printf("   ├── Content-Length: %d bytes (%.2f MB)\n", probeInfo.ContentLength, float64(probeInfo.ContentLength)/(1024*1024))
	fmt.Printf("   └── Sanitized Filename: %s\n", probeInfo.Filename)

	// Step 4: Partition file into chunks
	chunks, err := engine.PartitionFile(probeInfo.ContentLength, numWorkers)
	if err != nil {
		log.Fatalf("❌ Partition failed: %v", err)
	}
	fmt.Printf("\n[Step 4] Partitioned file into %d deterministic closed byte ranges:\n", len(chunks))
	for _, c := range chunks {
		fmt.Printf("   ├── Chunk %d: [%d, %d] (size: %d bytes)\n", c.Index+1, c.Start, c.End, c.Size())
	}

	// Step 5: Execute Parallel Chunk Download
	fmt.Printf("\n[Step 5] Streaming chunks simultaneously with %d parallel workers...\n", numWorkers)
	startTime := time.Now()
	coordinator := engine.NewCoordinator(server.Client())
	downloadedPath, err := coordinator.DownloadParallel(ctx, probeInfo, destDir, numWorkers)
	if err != nil {
		log.Fatalf("❌ DownloadParallel failed: %v", err)
	}
	elapsed := time.Since(startTime)
	fmt.Printf("   └── Download complete in %v (throughput: %.2f MB/s)\n",
		elapsed, (float64(totalSize)/(1024*1024))/elapsed.Seconds())

	// Step 6: Verify Integrity
	fmt.Println("\n[Step 6] Verifying Bit-for-Bit Checksum Integrity...")
	actualHash, err := storage.ComputeFileHash(downloadedPath, storage.AlgorithmSHA256)
	if err != nil {
		log.Fatalf("❌ Failed to compute file hash: %v", err)
	}
	fmt.Printf("   ├── Calculated SHA-256: %s\n", actualHash)
	fmt.Printf("   └── Expected SHA-256:   %s\n", expectedHash)

	if actualHash != expectedHash {
		log.Fatalf("❌ CHECKSUM MISMATCH! Data corruption detected.")
	}

	// Verify no remaining .part file
	partFile := downloadedPath + storage.PartFileExtension
	if _, err := os.Stat(partFile); !os.IsNotExist(err) {
		log.Fatalf("❌ Staging .part file was not cleaned up!")
	}
	fmt.Printf("   └── Staging file (%s) safely committed\n", partFile)

	fmt.Println("\n✅ All T-004 Parallel Download Invariants Verified Successfully!")
	fmt.Printf("👉 Inspect the 10MB downloaded file: ls -lh %s\n", downloadedPath)
	fmt.Println("==================================================")
}
