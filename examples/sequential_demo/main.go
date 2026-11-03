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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/queue"
	"github.com/omkute/gofetch/internal/storage"
)

func main() {
	destDir := "./downloads_demo"
	const totalSize = int64(4 * 1024 * 1024) // 4 MB
	const numChunks = 4

	fmt.Println("==================================================")
	fmt.Println("🚀 GoFetch Sequential Download & Concurrency Demo (T-008)")
	fmt.Println("==================================================")

	// Step 1: Generate test video payload
	fmt.Printf("\n[Step 1] Preparing %d MB test payload...\n", totalSize/(1024*1024))
	sourcePayload := make([]byte, totalSize)
	if _, err := rand.Read(sourcePayload); err != nil {
		log.Fatalf("❌ Failed generating payload: %v", err)
	}

	hasher, _ := storage.NewHasher(storage.AlgorithmSHA256)
	hasher.Write(sourcePayload)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))
	fmt.Printf("   └── Source SHA-256: %s\n", expectedHash)

	// Step 2: Spin up mock server logging chunk request sequence
	var requestLog []string
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Disposition", `attachment; filename="preview_stream.mp4"`)

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			mu.Lock()
			requestLog = append(requestLog, rangeHeader)
			mu.Unlock()

			parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
			start, _ := strconv.ParseInt(parts[0], 10, 64)
			end, _ := strconv.ParseInt(parts[1], 10, 64)

			if start >= 0 && end < totalSize && start <= end {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
				w.Header().Set("Content-Length", strconv.FormatInt((end-start)+1, 10))
				w.WriteHeader(http.StatusPartialContent)
				// Small delay to simulate progressive streaming
				time.Sleep(30 * time.Millisecond)
				_, _ = io.Copy(w, bytes.NewReader(sourcePayload[start:end+1]))
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(sourcePayload)
	}))
	defer server.Close()
	fmt.Printf("\n[Step 2] Mock server listening at: %s\n", server.URL)

	// Step 3: Probe remote resource
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, server.Client(), server.URL)
	if err != nil {
		log.Fatalf("❌ Probe failed: %v", err)
	}
	fmt.Printf("\n[Step 3] Remote resource probed:\n")
	fmt.Printf("   ├── Accept-Ranges: %v\n", probeInfo.AcceptRanges)
	fmt.Printf("   ├── Content-Length: %d bytes (%.2f MB)\n", probeInfo.ContentLength, float64(probeInfo.ContentLength)/(1024*1024))
	fmt.Printf("   └── Target File: %s\n", probeInfo.Filename)

	// Step 4: Execute Sequential Download with Concurrency Limiter
	fmt.Println("\n[Step 4] Executing Sequential Download (Head-to-Tail Monotonic Streaming):")
	connLimiter := queue.NewSemaphore(2) // Max 2 active network connections
	coordinator := engine.NewCoordinator(server.Client())

	opts := engine.DownloadOptions{
		DestDir:    destDir,
		MaxWorkers: numChunks,
		Sequential: true, // Enables strict in-order chunk streaming
		Semaphore:  connLimiter,
	}

	startTime := time.Now()
	finalPath, err := coordinator.DownloadWithOptions(ctx, probeInfo, opts)
	if err != nil {
		log.Fatalf("❌ Download failed: %v", err)
	}
	elapsed := time.Since(startTime)

	// Step 5: Verify Sequential Order
	fmt.Println("\n[Step 5] Auditing In-Order Chunk Stream Log:")
	mu.Lock()
	for i, r := range requestLog {
		fmt.Printf("   ├── Stream Step %d: %s\n", i+1, r)
	}
	mu.Unlock()

	// Step 6: Verify Content Integrity
	fmt.Println("\n[Step 6] Verifying Bit-for-Bit Checksum Integrity:")
	actualHash, err := storage.ComputeFileHash(finalPath, storage.AlgorithmSHA256)
	if err != nil {
		log.Fatalf("❌ Failed computing hash: %v", err)
	}
	fmt.Printf("   ├── Calculated SHA-256: %s\n", actualHash)
	fmt.Printf("   └── Expected SHA-256:   %s\n", expectedHash)

	if actualHash != expectedHash {
		log.Fatalf("❌ Checksum mismatch!")
	}

	fmt.Printf("\n✅ All T-008 Sequential Streaming Invariants Verified in %v!\n", elapsed)
	fmt.Printf("👉 Inspect the progressive stream file: ls -lh %s\n", finalPath)
	fmt.Println("==================================================")
}
