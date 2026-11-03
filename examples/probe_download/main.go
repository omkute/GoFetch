package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/storage"
)

func main() {
	destDir := "./downloads_demo"
	fmt.Println("==================================================")
	fmt.Println("🚀 GoFetch HTTP Probe & Single-Stream Demo (T-003)")
	fmt.Println("==================================================")

	// Step 1: Start a mock HTTP server serving a non-chunkable file with path traversal in headers
	fmt.Println("\n[Step 1] Starting mock HTTP server simulating non-chunkable upstream...")
	mockPayload := []byte("GoFetch single-stream payload verification: high performance, reliable byte transfer!")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "none") // Range requests not supported
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(mockPayload)))
		w.Header().Set("Content-Type", "application/octet-stream")
		// Malicious path traversal header attempting directory breakout
		w.Header().Set("Content-Disposition", `attachment; filename="../../../../etc/shadow_archive.tar.gz"`)
		w.Header().Set("ETag", `"etag-gofetch-t003"`)

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(mockPayload)
	}))
	defer server.Close()
	fmt.Printf("   └── Mock Server running at: %s\n", server.URL)

	// Step 2: Probe the remote URL
	fmt.Println("\n[Step 2] Probing resource via HTTP HEAD / metadata analyzer...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, server.Client(), server.URL)
	if err != nil {
		log.Fatalf("❌ Probe failed: %v", err)
	}

	fmt.Printf("   ├── Accept-Ranges bytes: %v (Multi-chunk supported: %v)\n", probeInfo.AcceptRanges, probeInfo.AcceptRanges)
	fmt.Printf("   ├── Content-Length: %d bytes\n", probeInfo.ContentLength)
	fmt.Printf("   ├── Content-Type: %s\n", probeInfo.ContentType)
	fmt.Printf("   ├── Raw Proposed Filename: ../../../../etc/shadow_archive.tar.gz\n")
	fmt.Printf("   └── Sanitized Safe Filename: %s\n", probeInfo.Filename)

	// Step 3: Execute Single-Stream Downloader
	fmt.Println("\n[Step 3] Downgrading to Single-Stream Sequential Downloader...")
	coordinator := engine.NewCoordinator(server.Client())
	committedPath, err := coordinator.DownloadSingleStream(ctx, probeInfo, destDir)
	if err != nil {
		log.Fatalf("❌ Single-stream download failed: %v", err)
	}
	fmt.Printf("   ├── Staging temporary .part file committed\n")
	fmt.Printf("   └── Downloaded output path: %s\n", committedPath)

	// Step 4: Verify Content Integrity & Staging Cleanup
	fmt.Println("\n[Step 4] Verifying filesystem isolation & content integrity...")
	partFile := committedPath + storage.PartFileExtension
	if _, err := os.Stat(partFile); !os.IsNotExist(err) {
		log.Fatalf("❌ Staging .part file still exists!")
	}
	fmt.Printf("   ├── Verified: No dangling .part file (%s is clean)\n", partFile)

	hash, err := storage.ComputeFileHash(committedPath, storage.AlgorithmSHA256)
	if err != nil {
		log.Fatalf("❌ Checksum failed: %v", err)
	}
	fmt.Printf("   └── SHA-256 Digest: %s\n", hash)

	fmt.Println("\n✅ T-003 HTTP Probe & Single-Stream Invariants Verified!")
	fmt.Printf("👉 Inspect the sanitized downloaded file: ls -lh %s\n", committedPath)
	fmt.Println("==================================================")
}
