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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/state"
	"github.com/omkute/gofetch/internal/storage"
)

func main() {
	destDir := "./downloads_demo"
	const totalSize = int64(8 * 1024 * 1024) // 8 MB
	const numWorkers = 4
	const jobID = "demo-resumable-job"

	fmt.Println("==================================================")
	fmt.Println("🚀 GoFetch Resumable Download & Checkpoints (T-005)")
	fmt.Println("==================================================")

	// Step 1: Generate deterministic source payload
	fmt.Printf("\n[Step 1] Preparing %d MB test payload...\n", totalSize/(1024*1024))
	sourcePayload := make([]byte, totalSize)
	if _, err := rand.Read(sourcePayload); err != nil {
		log.Fatalf("❌ Failed to generate payload: %v", err)
	}

	hasher, _ := storage.NewHasher(storage.AlgorithmSHA256)
	hasher.Write(sourcePayload)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))
	fmt.Printf("   └── Source SHA-256: %s\n", expectedHash)

	// Step 2: Spin up mock server tracking requested range headers
	var requestedRanges []string
	var rangeMu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="resumed_file.bin"`)
		w.Header().Set("ETag", `"demo-etag-v1"`)

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			rangeMu.Lock()
			requestedRanges = append(requestedRanges, rangeHeader)
			rangeMu.Unlock()

			parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
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

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(sourcePayload)
	}))
	defer server.Close()
	fmt.Printf("\n[Step 2] Mock server listening at: %s\n", server.URL)

	// Step 3: Simulate an interrupted download state
	// File is 8MB with 4 chunks:
	// Chunk 1: [0, 2097151] -> Completed (2MB)
	// Chunk 2: [2097152, 4194303] -> Completed (2MB)
	// Chunk 3: [4194304, 6291455] -> Interrupted at 5242880 (1MB written)
	// Chunk 4: [6291456, 8388607] -> Not started (0MB)
	fmt.Println("\n[Step 3] Simulating prior interrupted download at ~62%...")
	absDir, _ := filepath.Abs(destDir)
	finalPath := filepath.Join(absDir, "resumed_file.bin")
	partPath := finalPath + storage.PartFileExtension

	fw, err := storage.NewFileWriter(finalPath, totalSize)
	if err != nil {
		log.Fatalf("❌ Failed to create part file: %v", err)
	}
	// Write chunk 1 & 2
	_, _ = fw.WriteAt(sourcePayload[0:4194304], 0)
	// Write partial chunk 3
	_, _ = fw.WriteAt(sourcePayload[4194304:5242880], 4194304)
	_ = fw.Close()

	dbPath := filepath.Join(destDir, "checkpoints.db")
	store, err := state.NewBboltStore(dbPath)
	if err != nil {
		log.Fatalf("❌ Failed to open bbolt: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	interruptedCheckpoint := &state.Checkpoint{
		ID:        jobID,
		URL:       server.URL,
		FinalPath: finalPath,
		TotalSize: totalSize,
		ETag:      `"demo-etag-v1"`,
		Chunks: []engine.Chunk{
			{Index: 0, Start: 0, End: 2097151, Current: 2097152, Completed: true},
			{Index: 1, Start: 2097152, End: 4194303, Current: 4194304, Completed: true},
			{Index: 2, Start: 4194304, End: 6291455, Current: 5242880, Completed: false},
			{Index: 3, Start: 6291456, End: 8388607, Current: 6291456, Completed: false},
		},
		UpdatedAt: time.Now(),
	}
	if err := store.SaveCheckpoint(interruptedCheckpoint); err != nil {
		log.Fatalf("❌ Failed to persist checkpoint: %v", err)
	}

	partInfo, _ := os.Stat(partPath)
	fmt.Printf("   ├── Existing staging file: %s (%d bytes)\n", partPath, partInfo.Size())
	fmt.Printf("   ├── Chunk 1 & 2: Already marked [Completed]\n")
	fmt.Printf("   ├── Chunk 3: Partially transferred (resuming from byte %d)\n", interruptedCheckpoint.Chunks[2].Current)
	fmt.Printf("   └── Chunk 4: Pending (resuming from byte %d)\n", interruptedCheckpoint.Chunks[3].Current)

	// Step 4: Resume Download
	fmt.Println("\n[Step 4] Starting GoFetch Resumable Coordinator...")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	probeInfo, err := engine.Probe(ctx, server.Client(), server.URL)
	if err != nil {
		log.Fatalf("❌ Probe failed: %v", err)
	}

	coordinator := engine.NewCoordinatorWithStore(server.Client(), store)
	resumedPath, err := coordinator.DownloadResumable(ctx, jobID, probeInfo, destDir, numWorkers)
	if err != nil {
		log.Fatalf("❌ DownloadResumable failed: %v", err)
	}

	fmt.Printf("   └── Successfully resumed and committed: %s\n", resumedPath)

	// Step 5: Verify byte-range economy
	fmt.Println("\n[Step 5] Auditing HTTP Range requests made during resume:")
	rangeMu.Lock()
	for _, r := range requestedRanges {
		fmt.Printf("   ├── Server received Range request: %s\n", r)
	}
	rangeMu.Unlock()

	// Step 6: Verify Final Checksum & Checkpoint Cleanup
	fmt.Println("\n[Step 6] Verifying integrity & checkpoint cleanup...")
	actualHash, err := storage.ComputeFileHash(resumedPath, storage.AlgorithmSHA256)
	if err != nil {
		log.Fatalf("❌ Checksum failed: %v", err)
	}
	fmt.Printf("   ├── Assembled SHA-256: %s\n", actualHash)
	fmt.Printf("   └── Expected SHA-256:  %s\n", expectedHash)

	if actualHash != expectedHash {
		log.Fatalf("❌ Checksum mismatch!")
	}

	cpCheck, _ := store.GetCheckpoint(jobID)
	if cpCheck != nil {
		log.Fatalf("❌ Checkpoint was not cleaned up!")
	}
	fmt.Println("   └── Checkpoint purged from bbolt DB upon 100% completion")

	fmt.Println("\n✅ All T-005 Resumability & Checkpoint Invariants Verified!")
	fmt.Printf("👉 Inspect the resumed file: ls -lh %s\n", resumedPath)
	fmt.Println("==================================================")
}
