package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/omkute/gofetch/internal/storage"
)

func main() {
	targetDir := "./downloads_demo"
	finalTarget := filepath.Join(targetDir, "sample_5mb.bin")

	const (
		totalSize = int64(5 * 1024 * 1024) // 5 MB
		numChunks = 4
		chunkSize = totalSize / numChunks
	)

	fmt.Println("==================================================")
	fmt.Println("🚀 GoFetch Manual Storage Demo (T-002)")
	fmt.Println("==================================================")

	// Step 1: Preallocation & Staging file
	fmt.Printf("\n[Step 1] Initializing FileWriter for %s (%d bytes)...\n", finalTarget, totalSize)
	writer, err := storage.NewFileWriter(finalTarget, totalSize)
	if err != nil {
		log.Fatalf("❌ Failed to create FileWriter: %v", err)
	}

	partPath := writer.PartPath()
	fmt.Printf("   ├── Staging file created: %s\n", partPath)

	info, err := os.Stat(partPath)
	if err != nil {
		log.Fatalf("❌ Could not stat part file: %v", err)
	}
	fmt.Printf("   └── Preallocated size on disk: %d bytes (%.2f MB)\n", info.Size(), float64(info.Size())/(1024*1024))

	// Step 2: Concurrent Disjoint Chunk Writes
	fmt.Printf("\n[Step 2] Simulating %d concurrent workers writing disjoint byte chunks...\n", numChunks)
	var wg sync.WaitGroup
	for i := 0; i < numChunks; i++ {
		wg.Add(1)
		chunkIndex := i
		go func() {
			defer wg.Done()
			offset := int64(chunkIndex) * chunkSize
			payload := make([]byte, chunkSize)
			if _, err := rand.Read(payload); err != nil {
				log.Printf("❌ Chunk %d error generating bytes: %v", chunkIndex, err)
				return
			}

			n, writeErr := writer.WriteAt(payload, offset)
			if writeErr != nil {
				log.Printf("❌ Chunk %d WriteAt error: %v", chunkIndex, writeErr)
				return
			}
			fmt.Printf("   ├── Worker %d: successfully wrote %d bytes at offset %d\n", chunkIndex+1, n, offset)
		}()
	}
	wg.Wait()

	// Step 3: Atomic Commitment
	fmt.Println("\n[Step 3] Flushed buffers and committing file atomically...")
	if err := writer.Commit(); err != nil {
		log.Fatalf("❌ Commit failed: %v", err)
	}
	fmt.Printf("   ├── Staging file removed: %s\n", partPath)
	fmt.Printf("   └── Final file committed: %s\n", finalTarget)

	// Step 4: Streaming Checksum Verification
	fmt.Println("\n[Step 4] Computing and verifying SHA-256 streaming hash...")
	hashVal, err := storage.ComputeFileHash(finalTarget, storage.AlgorithmSHA256)
	if err != nil {
		log.Fatalf("❌ Checksum computation failed: %v", err)
	}
	fmt.Printf("   └── SHA-256 Digest: %s\n", hashVal)

	valid, err := storage.VerifyFile(finalTarget, hashVal, storage.AlgorithmSHA256)
	if err != nil || !valid {
		log.Fatalf("❌ Checksum verification failed (valid=%v, err=%v)", valid, err)
	}
	fmt.Println("\n✅ All Storage invariants verified successfully!")
	fmt.Printf("👉 You can inspect the generated file directly: ls -lh %s\n", finalTarget)
	fmt.Println("==================================================")
}
