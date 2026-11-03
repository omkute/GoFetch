package storage_test

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/omkute/gofetch/internal/storage"
)

func TestNewFileWriter_Preallocation(t *testing.T) {
	tempDir := t.TempDir()
	finalPath := filepath.Join(tempDir, "downloads", "testfile.bin")
	totalSize := int64(1024 * 1024 * 5) // 5 MB

	writer, err := storage.NewFileWriter(finalPath, totalSize)
	if err != nil {
		t.Fatalf("unexpected error creating FileWriter: %v", err)
	}
	defer func() {
		_ = writer.Close()
	}()

	partPath := writer.PartPath()
	if expected := finalPath + storage.PartFileExtension; partPath != expected {
		t.Fatalf("expected PartPath to be %q, got %q", expected, partPath)
	}

	info, err := os.Stat(partPath)
	if err != nil {
		t.Fatalf("failed to stat part file %q: %v", partPath, err)
	}

	if info.Size() != totalSize {
		t.Fatalf("expected preallocated size %d, got %d", totalSize, info.Size())
	}

	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Fatalf("final destination file should not exist prior to commit")
	}
}

func TestFileWriter_ConcurrentDisjointWrites(t *testing.T) {
	tempDir := t.TempDir()
	finalPath := filepath.Join(tempDir, "concurrent.bin")

	const (
		numChunks = 8
		chunkSize = 256 * 1024 // 256 KB per chunk
		totalSize = int64(numChunks * chunkSize)
	)

	writer, err := storage.NewFileWriter(finalPath, totalSize)
	if err != nil {
		t.Fatalf("failed to create FileWriter: %v", err)
	}

	expectedPayload := make([]byte, totalSize)
	if _, err := rand.Read(expectedPayload); err != nil {
		t.Fatalf("failed to generate random payload: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, numChunks)

	for i := 0; i < numChunks; i++ {
		wg.Add(1)
		chunkIdx := i
		go func() {
			defer wg.Done()
			offset := int64(chunkIdx * chunkSize)
			data := expectedPayload[offset : offset+chunkSize]

			n, writeErr := writer.WriteAt(data, offset)
			if writeErr != nil {
				errCh <- writeErr
				return
			}
			if n != len(data) {
				t.Errorf("chunk %d: expected to write %d bytes, wrote %d", chunkIdx, len(data), n)
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for writeErr := range errCh {
		t.Fatalf("concurrent write error: %v", writeErr)
	}

	// Commit temporary part file to final destination
	if err := writer.Commit(); err != nil {
		t.Fatalf("failed to commit file: %v", err)
	}

	// Staging part file should no longer exist
	if _, err := os.Stat(writer.PartPath()); !os.IsNotExist(err) {
		t.Fatalf("part file %q still exists after commit", writer.PartPath())
	}

	// Final destination file must exist and contain exact byte sequence
	finalContent, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed to read committed final file: %v", err)
	}

	if int64(len(finalContent)) != totalSize {
		t.Fatalf("expected final file size %d, got %d", totalSize, len(finalContent))
	}

	if !bytes.Equal(finalContent, expectedPayload) {
		t.Fatal("committed file content does not match expected payload (data corruption)")
	}
}

func TestFileWriter_Discard(t *testing.T) {
	tempDir := t.TempDir()
	finalPath := filepath.Join(tempDir, "discard.bin")

	writer, err := storage.NewFileWriter(finalPath, 1024)
	if err != nil {
		t.Fatalf("failed to create FileWriter: %v", err)
	}

	partPath := writer.PartPath()
	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf("part file should exist before discard: %v", err)
	}

	if err := writer.Discard(); err != nil {
		t.Fatalf("unexpected error discarding writer: %v", err)
	}

	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("part file should be deleted after Discard()")
	}

	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Fatalf("final file should not exist after Discard()")
	}
}
