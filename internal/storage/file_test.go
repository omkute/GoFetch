package storage_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/omkute/gofetch/internal/storage"
)

func TestNewFileWriterWithPartPath_CustomStaging(t *testing.T) {
	tempDir := t.TempDir()
	finalPath := filepath.Join(tempDir, "shared.bin")
	partPath := finalPath + ".job-123" + storage.PartFileExtension
	payload := []byte("staged bytes")

	writer, err := storage.NewFileWriterWithPartPath(finalPath, partPath, int64(len(payload)))
	if err != nil {
		t.Fatalf("NewFileWriterWithPartPath failed: %v", err)
	}
	if writer.PartPath() != partPath {
		t.Fatalf("PartPath() = %q, want %q", writer.PartPath(), partPath)
	}
	if writer.FinalPath() != finalPath {
		t.Fatalf("FinalPath() = %q, want %q", writer.FinalPath(), finalPath)
	}
	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf("expected staged file %q: %v", partPath, err)
	}
	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Fatalf("final file %q must not exist before commit", finalPath)
	}

	if _, err := writer.WriteAt(payload, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	committed, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed to read committed file: %v", err)
	}
	if !bytes.Equal(committed, payload) {
		t.Fatalf("committed bytes = %q, want %q", committed, payload)
	}
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("staging file %q should be gone after commit: %v", partPath, err)
	}
}
