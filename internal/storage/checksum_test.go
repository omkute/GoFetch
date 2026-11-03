package storage_test

import (
	"crypto/md5"  //nolint:gosec // testing MD5 verification
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/omkute/gofetch/internal/storage"
)

func TestComputeFileHash_SHA256(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "sample.txt")
	sampleData := []byte("GoFetch high performance concurrent download manager")

	if err := os.WriteFile(filePath, sampleData, 0600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	expectedHasher := sha256.New()
	expectedHasher.Write(sampleData)
	expectedHex := hex.EncodeToString(expectedHasher.Sum(nil))

	actualHex, err := storage.ComputeFileHash(filePath, storage.AlgorithmSHA256)
	if err != nil {
		t.Fatalf("ComputeFileHash failed: %v", err)
	}

	if actualHex != expectedHex {
		t.Fatalf("expected sha256 %s, got %s", expectedHex, actualHex)
	}
}

func TestComputeFileHash_MD5(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "sample_md5.txt")
	sampleData := []byte("MD5 integrity checksum test payload")

	if err := os.WriteFile(filePath, sampleData, 0600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	expectedHasher := md5.New() //nolint:gosec // testing MD5 verification
	expectedHasher.Write(sampleData)
	expectedHex := hex.EncodeToString(expectedHasher.Sum(nil))

	actualHex, err := storage.ComputeFileHash(filePath, storage.AlgorithmMD5)
	if err != nil {
		t.Fatalf("ComputeFileHash failed: %v", err)
	}

	if actualHex != expectedHex {
		t.Fatalf("expected md5 %s, got %s", expectedHex, actualHex)
	}
}

func TestVerifyFile_Scenarios(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "verify.txt")
	data := []byte("Verified payload contents")

	if err := os.WriteFile(filePath, data, 0600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	hasher := sha256.New()
	hasher.Write(data)
	correctHash := hex.EncodeToString(hasher.Sum(nil))

	// 1. Success matching hash
	match, err := storage.VerifyFile(filePath, correctHash, storage.AlgorithmSHA256)
	if err != nil {
		t.Fatalf("unexpected error verifying file: %v", err)
	}
	if !match {
		t.Fatalf("expected hash to match")
	}

	// 2. Case-insensitive matching
	upperHash := " " + hex.EncodeToString(hasher.Sum(nil)) + " "
	match, err = storage.VerifyFile(filePath, upperHash, storage.AlgorithmSHA256)
	if err != nil {
		t.Fatalf("unexpected error with trimmed check: %v", err)
	}
	if !match {
		t.Fatalf("expected hash to match with whitespace trimming")
	}

	// 3. Mismatched hash
	wrongHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	match, err = storage.VerifyFile(filePath, wrongHash, storage.AlgorithmSHA256)
	if err != nil {
		t.Fatalf("unexpected error verifying mismatched hash: %v", err)
	}
	if match {
		t.Fatalf("expected mismatch to return false, got true")
	}

	// 4. Non-existent file
	_, err = storage.VerifyFile(filepath.Join(tempDir, "non_existent.txt"), correctHash, storage.AlgorithmSHA256)
	if err == nil {
		t.Fatalf("expected error for non-existent file, got nil")
	}

	// 5. Unsupported algorithm
	_, err = storage.VerifyFile(filePath, correctHash, "UNSUPPORTED_ALGO")
	if err == nil {
		t.Fatalf("expected error for unsupported algorithm, got nil")
	}
}
