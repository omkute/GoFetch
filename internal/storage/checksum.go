package storage

import (
	"crypto/md5"  //nolint:gosec // MD5 is supported as an option for legacy checksum verification
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
)

// HashAlgorithm represents a supported checksum hashing algorithm.
type HashAlgorithm string

const (
	// AlgorithmSHA256 represents SHA-256 hash algorithm.
	AlgorithmSHA256 HashAlgorithm = "sha256"
	// AlgorithmMD5 represents MD5 hash algorithm.
	AlgorithmMD5 HashAlgorithm = "md5"
)

// NewHasher returns a new hash.Hash instance for the requested algorithm.
func NewHasher(algo HashAlgorithm) (hash.Hash, error) {
	switch strings.ToLower(string(algo)) {
	case string(AlgorithmSHA256):
		return sha256.New(), nil
	case string(AlgorithmMD5):
		return md5.New(), nil //nolint:gosec // legacy md5 support
	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %q", algo)
	}
}

// ComputeReaderHash computes the hex-encoded digest of data read from r.
func ComputeReaderHash(r io.Reader, algo HashAlgorithm) (string, error) {
	hasher, err := NewHasher(algo)
	if err != nil {
		return "", err
	}

	if _, err := io.Copy(hasher, r); err != nil {
		return "", fmt.Errorf("failed to stream data to hasher: %w", err)
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// ComputeFileHash streams the file at filePath and computes its hex-encoded hash.
func ComputeFileHash(filePath string, algo HashAlgorithm) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file %q for hashing: %w", filePath, err)
	}
	defer func() {
		_ = f.Close()
	}()

	return ComputeReaderHash(f, algo)
}

// VerifyFile streams the file at filePath, calculates its hash, and compares it
// case-insensitively against expectedHash.
func VerifyFile(filePath, expectedHash string, algo HashAlgorithm) (bool, error) {
	cleanExpected := strings.TrimSpace(strings.ToLower(expectedHash))
	if cleanExpected == "" {
		return false, errors.New("expected hash cannot be empty")
	}

	actualHash, err := ComputeFileHash(filePath, algo)
	if err != nil {
		return false, err
	}

	return strings.ToLower(actualHash) == cleanExpected, nil
}
