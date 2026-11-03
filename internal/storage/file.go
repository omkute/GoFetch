package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PartFileExtension is the mandatory suffix appended to in-progress downloads.
const PartFileExtension = ".gofetch.part"

// FileWriter manages the lifecycle of an active download file on disk,
// enforcing preallocation, disjoint chunk writes, and atomic commitment.
type FileWriter struct {
	mu        sync.RWMutex
	finalPath string
	partPath  string
	file      *os.File
	totalSize int64
	closed    bool
}

// NewFileWriter initializes and preallocates a temporary `.gofetch.part` file.
// It ensures parent directories exist, creates or opens the file in read-write mode,
// and resizes the file to totalSize using sparse preallocation.
func NewFileWriter(finalPath string, totalSize int64) (*FileWriter, error) {
	if finalPath == "" {
		return nil, errors.New("final path cannot be empty")
	}

	cleanFinal := filepath.Clean(finalPath)
	dir := filepath.Dir(cleanFinal)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create target directory %q: %w", dir, err)
	}

	partPath := cleanFinal + PartFileExtension

	// Open with O_RDWR|O_CREATE, mode 0600 (restricted to user)
	file, err := os.OpenFile(partPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open part file %q: %w", partPath, err)
	}

	if totalSize >= 0 {
		if err := Preallocate(file, totalSize); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	return &FileWriter{
		finalPath: cleanFinal,
		partPath:  partPath,
		file:      file,
		totalSize: totalSize,
	}, nil
}

// PartPath returns the path to the active staging part file.
func (fw *FileWriter) PartPath() string {
	return fw.partPath
}

// FinalPath returns the target destination path.
func (fw *FileWriter) FinalPath() string {
	return fw.finalPath
}

// TotalSize returns the preallocated total byte size.
func (fw *FileWriter) TotalSize() int64 {
	return fw.totalSize
}

// Sync flushes file system in-memory copies of buffer data to physical storage.
func (fw *FileWriter) Sync() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return ErrClosed
	}

	return fw.file.Sync()
}

// Commit flushes pending buffers, closes the open handle, and atomically moves
// the staging `.gofetch.part` file to the final destination path via os.Rename.
func (fw *FileWriter) Commit() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return ErrClosed
	}

	// 1. Flush all dirty pages to disk
	if err := fw.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync part file: %w", err)
	}

	// 2. Close file handle before atomic rename
	if err := fw.file.Close(); err != nil {
		return fmt.Errorf("failed to close part file: %w", err)
	}
	fw.closed = true

	// 3. Atomically rename .part -> finalPath
	if err := os.Rename(fw.partPath, fw.finalPath); err != nil {
		return fmt.Errorf("failed to atomically rename %q to %q: %w", fw.partPath, fw.finalPath, err)
	}

	return nil
}

// Close flushes and closes the file without committing or deleting it,
// preserving the in-progress state for future resumption.
func (fw *FileWriter) Close() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return nil
	}

	_ = fw.file.Sync()
	fw.closed = true
	return fw.file.Close()
}

// Discard closes the open file handle and deletes the incomplete `.gofetch.part` file.
func (fw *FileWriter) Discard() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if !fw.closed {
		_ = fw.file.Close()
		fw.closed = true
	}

	if err := os.Remove(fw.partPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete part file %q: %w", fw.partPath, err)
	}

	return nil
}
