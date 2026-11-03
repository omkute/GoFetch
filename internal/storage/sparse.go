package storage

import (
	"errors"
	"fmt"
	"os"
)

var (
	// ErrClosed is returned when an operation is attempted on a closed FileWriter.
	ErrClosed = errors.New("file writer is closed")
	// ErrNegativeSize is returned when preallocating a negative file size.
	ErrNegativeSize = errors.New("preallocation size cannot be negative")
)

// Preallocate resizes the file to the target size using Truncate.
// On modern OS filesystems, this marks disk space or allocates a sparse file
// preventing fragmentation during concurrent random-access writes.
func Preallocate(f *os.File, size int64) error {
	if f == nil {
		return errors.New("nil file pointer")
	}
	if size < 0 {
		return ErrNegativeSize
	}
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("failed to preallocate file to %d bytes: %w", size, err)
	}
	return nil
}

// WriteAt writes len(p) bytes to the underlying preallocated file at offset off.
// Multiple concurrent goroutines may call WriteAt simultaneously to disjoint byte
// ranges without lock contention.
func (fw *FileWriter) WriteAt(p []byte, off int64) (int, error) {
	fw.mu.RLock()
	defer fw.mu.RUnlock()

	if fw.closed {
		return 0, ErrClosed
	}

	return fw.file.WriteAt(p, off)
}
