package engine

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidFileSize is returned when file size is zero or negative.
	ErrInvalidFileSize = errors.New("total file size must be greater than zero")
	// ErrInvalidChunkCount is returned when chunk count is zero or negative.
	ErrInvalidChunkCount = errors.New("number of chunks must be greater than zero")
)

// Chunk represents a discrete closed byte interval [Start, End] assigned to a download worker.
type Chunk struct {
	Index     int   `json:"index"`
	Start     int64 `json:"start"`
	End       int64 `json:"end"`     // inclusive end byte position
	Current   int64 `json:"current"` // current write offset position (starts at Start)
	Completed bool  `json:"completed"`
}

// Size returns the total byte size of this chunk.
func (c *Chunk) Size() int64 {
	if c.End < c.Start {
		return 0
	}
	return (c.End - c.Start) + 1
}

// Remaining returns the remaining unread byte count for this chunk.
func (c *Chunk) Remaining() int64 {
	if c.Completed || c.Current > c.End {
		return 0
	}
	return (c.End - c.Current) + 1
}

// RangeHeader constructs the HTTP Range header string for fetching this chunk.
func (c *Chunk) RangeHeader() string {
	return fmt.Sprintf("bytes=%d-%d", c.Current, c.End)
}

// PartitionFile divides a file of totalSize into numChunks non-overlapping, closed byte ranges.
// It distributes any integer division remainder across the initial chunks, ensuring zero gaps
// and zero overlapping byte boundaries.
func PartitionFile(totalSize int64, numChunks int) ([]Chunk, error) {
	if totalSize <= 0 {
		return nil, ErrInvalidFileSize
	}
	if numChunks <= 0 {
		return nil, ErrInvalidChunkCount
	}

	// Clamp chunk count to totalSize to prevent 0-byte chunks
	if int64(numChunks) > totalSize {
		numChunks = int(totalSize)
	}

	chunks := make([]Chunk, numChunks)
	baseChunkSize := totalSize / int64(numChunks)
	remainder := totalSize % int64(numChunks)

	var currentOffset int64
	for i := 0; i < numChunks; i++ {
		chunkSize := baseChunkSize
		if int64(i) < remainder {
			chunkSize++
		}

		endOffset := currentOffset + chunkSize - 1
		chunks[i] = Chunk{
			Index:   i,
			Start:   currentOffset,
			End:     endOffset,
			Current: currentOffset,
		}
		currentOffset = endOffset + 1
	}

	return chunks, nil
}
