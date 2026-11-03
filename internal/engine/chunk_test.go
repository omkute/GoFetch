package engine_test

import (
	"testing"

	"github.com/omkute/gofetch/internal/engine"
)

func TestPartitionFile_ExactDivision(t *testing.T) {
	totalSize := int64(100)
	numChunks := 4

	chunks, err := engine.PartitionFile(totalSize, numChunks)
	if err != nil {
		t.Fatalf("unexpected error partitioning file: %v", err)
	}

	if len(chunks) != numChunks {
		t.Fatalf("expected %d chunks, got %d", numChunks, len(chunks))
	}

	expected := []struct {
		start int64
		end   int64
	}{
		{0, 24},
		{25, 49},
		{50, 74},
		{75, 99},
	}

	for i, exp := range expected {
		if chunks[i].Start != exp.start || chunks[i].End != exp.end {
			t.Errorf("chunk %d: expected [%d, %d], got [%d, %d]",
				i, exp.start, exp.end, chunks[i].Start, chunks[i].End)
		}
		if chunks[i].Size() != (exp.end - exp.start + 1) {
			t.Errorf("chunk %d: expected size %d, got %d", i, exp.end-exp.start+1, chunks[i].Size())
		}
	}
}

func TestPartitionFile_UnevenDivision(t *testing.T) {
	totalSize := int64(10)
	numChunks := 3

	chunks, err := engine.PartitionFile(totalSize, numChunks)
	if err != nil {
		t.Fatalf("unexpected error partitioning file: %v", err)
	}

	if len(chunks) != numChunks {
		t.Fatalf("expected %d chunks, got %d", numChunks, len(chunks))
	}

	var sumSize int64
	for i, c := range chunks {
		sumSize += c.Size()
		if i > 0 {
			if c.Start != chunks[i-1].End+1 {
				t.Errorf("gap or overlap between chunk %d and %d: prev end=%d, curr start=%d",
					i-1, i, chunks[i-1].End, c.Start)
			}
		}
	}

	if sumSize != totalSize {
		t.Errorf("expected total size %d, got %d", totalSize, sumSize)
	}
	if chunks[len(chunks)-1].End != totalSize-1 {
		t.Errorf("expected last chunk end %d, got %d", totalSize-1, chunks[len(chunks)-1].End)
	}
}

func TestPartitionFile_MoreChunksThanBytes(t *testing.T) {
	totalSize := int64(3)
	numChunks := 10

	chunks, err := engine.PartitionFile(totalSize, numChunks)
	if err != nil {
		t.Fatalf("unexpected error partitioning file: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected clamped chunk count 3, got %d", len(chunks))
	}

	for i, c := range chunks {
		if c.Start != int64(i) || c.End != int64(i) {
			t.Errorf("chunk %d expected [%d, %d], got [%d, %d]", i, i, i, c.Start, c.End)
		}
	}
}

func TestPartitionFile_InvalidInputs(t *testing.T) {
	if _, err := engine.PartitionFile(0, 4); err == nil {
		t.Error("expected error for totalSize=0, got nil")
	}
	if _, err := engine.PartitionFile(-100, 4); err == nil {
		t.Error("expected error for negative totalSize, got nil")
	}
	if _, err := engine.PartitionFile(1024, 0); err == nil {
		t.Error("expected error for numChunks=0, got nil")
	}
	if _, err := engine.PartitionFile(1024, -2); err == nil {
		t.Error("expected error for negative numChunks, got nil")
	}
}

func TestPartitionFile_NoGapsOrOverlapsStress(t *testing.T) {
	testSizes := []int64{1024 * 1024 * 10, 7777777, 10000001, 1048576}
	testWorkerCounts := []int{1, 2, 4, 7, 8, 13, 16, 32}

	for _, size := range testSizes {
		for _, workers := range testWorkerCounts {
			chunks, err := engine.PartitionFile(size, workers)
			if err != nil {
				t.Fatalf("PartitionFile(%d, %d) failed: %v", size, workers, err)
			}

			if chunks[0].Start != 0 {
				t.Errorf("first chunk start must be 0, got %d", chunks[0].Start)
			}

			var totalCalculated int64
			for i, c := range chunks {
				totalCalculated += c.Size()
				if i > 0 {
					if c.Start != chunks[i-1].End+1 {
						t.Fatalf("gap/overlap detected between chunk %d and %d: %d != %d + 1",
							i-1, i, c.Start, chunks[i-1].End)
					}
				}
			}

			if totalCalculated != size {
				t.Fatalf("size mismatch for total=%d, workers=%d: got %d", size, workers, totalCalculated)
			}
			if chunks[len(chunks)-1].End != size-1 {
				t.Fatalf("last chunk end (%d) != totalSize-1 (%d)", chunks[len(chunks)-1].End, size-1)
			}
		}
	}
}
