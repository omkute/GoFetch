package bench_test

import (
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/omkute/gofetch/internal/storage"
)

func BenchmarkConcurrentDisjointWrites(b *testing.B) {
	const (
		numChunks = 8
		chunkSize = 64 * 1024 // 64 KB per chunk
		fileSize  = int64(numChunks * chunkSize)
	)

	payload := make([]byte, chunkSize)
	tempDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		finalPath := filepath.Join(tempDir, "bench_file_"+strconv.Itoa(i)+".bin")
		writer, err := storage.NewFileWriter(finalPath, fileSize)
		if err != nil {
			b.Fatalf("failed to create FileWriter: %v", err)
		}
		b.StartTimer()

		var wg sync.WaitGroup
		for c := 0; c < numChunks; c++ {
			wg.Add(1)
			chunkIdx := c
			go func() {
				defer wg.Done()
				offset := int64(chunkIdx * chunkSize)
				_, _ = writer.WriteAt(payload, offset)
			}()
		}
		wg.Wait()

		b.StopTimer()
		_ = writer.Commit()
		b.StartTimer()
	}
}
