package bench_test

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/state"
	"github.com/omkute/gofetch/internal/storage"
)

func BenchmarkHotPathWrites_NoTelemetry(b *testing.B) {
	const (
		numChunks = 8
		chunkSize = 64 * 1024
		fileSize  = int64(numChunks * chunkSize)
	)

	payload := make([]byte, chunkSize)
	tempDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		finalPath := filepath.Join(tempDir, "bench_notelem_"+strconv.Itoa(i)+".bin")
		writer, _ := storage.NewFileWriter(finalPath, fileSize)
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

func BenchmarkHotPathWrites_WithTelemetryEventBus(b *testing.B) {
	const (
		numChunks = 8
		chunkSize = 64 * 1024
		fileSize  = int64(numChunks * chunkSize)
	)

	bus := state.NewEventBus(1024)
	defer bus.Close()

	// Background consumer simulating telemetry persistence off the hot path
	consumer := state.NewAsyncTelemetryConsumer(bus, 100, 50*time.Millisecond, func(events []state.Event) error {
		// Simulates async processing off the hot path
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx)
	defer consumer.Stop()

	payload := make([]byte, chunkSize)
	tempDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		finalPath := filepath.Join(tempDir, "bench_telem_"+strconv.Itoa(i)+".bin")
		writer, _ := storage.NewFileWriter(finalPath, fileSize)
		b.StartTimer()

		var wg sync.WaitGroup
		for c := 0; c < numChunks; c++ {
			wg.Add(1)
			chunkIdx := c
			go func() {
				defer wg.Done()
				offset := int64(chunkIdx * chunkSize)
				_, _ = writer.WriteAt(payload, offset)

				// Emit chunk completion event to bus
				bus.Publish(state.Event{
					Type:  state.EventChunkCompleted,
					JobID: "bench-job",
					Payload: state.ChunkCompletedPayload{
						ChunkIndex: chunkIdx,
						BytesRead:  int64(len(payload)),
					},
				})
			}()
		}
		wg.Wait()

		b.StopTimer()
		_ = writer.Commit()
		b.StartTimer()
	}
}
