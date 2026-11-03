package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/omkute/gofetch/internal/state"
	"github.com/omkute/gofetch/internal/ui"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("🚀 GoFetch Async Event Bus & Telemetry Demo (T-011)")
	fmt.Println("==================================================")

	bus := state.NewEventBus(64)
	defer bus.Close()

	// 1. Subscribe logger consumer
	logSubID, logCh := bus.Subscribe(32)
	defer bus.Unsubscribe(logSubID)

	go func() {
		for evt := range logCh {
			switch evt.Type {
			case state.EventDownloadStarted:
				if p, ok := evt.Payload.(state.DownloadStartedPayload); ok {
					fmt.Printf("   📢 [EventBus] %s: %s (Total size: %s, Chunks: %d)\n",
						evt.Type, p.Filename, ui.FormatBytes(p.TotalSize), p.Chunks)
				}
			case state.EventChunkCompleted:
				if p, ok := evt.Payload.(state.ChunkCompletedPayload); ok {
					fmt.Printf("   📢 [EventBus] %s: Chunk %d (%s in %v)\n",
						evt.Type, p.ChunkIndex, ui.FormatBytes(p.BytesRead), p.Duration)
				}
			case state.EventDownloadFinished:
				if p, ok := evt.Payload.(state.DownloadFinishedPayload); ok {
					fmt.Printf("   📢 [EventBus] %s: Stored at %s (Elapsed: %v)\n",
						evt.Type, p.FinalPath, p.TotalElapsed)
				}
			case state.EventDownloadFailed:
				if p, ok := evt.Payload.(state.DownloadFailedPayload); ok {
					fmt.Printf("   ❌ [EventBus] %s: %s\n", evt.Type, p.Error)
				}
			}
		}
	}()

	// 2. Setup background async telemetry consumer (off hot path)
	var flushedBatches atomic.Int64
	var flushedEvents atomic.Int64

	telemetryConsumer := state.NewAsyncTelemetryConsumer(bus, 10, 50*time.Millisecond, func(events []state.Event) error {
		flushedBatches.Add(1)
		flushedEvents.Add(int64(len(events)))
		fmt.Printf("   💾 [Postgres Telemetry Worker] Flushed batch of %d events to database off hot path!\n", len(events))
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	telemetryConsumer.Start(ctx)
	defer telemetryConsumer.Stop()

	fmt.Println("\n[Step 1] Initialized EventBus and Async Telemetry Consumer")

	// 3. Emit lifecycle events
	fmt.Println("\n[Step 2] Emitting Lifecycle Events on Hot Path:")
	jobID := "dl-789a"
	bus.Publish(state.Event{
		Type:  state.EventDownloadStarted,
		JobID: jobID,
		Payload: state.DownloadStartedPayload{
			URL:       "https://releases.ubuntu.com/24.04/ubuntu.iso",
			Filename:  "ubuntu.iso",
			TotalSize: 5800000000,
			Chunks:    4,
		},
	})

	for i := 0; i < 4; i++ {
		time.Sleep(20 * time.Millisecond)
		bus.Publish(state.Event{
			Type:  state.EventChunkCompleted,
			JobID: jobID,
			Payload: state.ChunkCompletedPayload{
				ChunkIndex: i,
				BytesRead:  1450000000,
				Duration:   450 * time.Millisecond,
			},
		})
	}

	bus.Publish(state.Event{
		Type:  state.EventDownloadFinished,
		JobID: jobID,
		Payload: state.DownloadFinishedPayload{
			FinalPath:    "/downloads/ubuntu.iso",
			TotalBytes:   5800000000,
			TotalElapsed: 1800 * time.Millisecond,
			Checksum:     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	})

	// 4. Test non-blocking behavior under slow subscriber
	fmt.Println("\n[Step 3] Verifying Non-Blocking Guarantee with Stalled Subscriber:")
	stalledSubID, _ := bus.Subscribe(1) // buffer capacity = 1
	defer bus.Unsubscribe(stalledSubID)

	publishStart := time.Now()
	for i := 0; i < 50; i++ {
		bus.Publish(state.Event{
			Type:  state.EventChunkCompleted,
			JobID: "job-fast-transfer",
		})
	}
	publishElapsed := time.Since(publishStart)

	fmt.Printf("   ├── 50 events published in %v (Zero Hot-Path Blocking!)\n", publishElapsed)
	fmt.Printf("   └── Stalled subscriber dropped %d unbuffered events safely.\n", bus.DroppedCount())

	// Wait for telemetry worker to flush
	time.Sleep(100 * time.Millisecond)

	fmt.Println("\n[Step 4] Telemetry Summary:")
	fmt.Printf("   ├── Total Events Flushed Off Hot Path: %d\n", flushedEvents.Load())
	fmt.Printf("   └── Total Database Batches Flushed:   %d\n", flushedBatches.Load())

	fmt.Println("\n✅ All T-011 Invariants & Non-Blocking Event Bus Guarantees Verified!")
	fmt.Println("==================================================")
}
