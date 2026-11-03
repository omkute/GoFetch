package state_test

import (
	"context"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/state"
)

func TestEventBus_PublishAndReceive(t *testing.T) {
	bus := state.NewEventBus(10)
	defer bus.Close()

	subID, ch := bus.Subscribe(10)
	defer bus.Unsubscribe(subID)

	evt := state.Event{
		Type:      state.EventDownloadStarted,
		JobID:     "job-101",
		Timestamp: time.Now(),
		Payload: state.DownloadStartedPayload{
			URL:       "https://example.com/file.zip",
			Filename:  "file.zip",
			TotalSize: 1048576,
			Chunks:    4,
		},
	}

	ok := bus.Publish(evt)
	if !ok {
		t.Fatal("expected publish to succeed")
	}

	select {
	case received := <-ch:
		if received.Type != state.EventDownloadStarted {
			t.Errorf("expected type %s, got %s", state.EventDownloadStarted, received.Type)
		}
		if received.JobID != "job-101" {
			t.Errorf("expected job ID 'job-101', got %s", received.JobID)
		}
		payload, ok := received.Payload.(state.DownloadStartedPayload)
		if !ok || payload.Filename != "file.zip" {
			t.Errorf("unexpected payload: %+v", received.Payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestEventBus_FilterByType(t *testing.T) {
	bus := state.NewEventBus(10)
	defer bus.Close()

	subID, ch := bus.Subscribe(10, state.EventChunkCompleted)
	defer bus.Unsubscribe(subID)

	// Publish an event of a different type
	bus.Publish(state.Event{
		Type:  state.EventDownloadStarted,
		JobID: "job-1",
	})

	// Publish the matching event
	bus.Publish(state.Event{
		Type:  state.EventChunkCompleted,
		JobID: "job-1",
		Payload: state.ChunkCompletedPayload{
			ChunkIndex: 0,
			BytesRead:  1024,
		},
	})

	select {
	case received := <-ch:
		if received.Type != state.EventChunkCompleted {
			t.Errorf("expected chunk completed event, got %s", received.Type)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for filtered event")
	}

	// Verify no additional events are buffered
	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra event received: %+v", extra)
	default:
	}
}

func TestEventBus_NonBlockingWhenSlowSubscriber(t *testing.T) {
	bus := state.NewEventBus(1)
	defer bus.Close()

	// Channel capacity 1, consumer does not read
	subID, _ := bus.Subscribe(1)
	defer bus.Unsubscribe(subID)

	start := time.Now()
	// Rapidly publish 20 events
	for i := 0; i < 20; i++ {
		bus.Publish(state.Event{
			Type:  state.EventChunkCompleted,
			JobID: "job-slow",
		})
	}
	elapsed := time.Since(start)

	// Architectural Invariant: Publish on hot path must NEVER block!
	if elapsed > 100*time.Millisecond {
		t.Fatalf("Publish blocked hot path for %v, must be instantaneous", elapsed)
	}

	if bus.DroppedCount() == 0 {
		t.Errorf("expected dropped event counter > 0, got %d", bus.DroppedCount())
	}
}

func TestEventBus_Unsubscribe(t *testing.T) {
	bus := state.NewEventBus(10)
	defer bus.Close()

	subID, ch := bus.Subscribe(10)

	bus.Publish(state.Event{Type: state.EventDownloadStarted, JobID: "j1"})

	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for first event")
	}

	bus.Unsubscribe(subID)

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after unsubscribe")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for channel close")
	}

	// Subsequent publish should not panic or fail
	bus.Publish(state.Event{Type: state.EventDownloadStarted, JobID: "j2"})
}

func TestEventBus_Close(t *testing.T) {
	bus := state.NewEventBus(10)
	subID, ch := bus.Subscribe(10)
	defer bus.Unsubscribe(subID)

	bus.Close()

	// Subscribers should be notified of bus closure
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to close on bus.Close()")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for channel close on bus.Close()")
	}

	// Publishing after close returns false safely
	if ok := bus.Publish(state.Event{Type: state.EventDownloadStarted}); ok {
		t.Error("expected publish to return false after close")
	}
}

func TestPostgresConsumer_OffHotPathBuffer(t *testing.T) {
	bus := state.NewEventBus(50)
	defer bus.Close()

	var flushedCount int
	flushFn := func(events []state.Event) error {
		flushedCount += len(events)
		return nil
	}

	consumer := state.NewAsyncTelemetryConsumer(bus, 50, 50*time.Millisecond, flushFn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer.Start(ctx)

	// Emit events from hot path
	for i := 0; i < 5; i++ {
		bus.Publish(state.Event{
			Type:  state.EventChunkCompleted,
			JobID: "job-pg",
			Payload: state.ChunkCompletedPayload{
				ChunkIndex: i,
				BytesRead:  4096,
			},
		})
	}

	// Wait for background worker to flush
	time.Sleep(150 * time.Millisecond)
	consumer.Stop()

	if flushedCount != 5 {
		t.Errorf("expected 5 events flushed by background consumer, got %d", flushedCount)
	}
}
