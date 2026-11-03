package state

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TelemetryFlushFunc is a callback invoked when a batch of events is flushed.
type TelemetryFlushFunc func(events []Event) error

// AsyncTelemetryConsumer consumes events asynchronously off the transfer hot path
// and batches them for persistent storage or telemetry.
type AsyncTelemetryConsumer struct {
	bus       *EventBus
	subID     string
	ch        <-chan Event
	flushFn   TelemetryFlushFunc
	batchSize int
	flushFreq time.Duration
	stopCh    chan struct{}
	doneCh    chan struct{}
	mu        sync.Mutex
	running   bool
}

// NewAsyncTelemetryConsumer initializes an asynchronous telemetry consumer with a custom flush callback.
func NewAsyncTelemetryConsumer(bus *EventBus, batchSize int, flushFreq time.Duration, flushFn TelemetryFlushFunc) *AsyncTelemetryConsumer {
	if batchSize <= 0 {
		batchSize = 50
	}
	if flushFreq <= 0 {
		flushFreq = 500 * time.Millisecond
	}

	subID, ch := bus.Subscribe(batchSize*2)
	return &AsyncTelemetryConsumer{
		bus:       bus,
		subID:     subID,
		ch:        ch,
		flushFn:   flushFn,
		batchSize: batchSize,
		flushFreq: flushFreq,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Start begins processing events in a dedicated background goroutine.
func (c *AsyncTelemetryConsumer) Start(ctx context.Context) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.mu.Unlock()

	go c.run(ctx)
}

func (c *AsyncTelemetryConsumer) run(ctx context.Context) {
	defer close(c.doneCh)
	defer c.bus.Unsubscribe(c.subID)

	ticker := time.NewTicker(c.flushFreq)
	defer ticker.Stop()

	batch := make([]Event, 0, c.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		toFlush := make([]Event, len(batch))
		copy(toFlush, batch)
		batch = batch[:0]

		if c.flushFn != nil {
			_ = c.flushFn(toFlush)
		}
	}

	for {
		select {
		case <-c.stopCh:
			flush()
			return
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case evt, ok := <-c.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, evt)
			if len(batch) >= c.batchSize {
				flush()
			}
		}
	}
}

// Stop signals the background worker to flush any pending batch and terminate.
func (c *AsyncTelemetryConsumer) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.running = false
	c.mu.Unlock()

	close(c.stopCh)
	<-c.doneCh
}

// PostgresTelemetryConsumer persists telemetry events into PostgreSQL using pgxpool.
type PostgresTelemetryConsumer struct {
	consumer *AsyncTelemetryConsumer
	pool     *pgxpool.Pool
}

// NewPostgresTelemetryConsumer creates a Postgres telemetry consumer connected to pool.
func NewPostgresTelemetryConsumer(bus *EventBus, pool *pgxpool.Pool, batchSize int, flushFreq time.Duration) *PostgresTelemetryConsumer {
	flushFn := func(events []Event) error {
		if pool == nil || len(events) == 0 {
			return nil
		}
		// Batch insert into metrics table
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Execute batch insert in background without touching hot path
		for _, evt := range events {
			_, _ = pool.Exec(ctx,
				"INSERT INTO download_events (job_id, event_type, created_at) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
				evt.JobID, string(evt.Type), evt.Timestamp)
		}
		return nil
	}

	return &PostgresTelemetryConsumer{
		consumer: NewAsyncTelemetryConsumer(bus, batchSize, flushFreq, flushFn),
		pool:     pool,
	}
}

// Start begins the Postgres consumer background loop.
func (p *PostgresTelemetryConsumer) Start(ctx context.Context) {
	p.consumer.Start(ctx)
}

// Stop gracefully stops the Postgres consumer.
func (p *PostgresTelemetryConsumer) Stop() {
	p.consumer.Stop()
}
