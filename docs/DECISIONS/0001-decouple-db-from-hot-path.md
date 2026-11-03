# ADR-0001: Decoupling Database Persistence from the Streaming Hot Path

* **Status**: Accepted
* **Date**: 2026-11-03
* **Deciders**: Principal System Architect, Principal Product Manager

---

## Context
High-performance download managers operate on saturating multi-gigabit connections, transferring hundreds of megabytes per second across multiple parallel TCP streams. In early architecture explorations, using an external relational database (such as PostgreSQL) for real-time task state and metric tracking was evaluated.

However, executing synchronous database writes, ORM calls, or connection-pool queries inside the chunk read loop introduces severe penalties:
* Network I/O latency round-trips for every chunk update.
* Database connection pool exhaustion and transaction lock contention under high concurrency.
* Potential backpressure from slow database operations stalling high-speed network socket reads.

## Decision
We strictly isolate external databases (PostgreSQL) from the critical streaming hot path:
1. **Hot-Path Isolation**: The hot path consists solely of HTTP range reads into pre-allocated memory buffers and direct local disk writes via `os.File.WriteAt`.
2. **Lock-Free Atomic Counters**: High-frequency progress metrics (downloaded bytes, transfer rate) are tracked purely in-memory using `sync/atomic.Int64`.
3. **Asynchronous Event Channel**: When database persistence or historical audit logging is enabled, lifecycle events (`TaskStarted`, `ChunkFinished`, `TaskCompleted`, `TaskFailed`) are published non-blockingly to a buffered Go channel (`chan Event`).
4. **Out-of-Band Persistence**: A decoupled background worker consumes from the event channel and batches writes to PostgreSQL. Database downtime or latency spikes never block, throttle, or fail active downloads.

## Consequences

### Positive
* **Maximum Throughput**: Byte streaming achieves raw disk and network line rates without database latency jitter.
* **Fault Tolerance**: If the database server disconnects or restarts, ongoing downloads continue unimpeded.
* **Zero Hot-Path Allocations**: Progress calculations avoid serialization and heap allocations per network packet.

### Negative / Trade-Offs
* Database records reflect metrics asynchronously with minor propagation delay (milliseconds).
* If the process crashes violently before the async queue flushes, non-critical telemetry events could be dropped (critical download state is separately secured via local checkpoints; see [ADR-0003](file:///Users/omkute/Desktop/Projects/GoFetch/docs/DECISIONS/0003-embedded-bbolt-for-resumability-checkpoints.md)).
