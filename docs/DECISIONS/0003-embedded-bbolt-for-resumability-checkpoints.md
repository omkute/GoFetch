# ADR-0003: Embedded Key-Value Checkpointing (bbolt) for Resumable Downloads

* **Status**: Accepted
* **Date**: 2026-11-03
* **Deciders**: Principal System Architect, Principal Product Manager

---

## Context
Resumability is a primary acceptance criterion for GoFetch. When a download is paused, interrupted by a network drop, or terminated by `SIGINT`, the engine must record the state of all active and completed chunk ranges (`[start, current_offset, end]`), ETag headers, and destination paths.

Three persistence strategies were considered:
1. **Sidecar JSON Metadata Files (`<filename>.meta.json`)**: Simple to inspect, but vulnerable to corruption during sudden power losses and requires rewriting the full file per checkpoint flush.
2. **SQLite Database**: Full SQL query power, but standard Go drivers (`mattn/go-sqlite3`) require CGO, complicating cross-platform static binary builds (`CGO_ENABLED=0`). Pure-Go SQLite alternatives introduce non-trivial binary size and memory overhead.
3. **Embedded Pure-Go Key-Value Store (`go.etcd.io/bbolt`)**: Pure Go, zero external dependencies, ACID crash resilience, memory-mapped I/O, and zero CGO requirement.

## Decision
We select **`go.etcd.io/bbolt`** as the local embedded state and checkpoint engine:
1. **Location & Scope**: State is stored in a single local database file (`~/.gofetch/state.db`).
2. **Bucket Structure**:
   * `tasks`: Task metadata (URL, target path, file size, ETag, checksum, priority).
   * `chunks`: Per-task chunk slices recording verified completed byte boundaries.
3. **Checkpoint Flushing**: Checkpoints are updated on chunk completion or throttled intervals (every 1MB transferred or 1 second) and synced during graceful shutdown (`fsync`).
4. **Resume Verification**: On startup/resume, GoFetch inspects `state.db`, verifies that the `.part` file exists and matches the recorded length, checks if the remote server ETag matches via an HTTP `If-Range` header, and resumes only missing byte spans.

## Consequences

### Positive
* **100% Pure Go & Cross-Platform**: No CGO dependencies; compiles cleanly for Linux, macOS, and Windows.
* **ACID Durability**: Power cuts or process kills cannot corrupt the database.
* **Fast Reads via mmap**: Near-instantaneous recovery lookups on restart.

### Negative / Trade-Offs
* Single-writer concurrency lock inside `bbolt` (mitigated by batching checkpoint updates and offloading heavy metrics to in-memory atomics).
* Binary database format cannot be viewed with a simple text editor (addressed by `gofetch status` and `gofetch list` CLI commands).
