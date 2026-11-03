# Backlog — GoFetch v1 Build Plan

This backlog organizes the implementation of **GoFetch** v1 into 11 dependency-ordered vertical slices. Every slice delivers a fully testable, end-to-end outcome adhering to the architectural invariants in [`docs/ARCHITECTURE.md`](file:///Users/omkute/Desktop/Projects/GoFetch/docs/ARCHITECTURE.md) and requirements in [`docs/PRODUCT.md`](file:///Users/omkute/Desktop/Projects/GoFetch/docs/PRODUCT.md).

---

## Dependency Graph Overview

```
[T-001: Walking Skeleton]
           │
           ▼
[T-002: Storage Layer & Sparse File Preallocation]
           │
           ▼
[T-003: HTTP Probe & Single-Stream Downloader]
           │
           ▼
[T-004: Multi-Part Parallel Chunk Downloader]
           │
           ▼
[T-005: Resumability, Checkpoints & Transient Error Retry]
           │
           ▼
[T-006: Media Classifier & Directory Routing]
           │
           ▼
[T-007: Priority Queue & Task Scheduling]
           │
           ▼
[T-008: Sequential Download Mode & Concurrency Limiting]
           │
           ▼
[T-009: Interactive Terminal UI & Headless CLI Modes]
           │
           ▼
[T-010: Daemon Local REST API & IPC Integration]
           │
           ▼
[T-011: Asynchronous Event Bus & Off-Hot-Path Telemetry]
```

---

## Vertical Slices

### Milestone 0: Walking Skeleton

#### `T-001`: Walking Skeleton
* **Milestone**: 0 (Foundation)
* **Dependencies**: None
* **Scope**:
  * Set up Go module (`go.mod`), root `Makefile` (`make build`, `make test`, `make lint`, `make run`), and CLI skeleton via Cobra.
  * Implement minimal loopback HTTP daemon engine (`cmd/gofetch/daemon.go` & `internal/api/server.go`) binding to `127.0.0.1:12701`.
  * Expose `GET /api/v1/health`.
* **Deliverables**:
  * `go.mod`, `go.sum`, `Makefile`
  * `cmd/gofetch/main.go`, `cmd/gofetch/root.go`, `cmd/gofetch/daemon.go`
  * `internal/api/server.go`, `internal/api/routes.go`
  * `internal/api/server_test.go`
* **Testable Outcome**:
  * Running `go test -v -race ./internal/api/...` starts the server and verifies that `GET /api/v1/health` returns status `200 OK` with JSON `{"status":"ok"}`.

---

### Milestone 1: Core Engine & Storage Slices

#### `T-002`: Storage Layer & Sparse File Preallocation
* **Milestone**: 1 (Engine Core)
* **Dependencies**: `T-001`
* **Scope**:
  * Implement disk allocation in `internal/storage/file.go` and `internal/storage/sparse.go`.
  * Preallocate file size via `os.File.Truncate` to prevent disk fragmentation.
  * Implement thread-safe random-access writes via `os.File.WriteAt`.
  * Implement safe temp-to-final atomic rename (`.gofetch.part` -> final path) via `os.Rename`.
  * Add streaming checksum verification (SHA-256 and MD5) in `internal/storage/checksum.go`.
* **Deliverables**:
  * `internal/storage/file.go`, `internal/storage/sparse.go`, `internal/storage/checksum.go`
  * `internal/storage/sparse_test.go`, `internal/storage/checksum_test.go`
* **Testable Outcome**:
  * Multiple concurrent goroutines write disjoint byte blocks via `WriteAt` to a sparse `.part` file simultaneously without race conditions or data corruption; final file checksum matches expected source hash.

#### `T-003`: HTTP Probe & Single-Stream Downloader
* **Milestone**: 1 (Engine Core)
* **Dependencies**: `T-002`
* **Scope**:
  * Implement probe analyzer in `internal/engine/probe.go` sending HTTP `HEAD`/`GET` (checking `Accept-Ranges: bytes`, `Content-Length`, `Content-Type`).
  * Implement filename extraction and path traversal sanitization (stripping `../`, null bytes, slashes) from URLs and `Content-Disposition` headers.
  * Implement fallback single-stream sequential download for servers lacking `Range` header support.
* **Deliverables**:
  * `internal/engine/probe.go`, `internal/engine/coordinator.go`
  * `internal/engine/probe_test.go`
* **Testable Outcome**:
  * Test spins up an HTTP mock server returning a non-chunkable file (`Accept-Ranges: none`); engine probes metadata, sanitizes the filename, and completes the download as a single continuous stream.

#### `T-004`: Multi-Part Parallel Chunk Downloader
* **Milestone**: 1 (Engine Core)
* **Dependencies**: `T-003`
* **Scope**:
  * Implement byte range partitioner (`internal/engine/chunk.go`) dividing target file size into $N$ closed byte slices (`[start, end]`).
  * Implement worker goroutine pool (`internal/engine/worker.go`) executing parallel HTTP requests with `Range: bytes=start-end`.
  * Stream incoming chunk bytes directly to storage via `os.File.WriteAt` using `golang.org/x/sync/errgroup`.
* **Deliverables**:
  * `internal/engine/chunk.go`, `internal/engine/worker.go`
  * `internal/engine/chunk_test.go`, `internal/engine/worker_test.go`
* **Testable Outcome**:
  * Downloading a 10MB test payload from a mock HTTP server with 4 concurrent workers finishes successfully with all 4 workers streaming simultaneously, producing a bit-for-bit identical output file.

#### `T-005`: Resumability, Checkpoints & Transient Error Retry
* **Milestone**: 1 (Engine Core)
* **Dependencies**: `T-004`
* **Scope**:
  * Implement local chunk progress checkpointing in `internal/state/checkpoint.go` using embedded `bbolt` / metadata log.
  * Support graceful pause and context cancellation on `SIGINT` (flushing OS buffers with `fsync`).
  * Implement chunk retry with exponential backoff on transient network drops (`io.ErrUnexpectedEOF`, TCP disconnects).
  * On resume, probe server with `If-Range` / `ETag` and resume only incomplete byte ranges.
* **Deliverables**:
  * `internal/state/checkpoint.go`, `internal/state/bbolt.go`
  * `internal/state/checkpoint_test.go`, `tests/integration/resume_test.go`
* **Testable Outcome**:
  * Aborting an active 4-chunk download at 50% completion preserves `.part` and checkpoint metadata; initiating resume requests only the remaining 50% byte range and successfully finishes the file.

---

### Milestone 2: Organization, Queue & Control Slices

#### `T-006`: Media Classifier & Directory Routing
* **Milestone**: 2 (Organization & Scheduling)
* **Dependencies**: `T-005`
* **Scope**:
  * Implement file categorization in `internal/classifier/classifier.go` and `internal/classifier/rules.go`.
  * Detect media categories using MIME sniffing (`http.DetectContentType`) and file extension matching.
  * Automatically route finished files to destination folders (`Videos/`, `Documents/`, `Audio/`, `Archives/`, `Software/`, `Others/`).
  * Add bypass flag (`--no-organize`) and custom path config options.
* **Deliverables**:
  * `internal/classifier/classifier.go`, `internal/classifier/rules.go`, `internal/classifier/types.go`
  * `internal/classifier/classifier_test.go`
* **Testable Outcome**:
  * Downloading sample `.pdf`, `.mp4`, `.zip`, and `.bin` payloads automatically places each completed file into its corresponding category subfolder; unclassified files route to `Others/`.

#### `T-007`: Priority Queue & Task Scheduling
* **Milestone**: 2 (Organization & Scheduling)
* **Dependencies**: `T-006`
* **Scope**:
  * Implement priority task heap in `internal/queue/priority_heap.go` using `container/heap`.
  * Support priority levels: `PriorityHigh`, `PriorityNormal`, `PriorityLow`.
  * Implement queue scheduler with dynamic preemption: newly added high-priority tasks move to the front of pending queue.
* **Deliverables**:
  * `internal/queue/priority_heap.go`, `internal/queue/scheduler.go`, `internal/queue/task.go`
  * `internal/queue/priority_heap_test.go`, `internal/queue/scheduler_test.go`
* **Testable Outcome**:
  * With queue worker capacity saturated, queuing a `PriorityLow` task followed by a `PriorityHigh` task dispatches the `PriorityHigh` task first as soon as an execution slot frees up.

#### `T-008`: Sequential Download Mode & Concurrency Limiting
* **Milestone**: 2 (Organization & Scheduling)
* **Dependencies**: `T-007`
* **Scope**:
  * Implement concurrency limiter via token semaphore in `internal/queue/semaphore.go`.
  * Add support for `--max-concurrent` (`-j`, default: 3) and `--sequential` (`-j 1`) CLI flags.
  * Guarantee strictly ordered FIFO execution when running in sequential mode.
* **Deliverables**:
  * `internal/queue/semaphore.go`, `cmd/gofetch/add.go`
  * `internal/queue/semaphore_test.go`, `tests/integration/priority_test.go`
* **Testable Outcome**:
  * Queuing 5 distinct download URLs in `--sequential` mode verifies that exactly 1 download runs at any instant, and tasks complete strictly in submitted sequence.

---

### Milestone 3: Presentation, IPC & Telemetry Slices

#### `T-009`: Interactive Terminal UI & Headless CLI Modes
* **Milestone**: 3 (User Experience & IPC)
* **Dependencies**: `T-008`
* **Scope**:
  * Build interactive Bubbletea TUI model in `internal/ui/tui.go` and `internal/ui/progress.go`.
  * Render overall download progress bar, current transfer rate (MB/s), ETA countdown, and multi-chunk status bars.
  * Implement headless/quiet modes (`--quiet` and `--json`) outputting machine-readable JSON status for pipelines and automation.
* **Deliverables**:
  * `internal/ui/tui.go`, `internal/ui/progress.go`, `internal/ui/styles.go`
  * `internal/ui/progress_test.go`
* **Testable Outcome**:
  * TUI renders and refreshes at 10Hz without data races under `-race`; `--json` flag prints well-formed JSON progress lines to stdout without terminal escape characters.

#### `T-010`: Daemon Local REST API & IPC Integration
* **Milestone**: 3 (User Experience & IPC)
* **Dependencies**: `T-009`
* **Scope**:
  * Implement full REST endpoints in `internal/api/handlers.go` and `internal/api/routes.go`:
    * `POST /api/v1/downloads` (enqueue)
    * `GET /api/v1/downloads` (list with filters)
    * `GET /api/v1/downloads/{id}` (inspect chunk details)
    * `POST /api/v1/downloads/{id}/pause` & `resume`
    * `DELETE /api/v1/downloads/{id}` (cancel & optional purge)
    * `GET /api/v1/system/stats`
  * Wire CLI commands (`add`, `pause`, `resume`, `list`) to communicate seamlessly with daemon over loopback HTTP or run standalone in-process if daemon is inactive.
* **Deliverables**:
  * `internal/api/handlers.go`, `internal/api/routes.go`, `internal/api/models.go`
  * `tests/integration/daemon_api_test.go`
* **Testable Outcome**:
  * An automated integration test executes the full lifecycle of a download (enqueue, monitor progress, pause, resume, cancel) via the local REST API over `127.0.0.1:12701`.

#### `T-011`: Asynchronous Event Bus & Off-Hot-Path Telemetry
* **Milestone**: 3 (User Experience & IPC)
* **Dependencies**: `T-010`
* **Scope**:
  * Implement non-blocking asynchronous event bus in `internal/state/event_bus.go` using buffered channels.
  * Emit lifecycle events (`DownloadStarted`, `ChunkCompleted`, `DownloadFinished`, `DownloadFailed`).
  * Implement optional PostgreSQL consumer (`internal/state/postgres.go` via `pgx/v5`) running in an isolated background goroutine.
  * Guarantee zero database queries, zero connection locks, and zero blocking I/O on the chunk transfer hot path.
* **Deliverables**:
  * `internal/state/event_bus.go`, `internal/state/postgres.go`
  * `internal/state/event_bus_test.go`, `bench/bench_storage_test.go`
* **Testable Outcome**:
  * Benchmark confirms that streaming throughput and latency remain completely unchanged whether PostgreSQL telemetry persistence is enabled or disabled.
