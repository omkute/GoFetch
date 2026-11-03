# Code Map — GoFetch

This document outlines the codebase organization, module architecture, planned entry points, test suites, and package responsibilities for **GoFetch**, aligned with the architectural specifications in [`docs/ARCHITECTURE.md`](file:///Users/omkute/Desktop/Projects/GoFetch/docs/ARCHITECTURE.md).

---

## 1. Planned Packages & Module Directory

| Package Name | Package Path | Planned Entry Points / Key Files | Planned Test Files | Status | Description / Primary Responsibilities |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`main`** | `cmd/gofetch` | `main.go`<br>`root.go`<br>`add.go`<br>`pause.go`<br>`resume.go`<br>`list.go`<br>`daemon.go` | `main_test.go`<br>`cmd_test.go`<br>`root_test.go` | Active (T-001) | CLI entry point and Cobra command hierarchy; parses user flags, manages subcommands, initializes TUI or headless mode. |
| **`engine`** | `internal/engine` | `coordinator.go`<br>`probe.go`<br>`chunk.go`<br>`worker.go`<br>`options.go` | `coordinator_test.go`<br>`probe_test.go`<br>`chunk_test.go`<br>`worker_test.go` | Planned | Download lifecycle coordinator, HTTP `HEAD`/`GET` probe analyzer (`Accept-Ranges`), byte range partitioner, and parallel chunk worker pool. |
| **`storage`** | `internal/storage` | `file.go`<br>`sparse.go`<br>`writer.go`<br>`checksum.go` | `sparse_test.go`<br>`writer_test.go`<br>`checksum_test.go` | Planned | Disk I/O engine; sparse file preallocation (`Truncate`), thread-safe non-overlapping chunk writes (`WriteAt`), atomic file commitment (`.part` to destination), SHA-256/MD5 hashing. |
| **`queue`** | `internal/queue` | `scheduler.go`<br>`priority_heap.go`<br>`task.go`<br>`semaphore.go` | `scheduler_test.go`<br>`priority_heap_test.go`<br>`semaphore_test.go` | Planned | Priority-based task queue using a min/max heap; concurrency limiter (`--max-concurrent`) and sequential execution controller. |
| **`classifier`** | `internal/classifier` | `classifier.go`<br>`rules.go`<br>`types.go` | `classifier_test.go`<br>`rules_test.go` | Planned | Media type classification engine; inspects file magic bytes and extensions to organize completed downloads into `Videos/`, `Documents/`, `Audio/`, `Archives/`, `Software/`, etc. |
| **`state`** | `internal/state` | `tracker.go`<br>`checkpoint.go`<br>`bbolt.go`<br>`event_bus.go`<br>`postgres.go` | `tracker_test.go`<br>`bbolt_test.go`<br>`event_bus_test.go` | Planned | High-frequency atomic byte counters (`sync/atomic`), local resume state persistence via `bbolt`, and asynchronous non-blocking event channel for off-hot-path PostgreSQL telemetry. |
| **`ui`** | `internal/ui` | `tui.go`<br>`progress.go`<br>`model.go`<br>`styles.go` | `progress_test.go`<br>`model_test.go` | Planned | Interactive terminal user interface using `bubbletea` and `lipgloss`; visualizes real-time aggregate speed, ETA, and per-chunk transfer progress bars. |
| **`api`** | `internal/api` | `server.go`<br>`routes.go`<br>`handlers.go`<br>`middleware.go`<br>`models.go` | `server_test.go`<br>`handlers_test.go` | Active (T-001) | Local loopback REST / IPC server (`127.0.0.1:12701` / Unix Domain Socket) built on `chi/v5` for daemon operations and future GUI/extension integration. |
| **`config`** | `config` | `config.go`<br>`defaults.go`<br>`loader.go` | `config_test.go` | Planned | YAML configuration file loader, validation rules, environment variable overrides, and system default profiles. |
| **`bench`** | `bench` | `bench_storage.go`<br>`bench_engine.go` | `bench_storage_test.go`<br>`bench_engine_test.go` | Planned | Benchmarking suite for multi-core chunk disk writes (`WriteAt`), allocation profiles, and throughput stress testing. |
| **`integration`**| `tests/integration` | `harness.go`<br>`mock_server.go` | `e2e_download_test.go`<br>`resume_test.go`<br>`priority_test.go`<br>`daemon_api_test.go` | Planned | End-to-end integration test suite verifying network pause/resume recovery, priority preemption, media sorting, and daemon lifecycle with race detection. |

---

## 2. Package Dependency & Layering Architecture

The packages follow a strict unidirectional dependency graph to maintain testability and prevent cyclic imports:

```
[cmd/gofetch] ------------------------+
      |                               |
      v                               v
[internal/ui]                   [internal/api]
      |                               |
      +---------------+---------------+
                      |
                      v
              [internal/queue]
                      |
                      v
              [internal/engine]
                /          \
               v            v
     [internal/storage]  [internal/classifier]
               \            /
                v          v
              [internal/state] (Async Channel)
                      |
                      v
             [Off-Hot-Path DB (Postgres/bbolt)]
```

### Dependency Rules:
1. **Zero Hot-Path DB Imports**: `internal/engine` and `internal/storage` MUST NOT import database drivers (`pgx`). State updates are emitted as discrete events through `internal/state` channels.
2. **Deterministic Write Safety**: `internal/storage` operates independently of HTTP transport logic, accepting byte slices and explicit absolute offsets via `WriteAt`.
3. **Pluggable Presentation**: Neither `internal/engine` nor `internal/queue` depend on `internal/ui` or `internal/api`; they expose standard Go channels and hooks for status observation.
