# ⚡ GoFetch

> **High-performance, resilient, concurrent chunked download manager CLI and loopback daemon engine built in Go.**

GoFetch is a cross-platform download accelerator designed for extreme network resilience, blazing throughput, and clean local file organization. It seamlessly bridges terminal power users and automated background pipelines through both an interactive CLI/TUI and a persistent local daemon with an embedded real-time Web dashboard.

---

## ✨ Features

- **🚀 Segmented Multi-Connection Acceleration**: Dynamically partitions HTTP/HTTPS downloads into concurrent closed byte ranges (`Range: bytes=start-end`), writing directly to disk via non-overlapping `os.File.WriteAt` calls.
- **🛡️ Rock-Solid Resumability**: Embedded ACID key-value checkpointing (`bbolt`) ensures interrupted downloads resume seamlessly without re-downloading previously completed bytes.
- **🔄 Transient Failure Recovery**: Automatic exponential backoff retries on network drops, TCP disconnects, and packet truncations.
- **📊 Interactive Terminal UI (TUI)**: Beautiful, 10Hz responsive terminal visualization powered by Bubbletea and Lipgloss, displaying overall progress, per-chunk status bars, transfer rates, and remaining ETA.
- **🤖 Headless & Scriptable Modes**: Quiet (`-q`) and machine-readable JSON (`--json`) modes tailored for CI/CD pipelines, shell scripts, and background tools.
- **📁 Automated Media Classification**: Automatically inspects MIME magic bytes (`http.DetectContentType`) and file extensions to route finished files into organized subdirectories (`Videos/`, `Documents/`, `Audio/`, `Archives/`, `Software/`, `Others/`).
- **🎛️ Bandwidth Rate Limiter**: High-precision token-bucket rate limiting configurable globally or per-download (e.g. `--rate-limit 5MB`, `10M`).
- **🚦 Priority Queue & Dynamic Scheduling**: Global concurrency via `--max-concurrent` / `-j` (default 3). `--sequential` or `-j 1` runs downloads one at a time in strict submission (FIFO) order; `--chunk-sequential` separately fetches a single file's chunks head-to-tail.
- **🌐 Embedded Modern Desktop / Web GUI**: Zero-dependency single-page application embedded directly into the binary via `embed.FS`, featuring a real-time 60-second Canvas bandwidth graph, live queue controls, category filters, and SSE streaming.
- **🔒 Checksum Integrity Verification**: Post-download streaming cryptographic verification supporting both SHA-256 and MD5 digests.
- **🧩 Zero Database on Hot Path**: Memory-mapped sparse file preallocation (`os.Truncate`) and direct disk I/O; telemetry is decoupled onto asynchronous buffered channels.

---

## 🏗️ Architecture Overview

```
+-------------------------------------------------------------------------+
|                              Client Layer                               |
|                                                                         |
|   +--------------------------+         +----------------------------+   |
|   |  CLI Terminal Interface   |         | Browser Extension / Native |   |
|   |  (Cobra + Bubbletea/TUI) |         | Host Messaging (Post-v1)   |   |
|   +------------+-------------+         +--------------+-------------+   |
+----------------|--------------------------------------|-----------------+
                 |                                      |
                 | (In-process call or Loopback HTTP/UDS)|
                 v                                      v
+-------------------------------------------------------------------------+
|                       GoFetch Engine (Core Daemon)                      |
|                                                                         |
|  +-------------------------------------------------------------------+  |
|  | Local IPC / REST Server (127.0.0.1:12701 or Unix Domain Socket)   |  |
|  +---------------------------------+---------------------------------+  |
|                                    |                                    |
|                                    v                                    |
|  +-------------------------------------------------------------------+  |
|  | Queue & Scheduler (Priority Heap, Concurrency Semaphore)          |  |
|  +---------------------------------+---------------------------------+  |
|                                    |                                    |
|                                    v                                    |
|  +-------------------------------------------------------------------+  |
|  | Download Engine & Worker Pool (HTTP Range, Dynamic Chunking)      |  |
|  +---------------------------------+---------------------------------+  |
|                                    |                                    |
|                                    v                                    |
|  +-------------------------------------------------------------------+  |
|  | Storage & Disk IO (Sparse File Preallocation, Concurrent WriteAt) |  |
|  +---------------------------------+---------------------------------+  |
|                                    |                                    |
|                                    v                                    |
|  +-------------------------------------------------------------------+  |
|  | Media Classifier (MIME Sniffer & Extension Rule Engine)           |  |
|  +-------------------------------------------------------------------+  |
+-------------------------------------------------------------------------+
       |                                                    |
(Async Event Channel)                              (Direct File I/O)
       v                                                    v
+-----------------------------+               +---------------------------+
| State Store & Telemetry     |               | Target Filesystem         |
| - Fast In-Memory State      |               | - Temp: `*.part` (Sparse) |
| - Local Checkpoint (bbolt)  |               | - Final: `Downloads/`     |
| - Off-Hot-Path Postgres Sync|               |   - `Videos/`, `Docs/`,.. |
+-----------------------------+               +---------------------------+
```

---

## 📦 Installation & Build

### Prerequisites
- **Go 1.22+**
- **Make** (optional, standard POSIX)

### Compiling from Source

```bash
# Clone the repository
git clone https://github.com/omkute/gofetch.git
cd gofetch

# Compile the static optimized binary
make build

# The binary will be available at bin/gofetch
./bin/gofetch --help
```

---

## 🚀 Quick Start Guide

### 1. Standalone CLI Download (Direct In-Process)

Download any URL directly in your terminal with the interactive Bubbletea TUI:

```bash
# Basic segmented download (4 concurrent chunks by default)
gofetch https://releases.ubuntu.com/24.04/ubuntu-24.04-desktop-amd64.iso

# Custom chunks, destination directory, and bandwidth limit
gofetch -c 8 -d ~/Downloads -l 10MB https://example.com/large-archive.tar.gz

# Checksum verification with automatic directory classification
gofetch --checksum 5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8 \
        --checksum-algo sha256 \
        https://example.com/dataset.zip

# Batch download from a text file (one URL per line)
gofetch -f urls.txt -d ~/Downloads

# Headless / Scripting mode (plain text or machine-readable JSON)
gofetch --headless https://example.com/file.bin
gofetch --json https://example.com/file.bin > progress.jsonl
```

### 2. Interactive Web GUI Dashboard

Launch the zero-dependency embedded desktop web dashboard:

```bash
gofetch gui
# Or use the alias:
gofetch web
```

GoFetch automatically opens your default browser at `http://127.0.0.1:12701/`. If the background daemon is not already running, it starts it in-process.

### 3. Loopback Daemon & Queue Management

Run the GoFetch daemon as a background service:

```bash
# Start daemon on default loopback port 127.0.0.1:12701
gofetch daemon

# Or apply a global bandwidth ceiling across all tasks
gofetch daemon --rate-limit 20MB
```

Manage tasks concurrently from any other terminal tab:

```bash
# Enqueue a high-priority download
gofetch add https://example.com/movie.mp4 --priority high -c 8 -l 5MB

# Batch enqueue from a URL file
gofetch add -f urls.txt --priority normal

# List active, queued, and completed downloads
gofetch list
gofetch list --status downloading

# Pause and resume by Task ID
gofetch pause abc12345
gofetch resume abc12345

# Cancel and purge files
gofetch cancel abc12345 --purge

# Inspect daemon runtime metrics and aggregate throughput
gofetch stats
```

---

## ⚙️ Configuration (`~/.gofetch/config.yaml`)

The daemon discovers configuration from `$GOFETCH_CONFIG`, `~/.gofetch/config.yaml`, or `./config.yaml`; use `gofetch daemon --config <path>` to select a file explicitly. Unknown keys are rejected with an error naming the config file.

The complete schema is:

```yaml
concurrency:
  max_concurrent_downloads: 3 # global download limit
  chunks_per_download: 4      # default chunk count when a request omits max_chunks
storage:
  default_download_dir: "/absolute/path/to/Downloads" # default destination when a request omits destination_dir
  auto_classify: true                 # default classification when a request omits auto_classify
  category_paths:                     # category -> folder name (relative to the download directory)
    Videos: "Videos"
    Documents: "Documents"
network:
  user_agent: "GoFetch/2.0"
  connection_timeout_sec: 30
  global_rate_limit_bps: 20971520 # 20 MB/s
daemon:
  listen_addr: "127.0.0.1:12701"
  enable_web_gui: true
  task_db_path: "/absolute/path/to/.gofetch/daemon_tasks.db"
```

Use absolute paths in the YAML file; the loader does not expand `~`. When no config file is found, defaults for the download directory and task DB are derived from the current user's home directory.

Paths in `storage.category_paths` must be relative folder names; absolute paths, `..` segments, and null bytes are rejected. Per-category folder names may be nested (for example `Media/Videos`), and categories not listed use their built-in folder names.

**Precedence:** an explicit flag or request field wins over the config file, which wins over the built-in default. For example `gofetch daemon -j 1` overrides `concurrency.max_concurrent_downloads`, while a `POST /api/v1/downloads` body with `max_chunks`, `destination_dir`, `auto_classify`, `no_organize`, or `rate_limit` overrides the corresponding config default. Configuration currently applies to the daemon path; standalone/root `--config` is not wired yet.

---

## 📡 REST API & Real-Time Event Stream

When running in daemon mode (`127.0.0.1:12701`), GoFetch exposes local REST and SSE endpoints:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Serves the embedded Web GUI dashboard |
| `GET` | `/api/v1/health` | Daemon health check |
| `POST` | `/api/v1/downloads` | Enqueue a new download |
| `POST` | `/api/v1/downloads/batch`| Enqueue a batch of URLs |
| `GET` | `/api/v1/downloads` | List downloads (filter by `status`) |
| `GET` | `/api/v1/downloads/{id}` | Granular task metrics and chunk boundaries |
| `POST` | `/api/v1/downloads/{id}/pause` | Pause an active download |
| `POST` | `/api/v1/downloads/{id}/resume`| Resume a paused download |
| `DELETE`| `/api/v1/downloads/{id}` | Cancel/delete task (`delete_files=true`) |
| `POST` | `/api/v1/downloads/pause-all` | Simultaneously pause all tasks |
| `POST` | `/api/v1/downloads/resume-all` | Simultaneously resume all tasks |
| `GET` | `/api/v1/system/stats` | System metrics (throughput, memory, uptime) |
| `POST` | `/api/v1/system/rate-limit` | Dynamically update global speed limit |
| `GET` | `/api/v1/events` | Server-Sent Events (SSE) sub-100ms stream |

---

## 🧪 Testing & Code Quality

The GoFetch test suite verifies strict adherence to architectural invariants, concurrency safety, and zero race conditions:

```bash
# Run unit and race condition tests
make test
# (Equivalent to: go test -v -race ./...)

# Run storage & transfer benchmarks
make bench

# Run strict linter checks
make lint
```

---

## 📜 Invariants & Architectural Rules

1. **Decoupled Databases**: No relational database transactions ever run on the byte transfer hot path. Storage writes exclusively to disk via `WriteAt`; metrics stream over non-blocking Go channels.
2. **Safe Atomic Commits**: In-progress transfers write exclusively to `.gofetch.part` staging files and commit atomically via `os.Rename` only upon completion and checksum verification.
3. **Chunk Boundary Isolation**: Workers operate strictly on disjoint, deterministic byte intervals `[start, end]`.
4. **Dynamic Range Fallback**: If an endpoint lacks `Range` header support or returns HTTP `200` instead of `206`, the engine automatically downgrades to a single continuous stream without data corruption.
5. **Path Traversal Protection**: All URLs and `Content-Disposition` filenames are sanitized against directory traversal attacks (`../`, null bytes, slashes).

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.
