# GoFetch

**A concurrent, resumable download manager for Go — CLI/TUI, loopback daemon, and an embedded real-time web dashboard.**

GoFetch splits HTTP/HTTPS downloads into non-overlapping byte ranges, writes them straight to disk, and resumes interrupted transfers from persisted checkpoints. It runs as a standalone CLI or as a persistent loopback daemon with a live queue API and dashboard.

- **Go 1.26+** · static, zero-runtime-dependency binary
- Loopback HTTP on `127.0.0.1:12701` — no external database required
- Optional PostgreSQL telemetry, fully off the transfer hot path

---

## Contents

- [Features](#features)
- [Install](#install)
- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [REST API and event stream](#rest-api-and-event-stream)
- [Screenshots](#screenshots)
- [Development](#development)
- [Project layout](#project-layout)
- [Invariants](#invariants)
- [License](#license)

---

## Features

- **Segmented transfers** — partition a file into concurrent ranges and write with disjoint `WriteAt` calls.
- **Resume with revalidation** — bbolt checkpoints persist chunk offsets; `If-Range`/ETAG checks prevent resuming against a changed resource. Single-stream downloads are resumable too.
- **Failure recovery** — exponential-backoff retries for transient statuses (429/500/502/503/504) and network errors; servers that ignore Range requests are downgraded to a single stream.
- **Integrity first** — SHA-256/MD5 verification runs against the staged file *before* the atomic rename to its final name.
- **Per-job staging** — each job gets its own `.gofetch.part` path, so concurrent downloads of the same filename cannot collide.
- **Interactive TUI** — Bubbletea/Lipgloss progress view with per-chunk status, speed, and ETA; `--headless` and `--json` for scripts.
- **Loopback daemon + queue** — priority scheduling, `-j`/`--max-concurrent`, `--sequential` (FIFO) mode, pause/resume/cancel, and batch ingestion.
- **Embedded web dashboard** — `embed.FS` SPA with a 60-second bandwidth chart, category filters, live detail drawer with per-chunk view, and sub-100 ms SSE updates.
- **Automatic organization** — MIME/extension classification into `Videos/`, `Documents/`, `Audio/`, `Archives/`, `Software/`, and `Others/`, with configurable folder names.
- **Bandwidth control** — token-bucket limiter, per-task or global, adjustable at runtime.
- **Strict configuration** — YAML with unknown-key rejection, explicit flag > config > default precedence.

---

## Install

Prerequisites: **Go 1.26+**, `make` (optional), `golangci-lint` (for linting).

```bash
git clone https://github.com/omkute/gofetch.git
cd gofetch
make build                 # CGO_ENABLED=0 -> bin/gofetch
./bin/gofetch --help
```

Other targets: `make run`, `make test`, `make bench`, `make lint`, `make clean`.

---

## Quick start

### Standalone (in-process CLI/TUI)

```bash
# Segmented download with the interactive TUI (4 chunks by default)
gofetch https://example.com/large.iso

# 8 chunks, custom destination, 10 MB/s cap
gofetch -c 8 -d ~/Downloads -l 10MB https://example.com/archive.tar.gz

# Verify integrity before commit and organize the result
gofetch --checksum <sha256-hex> --checksum-algo sha256 https://example.com/dataset.zip

# Batch from a file (one URL per line), scripting modes
gofetch -f urls.txt -d ~/Downloads
gofetch --headless https://example.com/file.bin
gofetch --json https://example.com/file.bin > progress.jsonl
```

Useful flags: `--config`, `-j/--max-concurrent`, `--sequential`, `--chunk-sequential`, `--no-organize`, `--quiet`, `--input-file`.

### Daemon and queue

```bash
gofetch daemon                     # starts on 127.0.0.1:12701
gofetch daemon -j 1                # strict FIFO, one download at a time
gofetch daemon --rate-limit 20MB

gofetch add https://example.com/video.mp4 --priority high -c 8 -l 5MB
gofetch add -f urls.txt
gofetch list                       # add --status downloading|queued|paused|completed|failed
gofetch pause <id>
gofetch resume <id>
gofetch cancel <id> --purge
gofetch stats
```

### Web dashboard

```bash
gofetch gui                        # aliases: web, dashboard
```

Opens `http://127.0.0.1:12701/` in your default browser. The dashboard shows live progress, a 60-second speed chart, category filters, and a per-chunk detail drawer. The daemon serves the API even when the GUI is disabled via config.

---

## Architecture

```
CLI / TUI (Cobra, Bubbletea)
        |
        +--> Standalone engine (in-process)
        |
        +--> Loopback REST daemon (chi, 127.0.0.1:12701)
                 |
                 v
        Priority queue + concurrency scheduler
                 |
                 v
        Download coordinator
          - probe (HEAD/GET, Range, ETag)
          - chunk workers / retries / single-stream fallback
          - per-job .gofetch.part staging + checksum verification
                 |
        +--------+-------------------------+
        |                                  |
        v                                  v
  bbolt checkpoints                  Target filesystem
  async event bus / telemetry        WriteAt -> os.Rename -> classifier
  (optional Postgres consumer)
```

### Transfer lifecycle

1. **Probe** the URL for size, range support, ETag, and a sanitized filename.
2. **Partition** into bounded ranges; persist a checkpoint keyed by job ID.
3. **Stream** each range into the job's own sparse staging file; publish offsets through a non-blocking handoff.
4. **Recover** from transient statuses with backoff; fall back to one stream if the origin ignores Range.
5. **Verify** the staged bytes against the requested checksum.
6. **Commit** with `os.Rename`, then classify into a category folder.

No database query runs inside a chunk worker; checkpoint and telemetry writes are consumed asynchronously.

---

## Configuration

Config is discovered from `$GOFETCH_CONFIG`, `~/.gofetch/config.yaml`, or `./config.yaml`. Select a file explicitly with `gofetch --config <path>` (standalone) or `gofetch daemon --config <path>`. Unknown keys are rejected with an error naming the file.

```yaml
concurrency:
  max_concurrent_downloads: 3   # daemon-wide download limit
  chunks_per_download: 4        # default when a request omits max_chunks
storage:
  default_download_dir: "/absolute/path/to/Downloads"
  auto_classify: true
  category_paths:               # category -> folder, relative to download dir
    Videos: "Videos"
    Documents: "Documents"
network:
  user_agent: "GoFetch/2.0"
  connection_timeout_sec: 30    # TCP dial timeout, not an overall deadline
  global_rate_limit_bps: 20971520
  postgres_dsn: ""              # empty disables optional telemetry
daemon:
  listen_addr: "127.0.0.1:12701"
  enable_web_gui: true
  task_db_path: "/absolute/path/to/.gofetch/daemon_tasks.db"
```

- Use absolute paths in YAML; `~` is not expanded.
- `category_paths` values must be relative: absolute paths, `..` segments, and null bytes are rejected.
- Precedence: **explicit flag/request field > config file > built-in default**. Standalone honors `--dir`, `--chunks`, and `--no-organize` as overrides; the daemon honors `-j/--max-concurrent`, `--sequential`, `--addr`, and `--rate-limit`.
- With no config file, home-derived defaults apply. On standalone this means `~/Downloads` and the config user agent.
- Optional telemetry: set `network.postgres_dsn` to persist download events. The daemon creates a pool at startup and closes it on shutdown; all inserts run in a background consumer.

---

## REST API and event stream

| Method | Endpoint | Purpose |
| :--- | :--- | :--- |
| `GET` | `/` | Embedded dashboard (when `enable_web_gui` is true) |
| `GET` | `/api/v1/health` | Health check |
| `POST` | `/api/v1/downloads` | Enqueue one download |
| `POST` | `/api/v1/downloads/batch` | Enqueue multiple URLs |
| `GET` | `/api/v1/downloads` | List tasks (`?status=downloading`) |
| `GET` | `/api/v1/downloads/{id}` | Task detail including per-chunk boundaries |
| `POST` | `/api/v1/downloads/{id}/pause` | Pause |
| `POST` | `/api/v1/downloads/{id}/resume` | Resume |
| `DELETE` | `/api/v1/downloads/{id}` | Cancel (`?delete_files=true` to purge) |
| `POST` | `/api/v1/downloads/pause-all` | Pause all |
| `POST` | `/api/v1/downloads/resume-all` | Resume all |
| `GET` | `/api/v1/system/stats` | Aggregate metrics |
| `POST` | `/api/v1/system/rate-limit` | Update global limit at runtime |
| `GET` | `/api/v1/events` | SSE lifecycle events and sub-100 ms progress frames |

Queue order is deterministic: tasks are emitted in submission order (oldest first, ID tie-break), so dashboard rows stay put between frames.

---

## Screenshots

Images are kept in `docs/screenshots/`. See [docs/screenshots/README.md](docs/screenshots/README.md) for capture instructions and the expected file names.

| Capture | File | Status |
| :--- | :--- | :--- |
| Web dashboard with active queue | `docs/screenshots/dashboard.png` | add image |
| Task detail / chunk view | `docs/screenshots/chunk-details.png` | add image |
| Interactive TUI during a download | `docs/screenshots/tui.png` | add image |

Once images are added, embed them here, for example:

```markdown
![GoFetch dashboard](docs/screenshots/dashboard.png)
```

---

## Development

```bash
make test          # go test -v -race ./...
make lint          # golangci-lint run ./...  (expect 0 issues)
make bench         # storage and transfer benchmarks
go build ./...     # compile all packages
```

Integration tests start a real loopback daemon on an ephemeral port and exercise enqueue, progress, pause, resume, cancel, checksum failure, and batch ingestion over HTTP.

Specs and design notes live in `docs/PRODUCT.md`, `docs/SPECS/`, `docs/DECISIONS/`, and `tasks/BACKLOG.md`. `AGENTS.md` records the invariants and definition of done.

---

## Project layout

| Path | Contents |
| :--- | :--- |
| `cmd/gofetch` | Cobra commands, standalone engine, GUI launcher |
| `config` | YAML schema, loader, strict validation |
| `internal/api` | Daemon router, handlers, SSE, config wiring, telemetry |
| `internal/engine` | Probe, partition, chunk workers, retries, checkpoints |
| `internal/storage` | Sparse staging writer, checksum, atomic commit |
| `internal/queue` | Priority queue and concurrency scheduler |
| `internal/classifier` | MIME/extension rules and directory routing |
| `internal/state` | bbolt stores, event bus, telemetry consumer |
| `internal/ui`, `internal/web` | TUI and embedded dashboard assets |
| `tests/integration` | Real-listener end-to-end tests |
| `bench` | Benchmarks |

---

## Invariants

1. No database work on the chunk transfer hot path; telemetry and checkpoints use async buffered handoffs.
2. Downloads write to a per-job `.gofetch.part` and commit via `os.Rename` only after full completion and checksum verification.
3. Chunks write only to deterministic, non-overlapping byte ranges.
4. Range support is verified (`Accept-Ranges`, status `206`); a `200` response falls back safely to a single stream.
5. URLs and `Content-Disposition` filenames are sanitized against traversal (`../`, absolute paths, null bytes); configured category paths are validated the same way.
6. Never commit secrets or credentials.

---

## License

MIT License.
