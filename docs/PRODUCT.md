# Product Requirements Document (PRD) — GoFetch

## 1. Problem Statement & Target User Personas

### Problem Statement
* Standard browser download tools are fragile: they lack segmented acceleration, fail ungracefully on network hiccups, and lack granular queue controls.
* Terminal utilities (`curl`, `wget`) lack built-in multi-connection chunking, priority scheduling, and automated file organization.
* Existing desktop solutions (e.g., IDM) are proprietary, platform-limited (primarily Windows), bloated with ads, and lack scriptable CLI interfaces.
* Download folders (`~/Downloads`) rapidly become chaotic dumping grounds requiring tedious manual file sorting.

### Target User Personas
* **Terminal Power User & Developer ("Dev Dan")**:
  * Needs a lightweight, cross-platform CLI tool to download large datasets, ISOs, and SDKs at maximum network speed.
  * Values pause/resume resilience, scriptability, low resource footprint, and zero telemetry.
* **Heavy Media & Content Collector ("Media Maya")**:
  * Regularly downloads batches of mixed files (archives, video, audio, PDFs).
  * Needs priority queueing to finish urgent items first and automated folder sorting to keep disk storage organized.

---

## 2. Top 3 User Jobs-to-be-Done (JTBD)

* **JTBD 1: High-Speed & Resilient Retrieval**
  * *When* I am downloading large files over fluctuating or fast connections,
  * *I want to* split files into parallel chunk streams with automatic pause/resume and transient failure retry,
  * *So that* I saturate available bandwidth and never lose download progress when network drops occur.

* **JTBD 2: Controlled Queue & Priority Scheduling**
  * *When* I have multiple pending downloads,
  * *I want to* schedule them sequentially or with controlled concurrency, fast-tracking high-priority files,
  * *So that* critical downloads complete first without choking system or network resources.

* **JTBD 3: Automated Media Classification**
  * *When* files finish downloading,
  * *I want to* automatically route them into category-specific directories based on MIME type and file extension,
  * *So that* my storage remains clean and structured without manual post-download sorting.

---

## 3. Acceptance Criteria for v1

### Core Engine & Transfer
* **Multi-Part Chunking**: Dynamically splits HTTP/HTTPS downloads into configurable parallel byte ranges (`Range` header) when supported by the host server (`Accept-Ranges: bytes`).
* **Resumability**: Automatically preserves chunk state on disk to resume interrupted downloads without re-downloading completed bytes.
* **Error Recovery**: Automatically retries failed chunks with exponential backoff before marking a task as failed.
* **Checksum Verification**: Validates file integrity post-download via optional `--checksum` flag (supporting SHA-256 and MD5).

### Queue & Priority Management
* **Concurrency Limiting**: Supports global concurrency controls (`--max-concurrent` / `-j`, default: 3) via the queue scheduler and `concurrency.max_concurrent_downloads` config setting.
* **Sequential Mode**: `--sequential` or `-j 1` means strict sequential execution: exactly one download is active and items are processed in submission (FIFO) order, regardless of priority.
* **Chunk-Order Mode**: `--chunk-sequential` fetches the chunks of an individual file head-to-tail; it is independent of task-level sequential mode.
* **Priority Queue**: Supports assigning priority levels (`--priority high|normal|low`); pending high-priority items preempt lower-priority items in the queue.

### Media Classification & File Management
* **Automatic Category Detection**: Detects file types via extension mapping with fallback to MIME sniffing (`http.DetectContentType`).
* **Directory Routing**: Automatically moves finished files into classified subdirectories under the target root:
  * `Documents/` (`.pdf`, `.docx`, `.txt`, `.epub`, etc.)
  * `Video/` (`.mp4`, `.mkv`, `.avi`, `.mov`, etc.)
  * `Audio/` (`.mp3`, `.flac`, `.wav`, `.aac`, etc.)
  * `Archives/` (`.zip`, `.tar.gz`, `.rar`, `.7z`, etc.)
  * `Software/` (`.exe`, `.dmg`, `.deb`, `.rpm`, `.pkg`, etc.)
  * `Others/` (unmatched MIME types/extensions)
* **Configurable Classification**: Provides a flag (`--no-organize`) and config setting to enable, disable, or customize category mappings.

### CLI & Usability
* **Input Mechanism**: Explicit manual URL entry via CLI arguments (`gofetch <url>`) or batch file feeds (`--input-file <path>`); no automatic browser interception in v1.
* **Progress Visualization**: Interactive terminal UI/progress bar showing overall speed, ETA, percentage, and per-chunk status.
* **Non-Interactive Mode**: Headless/quiet output mode (`--quiet` or `--json`) suitable for scripting and background pipelines.
* **Configuration File**: Persistent configuration file (`config.yaml` / `config.json`) for default concurrency, chunk sizes, and folder mappings.
* **Cross-Platform Compilation**: Single static native binary with zero runtime dependencies for Linux (amd64, arm64), macOS (Apple Silicon, Intel), and Windows (amd64).

---

## 4. Non-Goals (Strictly OUT of Scope for v1)

* **GUI / Native Window Application**: Out of scope for v1 (strictly terminal-native CLI/TUI); planned for upcoming iterations.
* **Browser Extension / Automatic Link Interception**: Out of scope for v1 (downloads require manual link input in terminal); browser companion extension is deferred to future iterations.
* **Torrent & P2P Protocols**: No BitTorrent, DHT, or magnet link handling; HTTP(S) only.
* **Site-Specific Stream Extractors**: No media scrapers or decrypters (e.g., YouTube-DL, HLS `.m3u8`, DASH stream reassembly).
* **Remote Daemon / RPC API**: No background headless daemon service, REST API, or web dashboard (e.g., aria2-style JSON-RPC).
* **Cloud Storage Sync**: No direct upload/sync to cloud providers (e.g., S3, Google Drive, Dropbox).
* **Bandwidth Throttling / Speed Limiter**: No dynamic per-task or global upload/download rate limiting schedules in v1.

---

## 4. Acceptance Criteria for v2

### Desktop & Web GUI
* **Embedded Zero-Dependency Web GUI**: Self-contained SPA embedded directly into the binary via `embed.FS` and served on `http://127.0.0.1:12701/`.
* **Desktop Browser Opener**: `gofetch gui` (and aliases `web`, `dashboard`) auto-launches the user's default browser cross-platform (macOS, Linux, Windows).
* **Real-Time Bandwidth Visualization**: Live 60-second time-series speed graph rendered on HTML5 Canvas.
* **Category Drawer & Filter Tabs**: Granular filtering by category (Videos, Audio, Documents, Archives, Software, Others) and lifecycle status (Active, Queued, Paused, Completed, Failed).
* **Global Controls**: Dynamic global bandwidth throttle dropdown, Pause All, and Resume All buttons.

### Bandwidth Limiting & Batch Processing
* **Dynamic Bandwidth Limiter**: Token-bucket rate limiter supporting both per-task (`--rate-limit 5MB`) and global engine speed caps with hot dynamic updates via API and GUI.
* **Batch URL Ingestion**: Support for multi-line URL batch files via `--input-file` / `-f` CLI flag and `POST /api/v1/downloads/batch`.

### Persistence & Real-Time IPC
* **Durable Task Store**: Embedded BBolt database (`~/.gofetch/daemon_tasks.db`) preserving tasks across daemon restarts with automatic pause-recovery.
* **Server-Sent Events (SSE)**: `GET /api/v1/events` streaming sub-100ms real-time progress and lifecycle events directly from the asynchronous event bus.
* **System Telemetry**: `GET /api/v1/system/stats` reporting active/queued counts, aggregate speed, memory metrics, and uptime.
* **YAML Configuration**: Centralized configuration management (`~/.gofetch/config.yaml` or `--config`) for concurrency, network, and storage defaults.

---

## 5. Future Roadmap Preview (Post-v2)

* **v3 — Browser Companion Extension**: Browser extension (Chrome, Firefox, Edge) utilizing the Native Messaging API to automatically capture downloads without manual terminal copying.
* **v4 — Remote Daemon & Webhook Ecosystem**: Remote headless daemon with authenticated external REST/WebSocket endpoints and webhook notifications upon download completion.
