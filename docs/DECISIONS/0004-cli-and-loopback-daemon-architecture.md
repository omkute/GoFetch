# ADR-0004: Dual-Mode Topology: Standalone CLI and Loopback IPC Daemon

* **Status**: Accepted
* **Date**: 2026-11-03
* **Deciders**: Principal System Architect, Principal Product Manager

---

## Context
GoFetch v1 targets terminal users who expect an immediate CLI experience similar to `curl` or `wget`. However, the product roadmap specifies:
* **v2**: A native desktop graphical interface (GUI).
* **v3**: A browser companion extension to automatically capture download links without manual terminal pasting.

If GoFetch is built purely as an ephemeral single-process CLI, supporting a GUI or browser extension later would require a complete architectural rewrite. Conversely, requiring users to manually run a heavy background daemon service just to download a single file would create unnecessary friction for CLI users.

## Decision
We implement a **Dual-Mode Architectural Topology**:
1. **Ephemeral CLI Mode (Default)**: When invoked directly (e.g., `gofetch https://example.com/file.zip`), the CLI checks if a local daemon is running. If not, it executes the download coordinator in-process, rendering progress in the terminal and terminating cleanly upon completion.
2. **Daemon Mode (`gofetch daemon`)**: A persistent background engine binds to loopback only (`127.0.0.1:12701` and Unix Domain Socket `~/.gofetch/gofetch.sock` on Linux/macOS). It manages queues, concurrency limits, and active transfers.
3. **Transparent Command Routing**: CLI subcommands (`add`, `pause`, `resume`, `list`) automatically detect the daemon. If active, the CLI acts as a lightweight client sending REST requests to the loopback daemon. If inactive, the command acts on local state.
4. **Foundation for GUI & Extensions**: Future desktop GUIs (v2) and browser extensions (v3) will communicate with this exact same local REST/IPC API.

## Consequences

### Positive
* **Zero Friction for CLI Users**: No requirement to start a daemon before downloading a quick file.
* **Extensible & Future-Proof**: Native GUI and browser extensions can be built without touching the core download or storage engine.
* **Security by Design**: The daemon binds strictly to loopback (`127.0.0.1`), eliminating remote attack vectors.

### Negative / Trade-Offs
* The application maintains both direct in-process coordination and HTTP client routing (abstracted via common Go interfaces in `internal/engine`).
