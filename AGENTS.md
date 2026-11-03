# AGENTS.md — Developer & AI Agent Guidelines

Operational guide and rules of engagement for developers and AI agents working on GoFetch.

## 1. Tech Stack & Required Local Tools

- **Core Runtime**: Go 1.22+ (toolchain, standard library: `net/http`, `os`, `sync/atomic`, `io`).
- **CLI & TUI**: `github.com/spf13/cobra`, `github.com/charmbracelet/bubbletea`, `lipgloss`.
- **State & Router**: `go.etcd.io/bbolt` (local ACID checkpoints), `github.com/go-chi/chi/v5` (daemon router).
- **Async Persistence**: `github.com/jackc/pgx/v5` (isolated off the hot path).
- **Required Local Tools**: `go` (1.22+), `golangci-lint` (v1.55+), `make`, `git`, `curl`.

## 2. Essential Commands

- `make build`  — Compile static cross-platform binary (`bin/gofetch`).
- `make run`    — Run CLI in development mode (`go run cmd/gofetch/main.go`).
- `make test`   — Run unit and race-condition test suite (`go test -v -race ./...`).
- `make bench`  — Run storage and transfer benchmarks (`go test -bench=. ./bench/...`).
- `make lint`   — Run strict linter checks (`golangci-lint run ./...`).
- `make clean`  — Clean build artifacts and temporary test files (`rm -rf bin/ *.part`).

## 3. Invariants & "Never Do" Rules

- **NEVER put external DBs (Postgres) on the hot path**: Streaming chunk downloads write directly to memory/disk (`WriteAt`); DB metrics sync strictly via async buffered channels.
- **NEVER write directly to the final file name**: In-progress transfers MUST write to `.gofetch.part` and commit via atomic `os.Rename` only upon full completion and checksum verification.
- **NEVER commit secrets, credentials, or private keys**: No hardcoded API keys, auth tokens, or `.env` files committed to version control.
- **NEVER break tenant or filesystem isolation**: Sanitize all URLs and `Content-Disposition` filenames against path traversal (`../`, absolute paths, null bytes).
- **NEVER allow overlapping chunk writes**: Chunks must write strictly to deterministic, non-overlapping `[start, end]` byte boundaries via `os.File.WriteAt`.
- **NEVER assume HTTP Range support**: Always verify `Accept-Ranges: bytes` and status `206`; safely downgrade to single-stream download if the server returns `200`.

## 4. Definition of Done (DoD)

A task is considered complete only when:
1. **Tests Pass**: Unit and integration tests pass with race detection enabled (`go test -race ./...`).
2. **Lint Clean**: Zero warnings or errors from `golangci-lint run`.
3. **Invariants Preserved**: Zero DB queries on chunk transfer loops; atomic file renames strictly verified.
4. **Docs & Specs Updated**: Any configuration, flag, or architectural change is reflected in `docs/`.
5. **Git Hygiene**: Atomic commits following Conventional Commits format (`feat:`, `fix:`, `docs:`, `refactor:`).
