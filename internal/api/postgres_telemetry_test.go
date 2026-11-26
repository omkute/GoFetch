package api

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/omkute/gofetch/internal/state"
)

// telemetryState snapshots the optional telemetry fields under the server lock.
func telemetryState(s *Server) (*pgxpool.Pool, *state.PostgresTelemetryConsumer) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.telemetryPool, s.telemetryConsumer
}

func TestPostgresTelemetryConfigWiring(t *testing.T) {
	server := NewServer("")
	if got := server.daemonConfig().PostgresDSN; got != "" {
		t.Errorf("default PostgresDSN = %q, want empty", got)
	}

	const dsn = "postgres://gofetch@127.0.0.1:5432/gofetch"
	if err := server.SetDaemonConfig(DaemonConfig{PostgresDSN: dsn}); err != nil {
		t.Fatalf("SetDaemonConfig failed: %v", err)
	}
	if got := server.daemonConfig().PostgresDSN; got != dsn {
		t.Errorf("wired PostgresDSN = %q, want %q", got, dsn)
	}
}

// An empty DSN must leave the server behavior unchanged: no pool is created,
// no consumer is started, and start/stop are harmless no-ops.
func TestPostgresTelemetry_EmptyDSNIsNoop(t *testing.T) {
	server := NewServer("127.0.0.1:0")
	if err := server.SetDaemonConfig(DaemonConfig{PostgresDSN: ""}); err != nil {
		t.Fatalf("SetDaemonConfig failed: %v", err)
	}

	if err := server.startPostgresTelemetry(); err != nil {
		t.Fatalf("startPostgresTelemetry with empty DSN failed: %v", err)
	}
	pool, consumer := telemetryState(server)
	if pool != nil || consumer != nil {
		t.Fatalf("empty DSN created telemetry pool=%v consumer=%v", pool, consumer)
	}
	server.stopPostgresTelemetry()
	if pool, consumer = telemetryState(server); pool != nil || consumer != nil {
		t.Fatalf("stop changed empty telemetry state: pool=%v consumer=%v", pool, consumer)
	}
}

// With a DSN configured, the daemon creates and starts the consumer at startup
// and stops it (clearing both fields) on shutdown. The DSN points at a closed
// local port: no live Postgres server is needed because the pool connects
// lazily and no events are published during this test.
func TestPostgresTelemetry_EnabledLifecycle(t *testing.T) {
	const dsn = "postgres://gofetch@127.0.0.1:1/gofetch?connect_timeout=1"
	server := NewServer("127.0.0.1:0")
	if err := server.SetDaemonConfig(DaemonConfig{PostgresDSN: dsn}); err != nil {
		t.Fatalf("SetDaemonConfig failed: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- server.Start() }()

	select {
	case <-server.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server failed to start listening in time")
	}

	pool, consumer := telemetryState(server)
	if pool == nil || consumer == nil {
		t.Fatalf("configured DSN did not create telemetry: pool=%v consumer=%v", pool, consumer)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
	if pool, consumer = telemetryState(server); pool != nil || consumer != nil {
		t.Errorf("Shutdown left telemetry running: pool=%v consumer=%v", pool, consumer)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server.Start() error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestPostgresTelemetry_InvalidDSN(t *testing.T) {
	server := NewServer("")
	if err := server.SetDaemonConfig(DaemonConfig{PostgresDSN: "postgres://%zz@127.0.0.1:5432/gofetch"}); err != nil {
		t.Fatalf("SetDaemonConfig failed: %v", err)
	}
	if err := server.startPostgresTelemetry(); err == nil {
		t.Fatal("expected an error for an invalid postgres_dsn, got nil")
	}
	if pool, consumer := telemetryState(server); pool != nil || consumer != nil {
		t.Errorf("invalid DSN created telemetry: pool=%v consumer=%v", pool, consumer)
	}
}
