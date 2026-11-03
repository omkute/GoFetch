package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// DefaultAddr is the standard loopback address and port for the GoFetch daemon.
const DefaultAddr = "127.0.0.1:12701"

// Server represents the GoFetch loopback HTTP daemon engine.
type Server struct {
	addr       string
	actualAddr string
	listener   net.Listener
	httpServer *http.Server
	ready      chan struct{}
	mu         sync.RWMutex

	jobs   map[string]*jobRecord
	jobsMu sync.RWMutex
}

// NewServer creates a new Server instance configured for the provided address.
// If addr is empty, DefaultAddr is used.
func NewServer(addr string) *Server {
	if addr == "" {
		addr = DefaultAddr
	}
	return &Server{
		addr:  addr,
		ready: make(chan struct{}),
		jobs:  make(map[string]*jobRecord),
	}
}

// Router returns the configured HTTP handler with all registered API routes.
func (s *Server) Router() http.Handler {
	return s.setupRoutes()
}

// Addr returns the configured address or active listening address if started.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.actualAddr != "" {
		return s.actualAddr
	}
	return s.addr
}

// Ready returns a channel that is closed once the server listener is active.
func (s *Server) Ready() <-chan struct{} {
	return s.ready
}

// Start begins listening on the configured address and serves incoming HTTP requests.
func (s *Server) Start() error {
	s.mu.Lock()
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to bind to %s: %w", s.addr, err)
	}

	s.listener = ln
	s.actualAddr = ln.Addr().String()
	s.httpServer = &http.Server{
		Handler:           s.setupRoutes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	close(s.ready)
	s.mu.Unlock()

	if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server error: %w", err)
	}
	return nil
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	server := s.httpServer
	s.mu.Unlock()

	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}
