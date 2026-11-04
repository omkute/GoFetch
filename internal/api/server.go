package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/queue"
	"github.com/omkute/gofetch/internal/state"
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

	httpClient *http.Client
	jobs       map[string]*jobRecord
	jobsMu     sync.RWMutex

	scheduler         *queue.Scheduler
	taskStore         *state.TaskStore
	eventBus          *state.EventBus
	globalRateLimiter *engine.RateLimiter
	startTime         time.Time
}

func defaultStreamingClient() *http.Client {
	return &http.Client{
		Timeout: 0, // No overall timeout; allows streaming arbitrarily large files without arbitrary deadlines
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

// NewServer creates a new Server instance configured for the provided address.
// If addr is empty, DefaultAddr is used.
func NewServer(addr string) *Server {
	if addr == "" {
		addr = DefaultAddr
	}
	return &Server{
		addr:              addr,
		ready:             make(chan struct{}),
		jobs:              make(map[string]*jobRecord),
		httpClient:        defaultStreamingClient(),
		scheduler:         queue.NewScheduler(3),
		eventBus:          state.NewEventBus(1024),
		globalRateLimiter: engine.NewRateLimiter(0),
		startTime:         time.Now().UTC(),
	}
}

// SetClient sets a custom http.Client for outbound probing and chunk downloads.
func (s *Server) SetClient(client *http.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.httpClient = client
}

func (s *Server) getClient() *http.Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.httpClient != nil {
		return s.httpClient
	}
	return http.DefaultClient
}

// SetTaskStore sets a persistent TaskStore for daemon task durability.
func (s *Server) SetTaskStore(store *state.TaskStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.taskStore = store
}

// TaskStore returns the server's task store if configured.
func (s *Server) TaskStore() *state.TaskStore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.taskStore
}

// SetScheduler replaces the queue scheduler.
func (s *Server) SetScheduler(sched *queue.Scheduler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduler = sched
}

// Scheduler returns the active queue scheduler.
func (s *Server) Scheduler() *queue.Scheduler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.scheduler
}

// EventBus returns the active event bus.
func (s *Server) EventBus() *state.EventBus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.eventBus
}

// GlobalRateLimiter returns the engine's global bandwidth limiter.
func (s *Server) GlobalRateLimiter() *engine.RateLimiter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.globalRateLimiter
}

// SetGlobalRateLimit sets the global bandwidth cap in bytes per second.
func (s *Server) SetGlobalRateLimit(bps int64) {
	s.mu.RLock()
	lim := s.globalRateLimiter
	s.mu.RUnlock()
	if lim != nil {
		lim.SetLimit(bps)
	}
}

func (s *Server) loadPersistedTasks() {
	s.mu.RLock()
	ts := s.taskStore
	s.mu.RUnlock()
	if ts == nil {
		return
	}

	tasks, err := ts.ListTasks()
	if err != nil {
		return
	}

	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	for _, t := range tasks {
		status := t.Status
		errMsg := t.Error
		if status == "downloading" || status == "queued" {
			status = "paused"
			errMsg = "Interrupted by daemon restart"
			t.Status = status
			t.Error = errMsg
			_ = ts.SaveTask(t)
		}

		s.jobs[t.ID] = &jobRecord{
			ID:              t.ID,
			URL:             t.URL,
			FileName:        t.FileName,
			TargetPath:      t.TargetPath,
			Status:          status,
			Priority:        t.Priority,
			TotalBytes:      t.TotalBytes,
			DownloadedBytes: t.DownloadedBytes,
			SpeedBps:        t.SpeedBps,
			ETASeconds:      t.ETASeconds,
			Category:        t.Category,
			CreatedAt:       t.CreatedAt,
			Chunks:          make([]ChunkDetail, 0),
			maxChunks:       t.MaxChunks,
			destDir:         t.DestDir,
			rateLimitStr:    t.RateLimit,
			Error:           errMsg,
		}
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
	s.scheduler.Start(context.Background())
	s.loadPersistedTasks()

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
	s.scheduler.Stop()

	s.jobsMu.Lock()
	for _, job := range s.jobs {
		if job.cancel != nil {
			job.cancel()
		}
	}
	s.jobsMu.Unlock()

	s.mu.Lock()
	server := s.httpServer
	ts := s.taskStore
	s.mu.Unlock()

	if ts != nil {
		_ = ts.Close()
	}
	s.eventBus.Close()

	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}
