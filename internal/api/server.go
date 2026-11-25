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

// persistBufferSize bounds the asynchronous progress-persistence handoff.
const persistBufferSize = 256

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
	checkpointStore   engine.CheckpointStore
	eventBus          *state.EventBus
	globalRateLimiter *engine.RateLimiter
	startTime         time.Time

	// Progress snapshots are handed off through a buffered channel so that no
	// database call is made from the chunk transfer goroutines.
	persistCh     chan state.PersistedTask
	persistCancel context.CancelFunc
	persistDone   chan struct{}
	persistOnce   sync.Once
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
		persistCh:         make(chan state.PersistedTask, persistBufferSize),
		persistDone:       make(chan struct{}),
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

// SetCheckpointStore sets the store used to persist resumable chunk offsets.
func (s *Server) SetCheckpointStore(store engine.CheckpointStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpointStore = store
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
			checksum:        t.Checksum,
			checksumAlgo:    t.ChecksumAlgo,
			autoClassify:    t.AutoClassify,
			Error:           errMsg,
		}
	}
}

// savePersistedTask performs the actual DB write from the background consumer.
func (s *Server) savePersistedTask(snap *state.PersistedTask) {
	if snap == nil {
		return
	}
	s.mu.RLock()
	ts := s.taskStore
	s.mu.RUnlock()
	if ts != nil {
		_ = ts.SaveTask(snap)
	}
}

// runPersistConsumer drains progress snapshots off the hot path and persists
// them in a dedicated goroutine. Shutdown cancels it and waits for a final drain.
func (s *Server) runPersistConsumer(ctx context.Context) {
	defer s.persistOnce.Do(func() { close(s.persistDone) })

	for {
		select {
		case snap := <-s.persistCh:
			s.savePersistedTask(&snap)
		case <-ctx.Done():
			for {
				select {
				case snap := <-s.persistCh:
					s.savePersistedTask(&snap)
				default:
					return
				}
			}
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

	persistCtx, cancelPersist := context.WithCancel(context.Background())
	s.mu.Lock()
	s.persistCancel = cancelPersist
	s.mu.Unlock()
	go s.runPersistConsumer(persistCtx)

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

	// Stop and drain the async persistence consumer before closing its store.
	s.mu.RLock()
	cancelPersist := s.persistCancel
	s.mu.RUnlock()
	if cancelPersist != nil {
		cancelPersist()
		<-s.persistDone
	}

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
	cpStore := s.checkpointStore
	s.mu.Unlock()

	if ts != nil {
		_ = ts.Close()
	}
	if cpStore != nil {
		_ = cpStore.Close()
	}
	s.eventBus.Close()

	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}
