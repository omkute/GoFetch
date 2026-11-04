package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/omkute/gofetch/internal/engine"
)

// SystemStatsResponse encapsulates engine runtime and queue telemetry.
type SystemStatsResponse struct {
	ActiveDownloads      int    `json:"active_downloads"`
	QueuedDownloads      int    `json:"queued_downloads"`
	CompletedDownloads   int    `json:"completed_downloads"`
	FailedDownloads      int    `json:"failed_downloads"`
	PausedDownloads      int    `json:"paused_downloads"`
	TotalSpeedBps        int64  `json:"total_speed_bps"`
	OpenConnections      int    `json:"open_connections"`
	MemoryAllocatedBytes uint64 `json:"memory_allocated_bytes"`
	UptimeSeconds        int64  `json:"uptime_seconds"`
	GlobalRateLimitBps   int64  `json:"global_rate_limit_bps"`
}

// SetRateLimitRequest accepts human-readable or exact bps rate limits.
type SetRateLimitRequest struct {
	RateLimit    string `json:"rate_limit"`
	RateLimitBps int64  `json:"rate_limit_bps"`
}

func (s *Server) handleSystemStats(w http.ResponseWriter, _ *http.Request) {
	s.jobsMu.RLock()
	var (
		active    int
		queued    int
		completed int
		failed    int
		paused    int
		speed     int64
	)
	for _, job := range s.jobs {
		switch job.Status {
		case "downloading":
			active++
			speed += job.SpeedBps
		case "queued":
			queued++
		case "completed":
			completed++
		case "failed":
			failed++
		case "paused":
			paused++
		}
	}
	s.jobsMu.RUnlock()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	uptime := int64(time.Since(s.startTime).Seconds())
	if uptime < 0 {
		uptime = 0
	}

	limit := int64(0)
	if lim := s.GlobalRateLimiter(); lim != nil {
		limit = lim.Limit()
	}

	resp := SystemStatsResponse{
		ActiveDownloads:      active,
		QueuedDownloads:      queued,
		CompletedDownloads:   completed,
		FailedDownloads:      failed,
		PausedDownloads:      paused,
		TotalSpeedBps:        speed,
		OpenConnections:      active * 4,
		MemoryAllocatedBytes: mem.Alloc,
		UptimeSeconds:        uptime,
		GlobalRateLimitBps:   limit,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleSetGlobalRateLimit(w http.ResponseWriter, r *http.Request) {
	var req SetRateLimitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}

	bps := req.RateLimitBps
	if req.RateLimit != "" {
		parsed, err := engine.ParseRateLimit(req.RateLimit)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
			return
		}
		bps = parsed
	}

	s.SetGlobalRateLimit(bps)
	s.handleSystemStats(w, r)
}
