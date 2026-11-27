package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/omkute/gofetch/internal/state"
)

// SSEProgressInterval is the cadence at which progress frames are pushed to
// SSE clients. It must stay below 100ms to satisfy the sub-100ms real-time
// progress requirement. Lifecycle events are still forwarded immediately as
// they arrive; only the periodic progress snapshot is governed by this value.
const SSEProgressInterval = 50 * time.Millisecond

// SSEProgressPayload aggregates active download statistics for real-time dashboards.
type SSEProgressPayload struct {
	ActiveDownloads int            `json:"active_downloads"`
	TotalSpeedBps   int64          `json:"total_speed_bps"`
	Downloads       []DownloadItem `json:"downloads"`
}

func (s *Server) handleEventsSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Subscribe to lifecycle event bus
	subID, subCh := s.eventBus.Subscribe(
		64,
		state.EventDownloadStarted,
		state.EventChunkCompleted,
		state.EventDownloadFinished,
		state.EventDownloadFailed,
	)
	defer s.eventBus.Unsubscribe(subID)

	ticker := time.NewTicker(SSEProgressInterval)
	defer ticker.Stop()

	// Send initial state snapshot immediately
	s.sendProgressFrame(w, flusher)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-subCh:
			if !ok {
				return
			}
			data, _ := json.Marshal(evt)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Type, string(data))
			flusher.Flush()
		case <-ticker.C:
			s.sendProgressFrame(w, flusher)
		}
	}
}

// sortedDownloadItems returns a deterministic snapshot of jobs in submission
// order (oldest first, ID as tie-breaker). Go randomizes map iteration order on
// every range, so emitting jobs directly from s.jobs makes the GUI table
// reshuffle on every SSE frame even when the task set is unchanged.
func (s *Server) sortedDownloadItems(statusFilter string) ([]DownloadItem, int64, int) {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()

	jobs := make([]*jobRecord, 0, len(s.jobs))
	var totalSpeed int64
	activeCount := 0
	for _, job := range s.jobs {
		if statusFilter != "" && !strings.EqualFold(job.Status, statusFilter) {
			continue
		}
		jobs = append(jobs, job)
		if job.Status == "downloading" {
			activeCount++
			totalSpeed += job.SpeedBps
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].ID < jobs[j].ID
		}
		return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
	})

	items := make([]DownloadItem, 0, len(jobs))
	for _, job := range jobs {
		pct := 0.0
		if job.TotalBytes > 0 {
			pct = (float64(job.DownloadedBytes) / float64(job.TotalBytes)) * 100.0
		}
		items = append(items, DownloadItem{
			ID:              job.ID,
			URL:             job.URL,
			FileName:        job.FileName,
			Status:          job.Status,
			Priority:        job.Priority,
			TotalBytes:      job.TotalBytes,
			DownloadedBytes: job.DownloadedBytes,
			ProgressPercent: pct,
			SpeedBps:        job.SpeedBps,
			ETASeconds:      job.ETASeconds,
			Category:        job.Category,
			Error:           job.Error,
		})
	}
	return items, totalSpeed, activeCount
}

func (s *Server) sendProgressFrame(w http.ResponseWriter, flusher http.Flusher) {
	items, totalSpeed, activeCount := s.sortedDownloadItems("")

	payload := SSEProgressPayload{
		ActiveDownloads: activeCount,
		TotalSpeedBps:   totalSpeed,
		Downloads:       items,
	}

	data, err := json.Marshal(payload)
	if err == nil {
		_, _ = fmt.Fprintf(w, "event: progress\ndata: %s\n\n", string(data))
		flusher.Flush()
	}
}
