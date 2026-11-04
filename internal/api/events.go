package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/omkute/gofetch/internal/state"
)

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

	ticker := time.NewTicker(500 * time.Millisecond)
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

func (s *Server) sendProgressFrame(w http.ResponseWriter, flusher http.Flusher) {
	s.jobsMu.RLock()
	var totalSpeed int64
	activeCount := 0
	items := make([]DownloadItem, 0, len(s.jobs))
	for _, job := range s.jobs {
		pct := 0.0
		if job.TotalBytes > 0 {
			pct = (float64(job.DownloadedBytes) / float64(job.TotalBytes)) * 100.0
		}
		if job.Status == "downloading" {
			activeCount++
			totalSpeed += job.SpeedBps
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
	s.jobsMu.RUnlock()

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
