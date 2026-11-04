package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/omkute/gofetch/internal/queue"
)

// BatchDownloadRequest contains multiple URLs for simultaneous batch registration.
type BatchDownloadRequest struct {
	URLs           []string `json:"urls"`
	DestinationDir string   `json:"destination_dir"`
	Priority       string   `json:"priority"`
	MaxChunks      int      `json:"max_chunks"`
	RateLimit      string   `json:"rate_limit"`
}

// BatchDownloadResponse confirms the enqueued tasks.
type BatchDownloadResponse struct {
	Enqueued int                `json:"enqueued"`
	Items    []DownloadResponse `json:"items"`
}

func (s *Server) handleBatchCreateDownloads(w http.ResponseWriter, r *http.Request) {
	var req BatchDownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}

	if len(req.URLs) == 0 {
		http.Error(w, `{"error":"urls list cannot be empty"}`, http.StatusBadRequest)
		return
	}

	items := make([]DownloadResponse, 0, len(req.URLs))
	for _, rawURL := range req.URLs {
		if rawURL == "" {
			continue
		}
		singleReq := CreateDownloadRequest{
			URL:            rawURL,
			DestinationDir: req.DestinationDir,
			Priority:       req.Priority,
			MaxChunks:      req.MaxChunks,
			RateLimit:      req.RateLimit,
		}
		resp := s.enqueueDownload(singleReq)
		items = append(items, resp)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(BatchDownloadResponse{
		Enqueued: len(items),
		Items:    items,
	})
}

func (s *Server) handlePauseAllDownloads(w http.ResponseWriter, _ *http.Request) {
	s.jobsMu.Lock()
	for _, job := range s.jobs {
		if job.Status == "downloading" || job.Status == "queued" {
			job.Status = "paused"
			s.scheduler.Cancel(job.ID)
			if job.cancel != nil {
				job.cancel()
				job.cancel = nil
			}
			s.persistJob(job)
		}
	}
	s.jobsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ActionResponse{ID: "all", Status: "paused"})
}

func (s *Server) handleResumeAllDownloads(w http.ResponseWriter, _ *http.Request) {
	s.jobsMu.Lock()
	for _, job := range s.jobs {
		if job.Status == "paused" {
			job.Status = "queued"
			job.Error = ""
			s.persistJob(job)
			schedTask := &queue.Task{
				ID:        job.ID,
				URL:       job.URL,
				Priority:  queue.ParsePriority(job.Priority),
				CreatedAt: time.Now().UTC(),
				Fn: func(taskCtx context.Context) error {
					return s.runDownload(taskCtx, job)
				},
			}
			_ = s.scheduler.Submit(schedTask)
		}
	}
	s.jobsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ActionResponse{ID: "all", Status: "resumed"})
}
