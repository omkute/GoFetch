package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"path"
	"strings"
	"time"

	chi "github.com/go-chi/chi/v5"
	"github.com/omkute/gofetch/internal/engine"
)

type jobRecord struct {
	ID              string
	URL             string
	FileName        string
	TargetPath      string
	Status          string
	Priority        string
	TotalBytes      int64
	DownloadedBytes int64
	SpeedBps        int64
	ETASeconds      int64
	Category        string
	CreatedAt       time.Time
	Chunks          []ChunkDetail
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Server) handleCreateDownload(w http.ResponseWriter, r *http.Request) {
	var req CreateDownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}

	if req.URL == "" {
		http.Error(w, `{"error":"url is required"}`, http.StatusBadRequest)
		return
	}

	priority := req.Priority
	if priority == "" {
		priority = "normal"
	}

	// Extract filename from URL
	cleanURL := strings.TrimRight(req.URL, "/")
	rawName := path.Base(cleanURL)
	fileName := engine.SanitizeFilename(rawName)

	id := generateID()
	record := &jobRecord{
		ID:        id,
		URL:       req.URL,
		FileName:  fileName,
		Status:    "queued",
		Priority:  priority,
		CreatedAt: time.Now().UTC(),
		Chunks:    make([]ChunkDetail, 0),
	}

	s.jobsMu.Lock()
	s.jobs[id] = record
	s.jobsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(DownloadResponse{
		ID:        record.ID,
		URL:       record.URL,
		FileName:  record.FileName,
		Status:    record.Status,
		Priority:  record.Priority,
		CreatedAt: record.CreatedAt,
	})
}

func (s *Server) handleListDownloads(w http.ResponseWriter, r *http.Request) {
	statusFilter := r.URL.Query().Get("status")

	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()

	items := make([]DownloadItem, 0, len(s.jobs))
	for _, job := range s.jobs {
		if statusFilter != "" && !strings.EqualFold(job.Status, statusFilter) {
			continue
		}

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
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ListDownloadsResponse{
		Items: items,
		Total: len(items),
	})
}

func (s *Server) handleGetDownload(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	s.jobsMu.RLock()
	job, exists := s.jobs[id]
	s.jobsMu.RUnlock()

	if !exists {
		http.Error(w, `{"error":"download not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(DownloadDetailResponse{
		ID:              job.ID,
		URL:             job.URL,
		FileName:        job.FileName,
		TargetPath:      job.TargetPath,
		Status:          job.Status,
		Priority:        job.Priority,
		TotalBytes:      job.TotalBytes,
		DownloadedBytes: job.DownloadedBytes,
		SpeedBps:        job.SpeedBps,
		ETASeconds:      job.ETASeconds,
		Category:        job.Category,
		Chunks:          job.Chunks,
	})
}

func (s *Server) handlePauseDownload(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	s.jobsMu.Lock()
	job, exists := s.jobs[id]
	if exists {
		job.Status = "paused"
	}
	s.jobsMu.Unlock()

	if !exists {
		http.Error(w, `{"error":"download not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ActionResponse{
		ID:     id,
		Status: "paused",
	})
}

func (s *Server) handleResumeDownload(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	s.jobsMu.Lock()
	job, exists := s.jobs[id]
	if exists {
		job.Status = "resumed"
	}
	s.jobsMu.Unlock()

	if !exists {
		http.Error(w, `{"error":"download not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ActionResponse{
		ID:     id,
		Status: "resumed",
	})
}

func (s *Server) handleDeleteDownload(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	s.jobsMu.Lock()
	_, exists := s.jobs[id]
	if exists {
		delete(s.jobs, id)
	}
	s.jobsMu.Unlock()

	if !exists {
		http.Error(w, `{"error":"download not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ActionResponse{
		ID:     id,
		Status: "deleted",
	})
}
