package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	chi "github.com/go-chi/chi/v5"
	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/ui"
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
	maxChunks       int
	destDir         string
	Error           string
	cancel          context.CancelFunc
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

	destDir := req.DestinationDir
	if destDir == "" {
		destDir = "."
	}

	maxChunks := req.MaxChunks
	if maxChunks <= 0 {
		maxChunks = 4
	}

	// Extract filename from URL
	cleanURL := strings.TrimRight(req.URL, "/")
	rawName := path.Base(cleanURL)
	fileName := engine.SanitizeFilename(rawName)

	id := generateID()
	record := &jobRecord{
		ID:         id,
		URL:        req.URL,
		FileName:   fileName,
		TargetPath: filepath.Join(destDir, fileName),
		Status:     "queued",
		Priority:   priority,
		CreatedAt:  time.Now().UTC(),
		Chunks:     make([]ChunkDetail, 0),
		maxChunks:  maxChunks,
		destDir:    destDir,
	}

	s.jobsMu.Lock()
	s.jobs[id] = record
	s.jobsMu.Unlock()

	resp := DownloadResponse{
		ID:        record.ID,
		URL:       record.URL,
		FileName:  record.FileName,
		Status:    record.Status,
		Priority:  record.Priority,
		CreatedAt: record.CreatedAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(resp)

	go s.runDownload(record)
}

func (s *Server) runDownload(record *jobRecord) {
	ctx, cancel := context.WithCancel(context.Background())

	s.jobsMu.Lock()
	if record.Status == "paused" {
		s.jobsMu.Unlock()
		cancel()
		return
	}
	record.cancel = cancel
	record.Status = "downloading"
	s.jobsMu.Unlock()

	client := s.getClient()
	probeCtx, probeCancel := context.WithTimeout(ctx, 30*time.Second)
	probe, err := engine.Probe(probeCtx, client, record.URL)
	probeCancel()
	if err != nil {
		s.jobsMu.Lock()
		if !errors.Is(err, context.Canceled) {
			record.Status = "failed"
			record.Error = err.Error()
			log.Printf("[Daemon] Download [%s] (%s) probe failed: %v", record.ID[:8], record.URL, err)
		}
		s.jobsMu.Unlock()
		return
	}

	targetDir := record.destDir
	if targetDir == "" {
		targetDir = "."
	}

	s.jobsMu.Lock()
	if probe.Filename != "" {
		record.FileName = probe.Filename
		record.TargetPath = filepath.Join(targetDir, probe.Filename)
	}
	record.TotalBytes = probe.ContentLength
	log.Printf("[Daemon] Started download [%s]: %s (size: %s, chunks: %d)",
		record.ID[:8], record.FileName, ui.FormatBytes(record.TotalBytes), record.maxChunks)
	s.jobsMu.Unlock()

	coord := engine.NewCoordinator(client)

	lastBytes := int64(0)
	lastTime := time.Now()

	opts := engine.DownloadOptions{
		DestDir:    targetDir,
		JobID:      record.ID,
		MaxWorkers: record.maxChunks,
		ProgressFn: func(chunkIndex int, bytesRead int) {
			s.jobsMu.Lock()
			record.DownloadedBytes += int64(bytesRead)
			now := time.Now()
			dur := now.Sub(lastTime).Seconds()
			if dur >= 0.25 {
				delta := record.DownloadedBytes - lastBytes
				if dur > 0 {
					record.SpeedBps = int64(float64(delta) / dur)
				}
				lastBytes = record.DownloadedBytes
				lastTime = now
				if record.SpeedBps > 0 && record.TotalBytes > record.DownloadedBytes {
					record.ETASeconds = (record.TotalBytes - record.DownloadedBytes) / record.SpeedBps
				}
			}
			s.jobsMu.Unlock()
		},
	}

	finalPath, err := coord.DownloadWithOptions(ctx, probe, opts)

	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()

	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		record.Status = "failed"
		record.Error = err.Error()
		log.Printf("[Daemon] Download [%s] transfer failed: %v", record.ID[:8], err)
		return
	}

	record.Status = "completed"
	record.DownloadedBytes = record.TotalBytes
	record.TargetPath = finalPath
	record.SpeedBps = 0
	record.ETASeconds = 0
	log.Printf("[Daemon] Completed download [%s]: %s -> %s", record.ID[:8], record.FileName, finalPath)
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
			Error:           job.Error,
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
	if !exists {
		s.jobsMu.RUnlock()
		http.Error(w, `{"error":"download not found"}`, http.StatusNotFound)
		return
	}

	resp := DownloadDetailResponse{
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
		Error:           job.Error,
		Chunks:          job.Chunks,
	}
	s.jobsMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePauseDownload(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	s.jobsMu.Lock()
	job, exists := s.jobs[id]
	if exists {
		job.Status = "paused"
		if job.cancel != nil {
			job.cancel()
			job.cancel = nil
		}
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
		job.Error = ""
		go s.runDownload(job)
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
	job, exists := s.jobs[id]
	if exists {
		if job.cancel != nil {
			job.cancel()
			job.cancel = nil
		}
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

