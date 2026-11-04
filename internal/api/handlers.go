package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	chi "github.com/go-chi/chi/v5"
	"github.com/omkute/gofetch/internal/classifier"
	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/queue"
	"github.com/omkute/gofetch/internal/state"
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
	rateLimitStr    string
	rateLimiter     *engine.RateLimiter
	Error           string
	cancel          context.CancelFunc
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Server) persistJob(job *jobRecord) {
	s.mu.RLock()
	ts := s.taskStore
	s.mu.RUnlock()
	if ts == nil || job == nil {
		return
	}

	_ = ts.SaveTask(&state.PersistedTask{
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
		DestDir:         job.destDir,
		MaxChunks:       job.maxChunks,
		RateLimit:       job.rateLimitStr,
		CreatedAt:       job.CreatedAt,
		UpdatedAt:       time.Now().UTC(),
	})
}

func (s *Server) enqueueDownload(req CreateDownloadRequest) DownloadResponse {
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

	var limiter *engine.RateLimiter
	if req.RateLimit != "" {
		if bps, err := engine.ParseRateLimit(req.RateLimit); err == nil && bps > 0 {
			limiter = engine.NewRateLimiter(bps)
		}
	}

	// Extract filename from URL
	cleanURL := strings.TrimRight(req.URL, "/")
	rawName := path.Base(cleanURL)
	fileName := engine.SanitizeFilename(rawName)

	id := generateID()
	record := &jobRecord{
		ID:           id,
		URL:          req.URL,
		FileName:     fileName,
		TargetPath:   filepath.Join(destDir, fileName),
		Status:       "queued",
		Priority:     priority,
		CreatedAt:    time.Now().UTC(),
		Chunks:       make([]ChunkDetail, 0),
		maxChunks:    maxChunks,
		destDir:      destDir,
		rateLimitStr: req.RateLimit,
		rateLimiter:  limiter,
	}

	s.jobsMu.Lock()
	s.jobs[id] = record
	s.persistJob(record)
	s.jobsMu.Unlock()

	resp := DownloadResponse{
		ID:        record.ID,
		URL:       record.URL,
		FileName:  record.FileName,
		Status:    record.Status,
		Priority:  record.Priority,
		CreatedAt: record.CreatedAt,
	}

	schedTask := &queue.Task{
		ID:        record.ID,
		URL:       record.URL,
		Priority:  queue.ParsePriority(record.Priority),
		CreatedAt: record.CreatedAt,
		Fn: func(taskCtx context.Context) error {
			return s.runDownload(taskCtx, record)
		},
	}

	_ = s.scheduler.Submit(schedTask)
	return resp
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

	resp := s.enqueueDownload(req)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) runDownload(ctx context.Context, record *jobRecord) error {
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.jobsMu.Lock()
	if record.Status == "paused" {
		s.jobsMu.Unlock()
		return nil
	}
	record.cancel = cancel
	record.Status = "downloading"
	s.persistJob(record)
	s.jobsMu.Unlock()

	client := s.getClient()
	probeCtx, probeCancel := context.WithTimeout(taskCtx, 30*time.Second)
	probe, err := engine.Probe(probeCtx, client, record.URL)
	probeCancel()
	if err != nil {
		s.jobsMu.Lock()
		if !errors.Is(err, context.Canceled) {
			record.Status = "failed"
			record.Error = err.Error()
			s.persistJob(record)
			log.Printf("[Daemon] Download [%s] (%s) probe failed: %v", record.ID[:8], record.URL, err)
			if s.eventBus != nil {
				s.eventBus.Publish(state.Event{
					Type:      state.EventDownloadFailed,
					JobID:     record.ID,
					Payload:   err.Error(),
					Timestamp: time.Now(),
				})
			}
		}
		s.jobsMu.Unlock()
		return err
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
	s.persistJob(record)
	log.Printf("[Daemon] Started download [%s]: %s (size: %s, chunks: %d)",
		record.ID[:8], record.FileName, ui.FormatBytes(record.TotalBytes), record.maxChunks)
	s.jobsMu.Unlock()

	if s.eventBus != nil {
		s.eventBus.Publish(state.Event{
			Type:      state.EventDownloadStarted,
			JobID:     record.ID,
			Payload:   record.FileName,
			Timestamp: time.Now(),
		})
	}

	coord := engine.NewCoordinator(client)

	lastBytes := int64(0)
	lastTime := time.Now()
	lastPersist := time.Now()

	// Rate limiting: cascade per-task limiter with global engine limiter if active
	effectiveLimiter := record.rateLimiter
	if effectiveLimiter == nil {
		effectiveLimiter = s.GlobalRateLimiter()
	}

	opts := engine.DownloadOptions{
		DestDir:     targetDir,
		JobID:       record.ID,
		MaxWorkers:  record.maxChunks,
		RateLimiter: effectiveLimiter,
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

			if now.Sub(lastPersist) >= 2*time.Second {
				s.persistJob(record)
				lastPersist = now
			}
			s.jobsMu.Unlock()

			if s.eventBus != nil {
				s.eventBus.Publish(state.Event{
					Type:      state.EventChunkCompleted,
					JobID:     record.ID,
					Payload:   int64(bytesRead),
					Timestamp: now,
				})
			}
		},
	}

	finalPath, err := coord.DownloadWithOptions(taskCtx, probe, opts)

	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()

	if err != nil {
		if errors.Is(err, context.Canceled) {
			record.Status = "paused"
			s.persistJob(record)
			return err
		}
		record.Status = "failed"
		record.Error = err.Error()
		s.persistJob(record)
		log.Printf("[Daemon] Download [%s] transfer failed: %v", record.ID[:8], err)
		if s.eventBus != nil {
			s.eventBus.Publish(state.Event{
				Type:      state.EventDownloadFailed,
				JobID:     record.ID,
				Payload:   err.Error(),
				Timestamp: time.Now(),
			})
		}
		return err
	}

	record.Status = "completed"
	record.DownloadedBytes = record.TotalBytes
	record.SpeedBps = 0
	record.ETASeconds = 0

	// Classify completed file into organized directory
	cls := classifier.NewClassifier()
	if cat, catErr := cls.ClassifyFile(finalPath); catErr == nil {
		record.Category = string(cat)
	}

	router := classifier.NewRouter(targetDir)
	if classifiedPath, routeErr := router.Route(finalPath); routeErr == nil {
		record.TargetPath = classifiedPath
	}

	s.persistJob(record)
	log.Printf("[Daemon] Download [%s] completed successfully: %s", record.ID[:8], record.TargetPath)
	if s.eventBus != nil {
		s.eventBus.Publish(state.Event{
			Type:      state.EventDownloadFinished,
			JobID:     record.ID,
			Payload:   record.TargetPath,
			Timestamp: time.Now(),
		})
	}
	return nil
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
		s.scheduler.Cancel(id)
		if job.cancel != nil {
			job.cancel()
			job.cancel = nil
		}
		s.persistJob(job)
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
	deleteFiles := r.URL.Query().Get("delete_files") == "true"

	s.jobsMu.Lock()
	job, exists := s.jobs[id]
	if exists {
		s.scheduler.Cancel(id)
		if job.cancel != nil {
			job.cancel()
			job.cancel = nil
		}
		if deleteFiles {
			_ = os.Remove(job.TargetPath)
			_ = os.Remove(job.TargetPath + ".gofetch.part")
		}
		delete(s.jobs, id)
		s.mu.RLock()
		ts := s.taskStore
		s.mu.RUnlock()
		if ts != nil {
			_ = ts.DeleteTask(id)
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
		Status: "deleted",
	})
}

