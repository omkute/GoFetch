package api

import (
	"encoding/json"
	"net/http"

	chi "github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/omkute/gofetch/internal/web"
)

// setupRoutes configures the chi router and all registered API endpoints.
func (s *Server) setupRoutes() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", s.handleHealth)
		r.Get("/events", s.handleEventsSSE)

		r.Route("/system", func(r chi.Router) {
			r.Get("/stats", s.handleSystemStats)
			r.Post("/rate-limit", s.handleSetGlobalRateLimit)
		})

		r.Route("/downloads", func(r chi.Router) {
			r.Post("/", s.handleCreateDownload)
			r.Post("/batch", s.handleBatchCreateDownloads)
			r.Post("/pause-all", s.handlePauseAllDownloads)
			r.Post("/resume-all", s.handleResumeAllDownloads)
			r.Get("/", s.handleListDownloads)
			r.Get("/{id}", s.handleGetDownload)
			r.Post("/{id}/pause", s.handlePauseDownload)
			r.Post("/{id}/resume", s.handleResumeDownload)
			r.Delete("/{id}", s.handleDeleteDownload)
		})
	})

	r.Handle("/*", web.Handler())

	return r
}

type healthResponse struct {
	Status string `json:"status"`
}

// handleHealth responds with the daemon engine health status.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(healthResponse{Status: "ok"})
}
