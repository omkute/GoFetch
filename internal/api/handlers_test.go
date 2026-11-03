package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/omkute/gofetch/internal/api"
)

func TestHandler_DownloadLifecycle(t *testing.T) {
	server := api.NewServer("")
	router := server.Router()

	// 1. Enqueue download: POST /api/v1/downloads
	createReqPayload := api.CreateDownloadRequest{
		URL:            "https://example.com/file.zip",
		DestinationDir: "/downloads",
		Priority:       "high",
		MaxChunks:      4,
		Checksum:       "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		ChecksumAlgo:   "sha256",
	}

	body, _ := json.Marshal(createReqPayload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/downloads", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected status 202 Accepted, got %d: %s", w.Code, w.Body.String())
	}

	var createResp api.DownloadResponse
	if err := json.Unmarshal(w.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("failed to decode create response: %v", err)
	}

	if createResp.ID == "" {
		t.Fatal("expected non-empty download ID")
	}
	if createResp.URL != createReqPayload.URL {
		t.Errorf("expected URL %q, got %q", createReqPayload.URL, createResp.URL)
	}
	if createResp.Priority != "high" {
		t.Errorf("expected priority 'high', got %q", createResp.Priority)
	}

	downloadID := createResp.ID

	// 2. List downloads: GET /api/v1/downloads
	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads", nil)
	listW := httptest.NewRecorder()
	router.ServeHTTP(listW, listReq)

	if listW.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK on list, got %d", listW.Code)
	}

	var listResp api.ListDownloadsResponse
	if err := json.Unmarshal(listW.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("failed to decode list response: %v", err)
	}

	if listResp.Total != 1 || len(listResp.Items) != 1 {
		t.Fatalf("expected 1 item in list, got total=%d, len=%d", listResp.Total, len(listResp.Items))
	}
	if listResp.Items[0].ID != downloadID {
		t.Errorf("expected item ID %q, got %q", downloadID, listResp.Items[0].ID)
	}

	// 3. Get single download: GET /api/v1/downloads/{id}
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads/"+downloadID, nil)
	getW := httptest.NewRecorder()
	router.ServeHTTP(getW, getReq)

	if getW.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK on get, got %d", getW.Code)
	}

	var detailResp api.DownloadDetailResponse
	if err := json.Unmarshal(getW.Body.Bytes(), &detailResp); err != nil {
		t.Fatalf("failed to decode detail response: %v", err)
	}
	if detailResp.ID != downloadID {
		t.Errorf("expected detail ID %q, got %q", downloadID, detailResp.ID)
	}

	// 4. Pause download: POST /api/v1/downloads/{id}/pause
	pauseReq := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/"+downloadID+"/pause", nil)
	pauseW := httptest.NewRecorder()
	router.ServeHTTP(pauseW, pauseReq)

	if pauseW.Code != http.StatusOK {
		t.Fatalf("expected status 200 on pause, got %d: %s", pauseW.Code, pauseW.Body.String())
	}

	var pauseResp api.ActionResponse
	_ = json.Unmarshal(pauseW.Body.Bytes(), &pauseResp)
	if pauseResp.Status != "paused" {
		t.Errorf("expected status 'paused', got %q", pauseResp.Status)
	}

	// 5. Resume download: POST /api/v1/downloads/{id}/resume
	resumeReq := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/"+downloadID+"/resume", nil)
	resumeW := httptest.NewRecorder()
	router.ServeHTTP(resumeW, resumeReq)

	if resumeW.Code != http.StatusOK {
		t.Fatalf("expected status 200 on resume, got %d: %s", resumeW.Code, resumeW.Body.String())
	}

	var resumeResp api.ActionResponse
	_ = json.Unmarshal(resumeW.Body.Bytes(), &resumeResp)
	if resumeResp.Status != "resumed" {
		t.Errorf("expected status 'resumed', got %q", resumeResp.Status)
	}

	// 6. Delete download: DELETE /api/v1/downloads/{id}
	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/downloads/"+downloadID, nil)
	delW := httptest.NewRecorder()
	router.ServeHTTP(delW, delReq)

	if delW.Code != http.StatusOK {
		t.Fatalf("expected status 200 on delete, got %d: %s", delW.Code, delW.Body.String())
	}

	// 7. Verify 404 after deletion: GET /api/v1/downloads/{id}
	checkReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads/"+downloadID, nil)
	checkW := httptest.NewRecorder()
	router.ServeHTTP(checkW, checkReq)

	if checkW.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found after deletion, got %d", checkW.Code)
	}
}
