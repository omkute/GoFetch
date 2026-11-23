package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/api"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestIntegration_DaemonRESTAPILifecycle(t *testing.T) {
	server := api.NewServer("127.0.0.1:12701")
	router := server.Router()

	// 1. Submit download via POST /api/v1/downloads
	payload := api.CreateDownloadRequest{
		URL:            "https://releases.ubuntu.com/24.04/ubuntu.iso",
		DestinationDir: "/tmp",
		Priority:       "high",
		MaxChunks:      8,
	}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/downloads", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", w.Code, w.Body.String())
	}

	var createResp api.DownloadResponse
	if err := json.Unmarshal(w.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("failed decoding create response: %v", err)
	}

	taskID := createResp.ID
	if taskID == "" {
		t.Fatal("expected non-empty task ID")
	}

	// 2. Query list via GET /api/v1/downloads
	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads", nil)
	listW := httptest.NewRecorder()
	router.ServeHTTP(listW, listReq)

	if listW.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on list, got %d", listW.Code)
	}

	var listResp api.ListDownloadsResponse
	if err := json.Unmarshal(listW.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("failed decoding list response: %v", err)
	}
	if listResp.Total != 1 || listResp.Items[0].ID != taskID {
		t.Fatalf("expected 1 task with id %s, got %+v", taskID, listResp)
	}

	// 3. Pause via POST /api/v1/downloads/{id}/pause
	pauseReq := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/"+taskID+"/pause", nil)
	pauseW := httptest.NewRecorder()
	router.ServeHTTP(pauseW, pauseReq)

	if pauseW.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on pause, got %d", pauseW.Code)
	}

	// 4. Resume via POST /api/v1/downloads/{id}/resume
	resumeReq := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/"+taskID+"/resume", nil)
	resumeW := httptest.NewRecorder()
	router.ServeHTTP(resumeW, resumeReq)

	if resumeW.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on resume, got %d", resumeW.Code)
	}

	// 5. Query detailed status via GET /api/v1/downloads/{id}
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads/"+taskID, nil)
	getW := httptest.NewRecorder()
	router.ServeHTTP(getW, getReq)

	if getW.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on detail, got %d", getW.Code)
	}

	var detailResp api.DownloadDetailResponse
	if err := json.Unmarshal(getW.Body.Bytes(), &detailResp); err != nil {
		t.Fatalf("failed decoding detail response: %v", err)
	}
	if detailResp.Status != "resumed" && detailResp.Status != "downloading" {
		t.Errorf("expected resumed or downloading status, got %s", detailResp.Status)
	}

	// 6. Delete via DELETE /api/v1/downloads/{id}
	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/downloads/"+taskID, nil)
	delW := httptest.NewRecorder()
	router.ServeHTTP(delW, delReq)

	if delW.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on delete, got %d", delW.Code)
	}

	// 7. Verify 404
	checkReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads/"+taskID, nil)
	checkW := httptest.NewRecorder()
	router.ServeHTTP(checkW, checkReq)

	if checkW.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found after delete, got %d", checkW.Code)
	}
}

func TestIntegration_DaemonChecksumVerification(t *testing.T) {
	tempDir := t.TempDir()
	server := api.NewServer("127.0.0.1:0")
	router := server.Router()

	content := []byte("Daemon checksum validation content stream!")
	server.SetClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			h := make(http.Header)
			h.Set("Accept-Ranges", "bytes")
			h.Set("Content-Length", fmt.Sprintf("%d", len(content)))
			if req.Method == http.MethodHead {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     h,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			}
			rangeHeader := req.Header.Get("Range")
			if rangeHeader != "" {
				var start, end int64
				_, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
				if err == nil && start >= 0 && end < int64(len(content)) && start <= end {
					slice := content[start : end+1]
					h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
					h.Set("Content-Length", fmt.Sprintf("%d", len(slice)))
					return &http.Response{
						StatusCode: http.StatusPartialContent,
						Header:     h,
						Body:       io.NopCloser(bytes.NewReader(slice)),
					}, nil
				}
			}

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     h,
				Body:       io.NopCloser(bytes.NewReader(content)),
			}, nil
		}),
	})
	server.Scheduler().Start(context.Background())
	defer server.Scheduler().Stop()

	// Enqueue with intentionally incorrect checksum
	reqPayload := api.CreateDownloadRequest{
		URL:            "http://mock.internal/data.bin",
		DestinationDir: tempDir,
		Checksum:       "wronghash000000000000000000000000000000000000000000000000000000",
		ChecksumAlgo:   "sha256",
		NoOrganize:     true,
	}
	body, _ := json.Marshal(reqPayload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/downloads", bytes.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", w.Code, w.Body.String())
	}

	var resp api.DownloadResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed unmarshaling response: %v", err)
	}

	// Wait briefly for worker to probe, download, and fail checksum verification
	var detail api.DownloadDetailResponse
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads/"+resp.ID, nil)
		detailW := httptest.NewRecorder()
		router.ServeHTTP(detailW, detailReq)
		if detailW.Code == http.StatusOK {
			_ = json.Unmarshal(detailW.Body.Bytes(), &detail)
			if detail.Status == "failed" {
				break
			}
		}
	}

	if detail.Status != "failed" {
		t.Fatalf("expected download status 'failed' due to checksum mismatch, got %s", detail.Status)
	}
	if !strings.Contains(detail.Error, "checksum mismatch") {
		t.Errorf("expected checksum mismatch error, got: %s", detail.Error)
	}
}
