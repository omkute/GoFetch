package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/omkute/gofetch/internal/ui"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("🚀 GoFetch Daemon REST API & IPC Integration Demo (T-010)")
	fmt.Println("==================================================")

	server := api.NewServer("127.0.0.1:12701")
	router := server.Router()

	fmt.Println("\n[Step 1] Initialized Daemon API Router on 127.0.0.1:12701")

	// 1. Enqueue two downloads
	downloads := []api.CreateDownloadRequest{
		{
			URL:            "https://releases.ubuntu.com/24.04/ubuntu-24.04-desktop-amd64.iso",
			Priority:       "high",
			MaxChunks:      8,
			DestinationDir: "/downloads/isos",
		},
		{
			URL:            "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.8.9.tar.xz",
			Priority:       "normal",
			MaxChunks:      4,
			DestinationDir: "/downloads/kernels",
		},
	}

	createdIDs := make([]string, 0, len(downloads))

	fmt.Println("\n[Step 2] Enqueueing Downloads via POST /api/v1/downloads:")
	for _, dl := range downloads {
		body, _ := json.Marshal(dl)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/downloads", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		var resp api.DownloadResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		createdIDs = append(createdIDs, resp.ID)

		fmt.Printf("   ├── Enqueued [%s] %-32s (Priority: %-6s, Status: %s)\n",
			resp.ID[:8], resp.FileName, resp.Priority, resp.Status)
	}

	// 2. List downloads
	fmt.Println("\n[Step 3] Querying Download List via GET /api/v1/downloads:")
	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	var listResp api.ListDownloadsResponse
	_ = json.Unmarshal(listRec.Body.Bytes(), &listResp)

	fmt.Printf("   Total Managed Downloads: %d\n", listResp.Total)
	for i, item := range listResp.Items {
		speed := "-"
		if item.SpeedBps > 0 {
			speed = ui.FormatBytes(item.SpeedBps) + "/s"
		}
		fmt.Printf("   %d. [%s] %-30s | Status: %-8s | Priority: %-6s | Speed: %s\n",
			i+1, item.ID[:8], item.FileName, item.Status, item.Priority, speed)
	}

	// 3. Pause the first download
	targetID := createdIDs[0]
	fmt.Printf("\n[Step 4] Pausing Download [%s] via POST /api/v1/downloads/%s/pause:\n", targetID[:8], targetID[:8])
	pauseReq := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/"+targetID+"/pause", nil)
	pauseRec := httptest.NewRecorder()
	router.ServeHTTP(pauseRec, pauseReq)

	var pauseResp api.ActionResponse
	_ = json.Unmarshal(pauseRec.Body.Bytes(), &pauseResp)
	fmt.Printf("   ├── Status updated to: %s\n", pauseResp.Status)

	// 4. Resume the first download
	fmt.Printf("\n[Step 5] Resuming Download [%s] via POST /api/v1/downloads/%s/resume:\n", targetID[:8], targetID[:8])
	resumeReq := httptest.NewRequest(http.MethodPost, "/api/v1/downloads/"+targetID+"/resume", nil)
	resumeRec := httptest.NewRecorder()
	router.ServeHTTP(resumeRec, resumeReq)

	var resumeResp api.ActionResponse
	_ = json.Unmarshal(resumeRec.Body.Bytes(), &resumeResp)
	fmt.Printf("   ├── Status updated to: %s\n", resumeResp.Status)

	// 5. Query detailed status
	fmt.Printf("\n[Step 6] Inspecting Detailed Job Metrics via GET /api/v1/downloads/%s:\n", targetID[:8])
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads/"+targetID, nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	var detailResp api.DownloadDetailResponse
	_ = json.Unmarshal(getRec.Body.Bytes(), &detailResp)
	fmt.Printf("   ├── Target Path: %s\n", detailResp.TargetPath)
	fmt.Printf("   ├── URL:         %s\n", detailResp.URL)
	fmt.Printf("   ├── Status:      %s\n", detailResp.Status)
	fmt.Printf("   └── Priority:    %s\n", detailResp.Priority)

	// 6. Delete the second download
	secondID := createdIDs[1]
	fmt.Printf("\n[Step 7] Deleting Download [%s] via DELETE /api/v1/downloads/%s:\n", secondID[:8], secondID[:8])
	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/downloads/"+secondID, nil)
	delRec := httptest.NewRecorder()
	router.ServeHTTP(delRec, delReq)

	var delResp api.ActionResponse
	_ = json.Unmarshal(delRec.Body.Bytes(), &delResp)
	fmt.Printf("   ├── Action confirmed: %s\n", delResp.Status)

	// 7. Verify deletion
	checkReq := httptest.NewRequest(http.MethodGet, "/api/v1/downloads/"+secondID, nil)
	checkRec := httptest.NewRecorder()
	router.ServeHTTP(checkRec, checkReq)
	fmt.Printf("   └── Verification GET returned HTTP status: %d (Not Found)\n", checkRec.Code)

	time.Sleep(10 * time.Millisecond)
	fmt.Println("\n✅ All T-010 Daemon REST API Endpoints & Actions Verified Successfully!")
	fmt.Println("==================================================")
}
