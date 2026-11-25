package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/api"
)

type closeNotifyBody struct {
	io.Reader
	once    sync.Once
	onClose func()
}

func (b *closeNotifyBody) Close() error {
	b.once.Do(func() {
		if b.onClose != nil {
			b.onClose()
		}
	})
	return nil
}

// TestIntegration_FIFOSchedulerSingleActiveSubmissionOrder queues five
// downloads with FIFO concurrency of one (the semantics of -j 1 and
// --sequential) and verifies that never more than one download is active and
// that they complete in submission order, even when a later task has higher
// priority.
func TestIntegration_FIFOSchedulerSingleActiveSubmissionOrder(t *testing.T) {
	tempDir := t.TempDir()
	const totalBytes = 4096
	const downloadCount = 5

	server := api.NewServer("127.0.0.1:0")
	server.SetConcurrency(1, true)

	var mu sync.Mutex
	outstanding := make(map[string]int)
	finished := make(map[string]bool)
	activeDownloads := 0
	maxActive := 0
	completed := []string{}

	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Accept-Ranges", "bytes")
		header.Set("Content-Length", strconv.Itoa(totalBytes))
		header.Set("Content-Type", "application/octet-stream")

		name := path.Base(req.URL.Path)
		payload := bytes.Repeat([]byte(name+"-"), totalBytes/2+1)[:totalBytes]

		if req.Method == http.MethodHead {
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        header,
				Body:          io.NopCloser(bytes.NewReader(nil)),
				ContentLength: 0,
				Request:       req,
			}, nil
		}

		start, end := 0, totalBytes-1
		status := http.StatusOK
		if r := req.Header.Get("Range"); r != "" {
			if _, err := fmt.Sscanf(r, "bytes=%d-%d", &start, &end); err != nil {
				return nil, fmt.Errorf("bad range %q: %w", r, err)
			}
			status = http.StatusPartialContent
			header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalBytes))
		}
		if end >= totalBytes {
			end = totalBytes - 1
		}
		if start < 0 || start > end {
			return nil, fmt.Errorf("invalid range bytes=%d-%d", start, end)
		}
		data := payload[start : end+1]
		header.Set("Content-Length", strconv.Itoa(len(data)))

		// Keep all chunk requests of one download in flight long enough that
		// the active-download accounting sees a single overlapping group.
		time.Sleep(10 * time.Millisecond)

		mu.Lock()
		if outstanding[name] == 0 && !finished[name] {
			activeDownloads++
			if activeDownloads > maxActive {
				maxActive = activeDownloads
			}
		}
		outstanding[name]++
		mu.Unlock()

		body := &closeNotifyBody{
			Reader: bytes.NewReader(data),
			onClose: func() {
				mu.Lock()
				outstanding[name]--
				if outstanding[name] == 0 && !finished[name] {
					finished[name] = true
					activeDownloads--
					completed = append(completed, name)
				}
				mu.Unlock()
			},
		}
		return &http.Response{
			StatusCode:    status,
			Header:        header,
			Body:          body,
			ContentLength: int64(len(data)),
			Request:       req,
		}, nil
	})}
	server.SetClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	scheduler := server.Scheduler()
	scheduler.Start(ctx)
	defer scheduler.Stop()

	router := server.Router()
	for i := 1; i <= downloadCount; i++ {
		name := fmt.Sprintf("file-%d.bin", i)
		priority := "normal"
		if i == downloadCount {
			priority = "high" // must not jump ahead in FIFO mode
		}
		reqBody, _ := json.Marshal(api.CreateDownloadRequest{
			URL:            "http://mock.local/" + name,
			DestinationDir: tempDir,
			Priority:       priority,
			NoOrganize:     true,
		})
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/downloads", bytes.NewReader(reqBody)))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("enqueue %s: status %d: %s", name, rec.Code, rec.Body.String())
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/downloads?status=completed", nil))
		if rec.Code == http.StatusOK {
			var list api.ListDownloadsResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &list); err == nil && list.Total == downloadCount {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	max := maxActive
	order := append([]string(nil), completed...)
	mu.Unlock()

	if max > 1 {
		t.Errorf("max simultaneously active downloads = %d, want <= 1", max)
	}
	if len(order) != downloadCount {
		t.Fatalf("completed %d downloads, want %d: %v", len(order), downloadCount, order)
	}
	for i := 0; i < downloadCount; i++ {
		want := fmt.Sprintf("file-%d.bin", i+1)
		if order[i] != want {
			t.Fatalf("completion[%d] = %s, want %s (full order: %v)", i, order[i], want, order)
		}
	}
}
