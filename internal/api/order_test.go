package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The GUI rebuilds its table from every SSE progress frame. A static task set
// must therefore produce the same row order on every frame and on every REST
// list response; Go's randomized map iteration used to reshuffle it each time.
func TestDownloadsOrderStableAcrossSSEFrames(t *testing.T) {
	s := NewServer("127.0.0.1:0")
	s.jobsMu.Lock()
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("task-%02d", i)
		s.jobs[id] = &jobRecord{ID: id, Status: "queued", CreatedAt: time.Unix(int64(i), 0).UTC()}
	}
	s.jobsMu.Unlock()

	ts := httptest.NewServer(s.Router())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	var orders []string
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var payload SSEProgressPayload
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &payload); err != nil {
			continue
		}
		if len(payload.Downloads) == 0 {
			continue
		}
		ids := make([]string, 0, len(payload.Downloads))
		for _, d := range payload.Downloads {
			ids = append(ids, d.ID)
		}
		orders = append(orders, strings.Join(ids, ","))
		if len(orders) >= 8 {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(orders) < 2 {
		t.Fatalf("only collected %d progress frames", len(orders))
	}
	for i := 1; i < len(orders); i++ {
		if orders[i] != orders[0] {
			t.Fatalf("queue order changed between progress frames:\n  frame 0: %s\n  frame %d: %s", orders[0], i, orders[i])
		}
	}
	want := "task-00,task-01,task-02,task-03,task-04,task-05,task-06,task-07"
	if orders[0] != want {
		t.Fatalf("progress frame order = %q, want submission order %q", orders[0], want)
	}
}

func TestDownloadsOrderStableAcrossListRequests(t *testing.T) {
	s := NewServer("127.0.0.1:0")
	s.jobsMu.Lock()
	for i := 7; i >= 0; i-- {
		id := fmt.Sprintf("task-%02d", i)
		s.jobs[id] = &jobRecord{ID: id, Status: "queued", CreatedAt: time.Unix(int64(i), 0).UTC()}
	}
	s.jobsMu.Unlock()

	ts := httptest.NewServer(s.Router())
	defer ts.Close()

	want := "task-00,task-01,task-02,task-03,task-04,task-05,task-06,task-07"
	for attempt := 0; attempt < 8; attempt++ {
		resp, err := http.Get(ts.URL + "/api/v1/downloads")
		if err != nil {
			t.Fatal(err)
		}
		var result ListDownloadsResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(result.Items))
		for _, item := range result.Items {
			ids = append(ids, item.ID)
		}
		if got := strings.Join(ids, ","); got != want {
			t.Fatalf("list attempt %d order = %q, want %q", attempt, got, want)
		}
	}
}
