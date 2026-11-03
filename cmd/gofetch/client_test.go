package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/omkute/gofetch/internal/api"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestAddCommand_Flags(t *testing.T) {
	cmd := newAddCmd()
	if cmd.Use != "add [url]" {
		t.Errorf("expected use 'add [url]', got %q", cmd.Use)
	}

	priorityFlag := cmd.Flag("priority")
	if priorityFlag == nil {
		t.Fatal("expected 'priority' flag on add command")
	}
	if priorityFlag.DefValue != "normal" {
		t.Errorf("expected default priority 'normal', got %q", priorityFlag.DefValue)
	}

	chunksFlag := cmd.Flag("chunks")
	if chunksFlag == nil {
		t.Fatal("expected 'chunks' flag on add command")
	}
	if chunksFlag.DefValue != "4" {
		t.Errorf("expected default chunks '4', got %q", chunksFlag.DefValue)
	}
}

func TestListCommand_Flags(t *testing.T) {
	cmd := newListCmd()
	if cmd.Use != "list" {
		t.Errorf("expected use 'list', got %q", cmd.Use)
	}

	statusFlag := cmd.Flag("status")
	if statusFlag == nil {
		t.Fatal("expected 'status' flag on list command")
	}
}

func TestPauseCommand_Args(t *testing.T) {
	cmd := newPauseCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{}) // No ID arg provided

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error executing pause without task ID")
	}
	if !strings.Contains(err.Error(), "requires") && !strings.Contains(buf.String(), "Usage") {
		t.Errorf("expected missing arg error message, got %v", err)
	}
}

func TestResumeCommand_Args(t *testing.T) {
	cmd := newResumeCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error executing resume without task ID")
	}
}

func TestAddCommand_Execute(t *testing.T) {
	origTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = origTransport }()

	var interceptedReq *http.Request
	http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		interceptedReq = req
		respBody, _ := json.Marshal(api.DownloadResponse{
			ID:        "abc12345",
			URL:       "https://example.com/archive.zip",
			FileName:  "archive.zip",
			Status:    "queued",
			Priority:  "high",
			CreatedAt: time.Now(),
		})
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Body:       io.NopCloser(bytes.NewReader(respBody)),
			Header:     make(http.Header),
		}, nil
	})

	cmd := newAddCmd()
	outBuf := new(bytes.Buffer)
	cmd.SetOut(outBuf)
	cmd.SetErr(outBuf)
	cmd.SetArgs([]string{"https://example.com/archive.zip", "--priority", "high", "--chunks", "8"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("unexpected command error: %v", err)
	}

	if interceptedReq == nil {
		t.Fatal("expected HTTP request to be intercepted")
	}
	if interceptedReq.Method != http.MethodPost {
		t.Errorf("expected POST method, got %s", interceptedReq.Method)
	}
	if !strings.Contains(outBuf.String(), "Enqueued download [abc12345]") {
		t.Errorf("expected output to contain 'Enqueued download [abc12345]', got: %s", outBuf.String())
	}
}

func TestListCommand_Execute(t *testing.T) {
	origTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = origTransport }()

	http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		respBody, _ := json.Marshal(api.ListDownloadsResponse{
			Items: []api.DownloadItem{
				{
					ID:              "dl-001",
					FileName:        "video.mp4",
					Status:          "downloading",
					Priority:        "high",
					ProgressPercent: 55.5,
					SpeedBps:        1024 * 1024 * 5,
					ETASeconds:      60,
				},
			},
			Total: 1,
		})
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(respBody)),
			Header:     make(http.Header),
		}, nil
	})

	cmd := newListCmd()
	outBuf := new(bytes.Buffer)
	cmd.SetOut(outBuf)
	cmd.SetErr(outBuf)
	cmd.SetArgs([]string{"--status", "downloading"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("unexpected command error: %v", err)
	}

	out := outBuf.String()
	if !strings.Contains(out, "dl-001") || !strings.Contains(out, "video.mp4") || !strings.Contains(out, "55.5%") {
		t.Errorf("unexpected list output: %s", out)
	}
}

func TestPauseAndResumeCommand_Execute(t *testing.T) {
	origTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = origTransport }()

	http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var action string
		if strings.HasSuffix(req.URL.Path, "/pause") {
			action = "paused"
		} else if strings.HasSuffix(req.URL.Path, "/resume") {
			action = "resumed"
		}
		respBody, _ := json.Marshal(api.ActionResponse{
			ID:     "task-42",
			Status: action,
		})
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(respBody)),
			Header:     make(http.Header),
		}, nil
	})

	// Test pause
	pauseCmd := newPauseCmd()
	pBuf := new(bytes.Buffer)
	pauseCmd.SetOut(pBuf)
	pauseCmd.SetErr(pBuf)
	pauseCmd.SetArgs([]string{"task-42"})

	if err := pauseCmd.Execute(); err != nil {
		t.Fatalf("pause command failed: %v", err)
	}
	if !strings.Contains(pBuf.String(), "Paused download [task-42]") {
		t.Errorf("expected paused confirmation, got: %s", pBuf.String())
	}

	// Test resume
	resumeCmd := newResumeCmd()
	rBuf := new(bytes.Buffer)
	resumeCmd.SetOut(rBuf)
	resumeCmd.SetErr(rBuf)
	resumeCmd.SetArgs([]string{"task-42"})

	if err := resumeCmd.Execute(); err != nil {
		t.Fatalf("resume command failed: %v", err)
	}
	if !strings.Contains(rBuf.String(), "Resumed download [task-42]") {
		t.Errorf("expected resumed confirmation, got: %s", rBuf.String())
	}
}

