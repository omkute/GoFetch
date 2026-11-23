package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func createMockTransport(content []byte) http.RoundTripper {
	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
		h := make(http.Header)
		h.Set("Accept-Ranges", "bytes")
		h.Set("Content-Length", fmt.Sprintf("%d", len(content)))
		h.Set("Content-Type", "application/octet-stream")

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
	})
}

func TestRootCmdHelp(t *testing.T) {
	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetArgs([]string{"--help"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error executing root help: %v", err)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("GoFetch is a concurrent")) {
		t.Errorf("expected help output to mention GoFetch description, got: %s", output)
	}
}

func TestDaemonCmdFlag(t *testing.T) {
	daemonCmd := newDaemonCmd()
	flag := daemonCmd.Flag("addr")
	if flag == nil {
		t.Fatal("expected 'addr' flag on daemon command")
	}
	if flag.DefValue != "127.0.0.1:12701" {
		t.Errorf("expected default addr '127.0.0.1:12701', got %q", flag.DefValue)
	}
}

func TestRootCmd_StandaloneExecution(t *testing.T) {
	tempDir := t.TempDir()
	content := []byte("Hello, GoFetch standalone CLI execution!")

	origTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = origTransport }()
	http.DefaultTransport = createMockTransport(content)

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"http://mock.local/sample.txt",
		"--dir", tempDir,
		"--no-organize",
		"--quiet",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("root standalone execution failed: %v", err)
	}

	targetFile := filepath.Join(tempDir, "sample.txt")
	downloaded, err := os.ReadFile(targetFile)
	if err != nil {
		t.Fatalf("failed reading downloaded file %q: %v", targetFile, err)
	}
	if !bytes.Equal(downloaded, content) {
		t.Errorf("content mismatch: got %q, want %q", string(downloaded), string(content))
	}
}

func TestRootCmd_StandaloneChecksumVerification(t *testing.T) {
	tempDir := t.TempDir()
	content := []byte("Cryptographic checksum verification content.")
	hasher := sha256.New()
	hasher.Write(content)
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	origTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = origTransport }()
	http.DefaultTransport = createMockTransport(content)

	// 1. Success matching hash
	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"http://mock.local/verify.txt",
		"--dir", tempDir,
		"--no-organize",
		"--quiet",
		"--checksum", expectedHash,
		"--checksum-algo", "sha256",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("checksum verification expected success, got error: %v", err)
	}

	// 2. Failure with wrong hash
	cmdBad := newRootCmd()
	bufBad := new(bytes.Buffer)
	cmdBad.SetOut(bufBad)
	cmdBad.SetErr(bufBad)
	cmdBad.SetArgs([]string{
		"http://mock.local/verify.txt",
		"--dir", tempDir,
		"--no-organize",
		"--quiet",
		"--checksum", "badhash0000000000000000000000000000000000000000000000000000000000",
		"--checksum-algo", "sha256",
	})

	errBad := cmdBad.Execute()
	if errBad == nil {
		t.Fatal("expected error on checksum mismatch, got nil")
	}
	if !strings.Contains(errBad.Error(), "checksum mismatch") {
		t.Errorf("expected checksum mismatch error, got: %v", errBad)
	}
}

func TestRootCmd_StandaloneJSONOutput(t *testing.T) {
	tempDir := t.TempDir()
	content := bytes.Repeat([]byte("GoFetch JSON telemetry test line! "), 200)

	origTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = origTransport }()
	http.DefaultTransport = createMockTransport(content)

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"http://mock.local/stream.bin",
		"--dir", tempDir,
		"--no-organize",
		"--json",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("json mode execution failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, `"status":"completed"`) {
		t.Errorf("expected JSON completed event, got:\n%s", output)
	}
}
