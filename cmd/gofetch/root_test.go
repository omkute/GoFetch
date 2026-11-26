package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newStandaloneTestOrigin serves content with real HTTP range support so CLI
// tests can exercise the streaming client without reaching the network.
func newStandaloneTestOrigin(t *testing.T, content []byte) *httptest.Server {
	t.Helper()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.ServeContent(w, req, filepath.Base(req.URL.Path), time.Time{}, bytes.NewReader(content))
	}))
	t.Cleanup(origin.Close)
	return origin
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
	origin := newStandaloneTestOrigin(t, content)

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		origin.URL + "/sample.txt",
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

	origin := newStandaloneTestOrigin(t, content)

	// 1. Success matching hash
	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		origin.URL + "/verify.txt",
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
		origin.URL + "/verify.txt",
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
	origin := newStandaloneTestOrigin(t, content)

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		origin.URL + "/stream.bin",
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
