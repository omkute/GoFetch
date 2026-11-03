package main

import (
	"bytes"
	"testing"
)

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

func TestRootCmdPositionalArg(t *testing.T) {
	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetArgs([]string{"https://example.com/test.zip"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error executing root with url: %v", err)
	}

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("Fetching: https://example.com/test.zip")) {
		t.Errorf("expected output to contain 'Fetching: ...', got: %s", output)
	}
}
