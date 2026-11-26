package main

import (
	"bytes"
	"testing"
)

func TestConcurrencyFlagsExist(t *testing.T) {
	root := newRootCmd()
	rootFlag := root.Flag("max-concurrent")
	if rootFlag == nil {
		t.Fatal("root command is missing --max-concurrent")
	}
	if rootFlag.DefValue != "3" {
		t.Errorf("root --max-concurrent default = %q, want \"3\"", rootFlag.DefValue)
	}
	if rootFlag.Shorthand != "j" {
		t.Errorf("root --max-concurrent shorthand = %q, want \"j\"", rootFlag.Shorthand)
	}
	if root.Flag("sequential") == nil {
		t.Error("root command is missing --sequential")
	}
	if root.Flag("chunk-sequential") == nil {
		t.Error("root command is missing --chunk-sequential")
	}

	daemon := newDaemonCmd()
	daemonFlag := daemon.Flag("max-concurrent")
	if daemonFlag == nil {
		t.Fatal("daemon command is missing --max-concurrent")
	}
	if daemonFlag.DefValue != "3" {
		t.Errorf("daemon --max-concurrent default = %q, want \"3\"", daemonFlag.DefValue)
	}
	if daemonFlag.Shorthand != "j" {
		t.Errorf("daemon --max-concurrent shorthand = %q, want \"j\"", daemonFlag.Shorthand)
	}
	if daemon.Flag("sequential") == nil {
		t.Error("daemon command is missing --sequential")
	}
}

func TestResolveDaemonConcurrency(t *testing.T) {
	tests := []struct {
		name        string
		flagValue   int
		flagChanged bool
		configValue int
		sequential  bool
		wantLimit   int
		wantFIFO    bool
	}{
		{"default when unset", 3, false, 0, false, 3, false},
		{"config used when flag unset", 3, false, 7, false, 7, false},
		{"explicit flag overrides config", 2, true, 7, false, 2, false},
		{"sequential implies one and FIFO", 3, false, 0, true, 1, true},
		{"j equals one enables FIFO", 1, true, 5, false, 1, true},
		{"invalid config falls back to default", 0, false, -1, false, 3, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limit, fifo := resolveDaemonConcurrency(tc.flagValue, tc.flagChanged, tc.configValue, tc.sequential)
			if limit != tc.wantLimit || fifo != tc.wantFIFO {
				t.Fatalf("resolveDaemonConcurrency() = (%d, %v), want (%d, %v)",
					limit, fifo, tc.wantLimit, tc.wantFIFO)
			}
		})
	}
}

func TestChunkSequentialFlagAccepted(t *testing.T) {
	tempDir := t.TempDir()
	content := []byte("chunk-sequential flag acceptance payload")
	origin := newStandaloneTestOrigin(t, content)

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		origin.URL + "/chunkseq.txt",
		"--dir", tempDir,
		"--no-organize",
		"-j", "1",
		"--chunk-sequential",
		"--quiet",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("--chunk-sequential run failed: %v", err)
	}
}
