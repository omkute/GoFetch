package ui_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/omkute/gofetch/internal/ui"
)

func TestHeadlessPrinter_Format(t *testing.T) {
	const total = int64(10 * 1024 * 1024)
	tracker := ui.NewProgressTracker(total, 4)
	tracker.AddChunkBytes(0, 2*1024*1024)
	tracker.AddChunkBytes(1, 2*1024*1024)

	buf := new(bytes.Buffer)
	printer := ui.NewHeadlessPrinter(tracker, buf)
	printer.PrintProgress("ubuntu-24.04.iso")

	output := buf.String()
	if !strings.Contains(output, "ubuntu-24.04.iso") {
		t.Errorf("expected filename in headless output, got: %s", output)
	}
	if !strings.Contains(output, "40.0%") {
		t.Errorf("expected 40.0%% in headless output, got: %s", output)
	}
	if !strings.Contains(output, "4.00 MB / 10.00 MB") {
		t.Errorf("expected byte count in headless output, got: %s", output)
	}
}

func TestModel_View(t *testing.T) {
	tracker := ui.NewProgressTracker(1024*1024, 2)
	model := ui.NewModel(tracker, "sample_archive.zip")

	view := model.View()
	if !strings.Contains(view, "sample_archive.zip") {
		t.Errorf("expected view to contain filename, got:\n%s", view)
	}
	if !strings.Contains(view, "GoFetch") {
		t.Errorf("expected view to contain header banner, got:\n%s", view)
	}
	if !strings.Contains(view, "0.0%") {
		t.Errorf("expected view to show initial progress 0.0%%, got:\n%s", view)
	}
}

func TestModel_View_ChunkStates(t *testing.T) {
	const total = int64(1024)
	const numChunks = 4
	tracker := ui.NewProgressTracker(total, numChunks)
	// Each chunk holds 256 bytes: chunk 1 is done, chunk 2 is in flight,
	// chunks 3 and 4 have not started.
	tracker.AddChunkBytes(0, 256)
	tracker.AddChunkBytes(1, 100)

	view := ui.NewModel(tracker, "states.bin").View()

	tests := []struct {
		index int
		want  string
	}{
		{1, "complete"},
		{2, "active"},
		{3, "pending"},
		{4, "pending"},
	}
	for _, tc := range tests {
		line := chunkLine(t, view, tc.index)
		if !strings.Contains(line, tc.want) {
			t.Errorf("chunk %d: expected %q state in line %q", tc.index, tc.want, line)
		}
	}
}

func chunkLine(t *testing.T, view string, index int) string {
	t.Helper()
	prefix := fmt.Sprintf("[%d]", index)
	for _, line := range strings.Split(view, "\n") {
		if strings.Contains(line, prefix) {
			return line
		}
	}
	t.Fatalf("no row for chunk %d in view:\n%s", index, view)
	return ""
}
