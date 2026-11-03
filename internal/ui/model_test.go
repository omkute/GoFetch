package ui_test

import (
	"bytes"
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
