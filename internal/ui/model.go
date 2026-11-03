package ui

import (
	"fmt"
	"io"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// TickMsg triggers UI repaint intervals.
type TickMsg time.Time

// Model implements the tea.Model interface for interactive Bubbletea rendering.
type Model struct {
	tracker  *ProgressTracker
	filename string
	quitting bool
}

// NewModel creates a Bubbletea UI model for a given download tracker.
func NewModel(tracker *ProgressTracker, filename string) Model {
	return Model{
		tracker:  tracker,
		filename: filename,
	}
}

// Init schedules the initial tick command.
func (m Model) Init() tea.Cmd {
	return tickCmd()
}

func tickCmd() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(t time.Time) tea.Msg {
		return TickMsg(t)
	})
}

// Update processes Bubbletea messages.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			m.quitting = true
			return m, tea.Quit
		}
	case TickMsg:
		if m.tracker.Percentage() >= 100.0 {
			return m, nil
		}
		return m, tickCmd()
	}

	return m, nil
}

// View renders the visual TUI layout.
func (m Model) View() string {
	if m.quitting {
		return "Download stopped.\n"
	}

	pct := m.tracker.Percentage()
	bar := RenderProgressBar(pct, 30)
	downloaded := FormatBytes(m.TotalDownloaded())
	total := FormatBytes(m.tracker.TotalBytes())
	speed := FormatBytes(int64(m.tracker.SpeedBytesPerSec()))
	eta := FormatDuration(m.tracker.ETA())

	var b strings.Builder
	b.WriteString("\n")
	b.WriteString(BannerStyle.Render("⚡ GoFetch Download Engine"))
	b.WriteString("\n")
	fmt.Fprintf(&b, "   Target: %s\n", FilenameStyle.Render(m.filename))
	fmt.Fprintf(&b, "   Progress: [%s] %.1f%%\n", ProgressFilledStyle.Render(bar), pct)
	b.WriteString(StatsStyle.Render(fmt.Sprintf("   Transferred: %s / %s | Speed: %s/s | ETA: %s",
		downloaded, total, speed, eta)))
	b.WriteString("\n")

	// Render miniature per-chunk progress bars if multi-chunk partitioned
	if m.tracker.NumChunks() > 1 {
		b.WriteString("\n   Chunks:\n")
		for i := 0; i < m.tracker.NumChunks(); i++ {
			chunkBytes := m.tracker.ChunkBytes(i)
			fmt.Fprintf(&b, "     [%d] %-12s %s\n",
				i+1,
				FormatBytes(chunkBytes),
				ChunkActiveStyle.Render("▶ active"))
		}
	}
	b.WriteString("\n")

	return b.String()
}

// TotalDownloaded returns total bytes transferred.
func (m Model) TotalDownloaded() int64 {
	return m.tracker.TotalDownloaded()
}

// HeadlessPrinter emits simple periodic text progress lines without ANSI escapes.
type HeadlessPrinter struct {
	tracker *ProgressTracker
	out     io.Writer
}

// NewHeadlessPrinter creates a printer for headless/CI/pipe executions.
func NewHeadlessPrinter(tracker *ProgressTracker, out io.Writer) *HeadlessPrinter {
	return &HeadlessPrinter{
		tracker: tracker,
		out:     out,
	}
}

// PrintProgress formats and writes a single progress summary line.
func (p *HeadlessPrinter) PrintProgress(filename string) {
	pct := p.tracker.Percentage()
	downloaded := FormatBytes(p.tracker.TotalDownloaded())
	total := FormatBytes(p.tracker.TotalBytes())
	speed := FormatBytes(int64(p.tracker.SpeedBytesPerSec()))
	eta := FormatDuration(p.tracker.ETA())

	_, _ = fmt.Fprintf(p.out, "[%5.1f%%] %s | %s / %s | %s/s | ETA: %s\n",
		pct, filename, downloaded, total, speed, eta)
}
