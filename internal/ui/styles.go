package ui

import (
	"github.com/charmbracelet/lipgloss"
)

var (
	// BannerStyle styles the GoFetch title banner.
	BannerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#7D56F4")).
			Padding(0, 1)

	// FilenameStyle styles the primary active target file name.
	FilenameStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#00D7D7"))

	// StatsStyle styles transfer throughput, ETA, and byte counters.
	StatsStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#8A8A8A"))

	// ProgressFilledStyle styles filled segments of progress bars.
	ProgressFilledStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00FFA3"))

	// ProgressEmptyStyle styles empty segments of progress bars.
	ProgressEmptyStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#3A3A3A"))

	// ChunkActiveStyle styles active chunk bars.
	ChunkActiveStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFD700"))
)
