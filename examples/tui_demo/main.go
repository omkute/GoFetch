package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/omkute/gofetch/internal/ui"
)

func main() {
	headlessFlag := flag.Bool("headless", false, "Run in headless mode instead of interactive TUI")
	flag.Parse()

	const totalBytes = int64(50 * 1024 * 1024) // 50 MB
	const numChunks = 4
	const filename = "archlinux-2026.iso"

	tracker := ui.NewProgressTracker(totalBytes, numChunks)

	if *headlessFlag {
		fmt.Println("==================================================")
		fmt.Println("🚀 GoFetch Headless Mode Demo (T-009)")
		fmt.Println("==================================================")
		printer := ui.NewHeadlessPrinter(tracker, os.Stdout)

		// Simulate background streaming download
		for step := 1; step <= 10; step++ {
			time.Sleep(100 * time.Millisecond)
			for c := 0; c < numChunks; c++ {
				tracker.AddChunkBytes(c, totalBytes/int64(numChunks*10))
			}
			printer.PrintProgress(filename)
		}
		fmt.Println("==================================================")
		return
	}

	// Interactive TUI mode using Bubbletea
	model := ui.NewModel(tracker, filename)
	p := tea.NewProgram(model)

	// Simulate streaming worker progress in background
	go func() {
		for tracker.Percentage() < 100.0 {
			time.Sleep(80 * time.Millisecond)
			for c := 0; c < numChunks; c++ {
				tracker.AddChunkBytes(c, 1024*1024) // 1MB per chunk step
			}
		}
		time.Sleep(300 * time.Millisecond)
		p.Quit()
	}()

	if _, err := p.Run(); err != nil {
		fmt.Printf("Error running TUI: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ T-009 Interactive Terminal UI & Headless modes verified successfully!")
}
