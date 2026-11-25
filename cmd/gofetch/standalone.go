package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/omkute/gofetch/internal/classifier"
	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/state"
	"github.com/omkute/gofetch/internal/ui"
)

type standaloneOptions struct {
	URL          string
	DestDir      string
	Chunks       int
	RateLimit    string
	Sequential   bool
	Checksum     string
	ChecksumAlgo string
	NoOrganize   bool
	Quiet        bool
	Headless     bool
	JSON         bool
	Out          io.Writer
	Client       *http.Client
}

func isTerminal(f *os.File) bool {
	stat, err := f.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

func runStandaloneDownload(ctx context.Context, opts standaloneOptions) error {
	out := opts.Out
	if out == nil {
		out = os.Stdout
	}

	destDir := opts.DestDir
	if destDir == "" {
		destDir = "."
	}

	chunks := opts.Chunks
	if chunks <= 0 {
		chunks = 4
	}

	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: 0}
	}

	// 1. Probe remote resource
	probeCtx, probeCancel := context.WithTimeout(ctx, 30*time.Second)
	probe, err := engine.Probe(probeCtx, client, opts.URL)
	probeCancel()
	if err != nil {
		return fmt.Errorf("probe failed for %s: %w", opts.URL, err)
	}

	if !opts.Quiet && !opts.JSON {
		_, _ = fmt.Fprintf(out, "Probed: %s (Size: %s, Range: %v)\n",
			probe.Filename, ui.FormatBytes(probe.ContentLength), probe.AcceptRanges)
	}

	tracker := ui.NewProgressTracker(probe.ContentLength, chunks)

	var limiter *engine.RateLimiter
	if opts.RateLimit != "" {
		if bps, parseErr := engine.ParseRateLimit(opts.RateLimit); parseErr == nil && bps > 0 {
			limiter = engine.NewRateLimiter(bps)
		}
	}

	jobSum := sha256.Sum256([]byte(destDir + "\x00" + opts.URL))
	jobID := hex.EncodeToString(jobSum[:])[:32]

	dlOpts := engine.DownloadOptions{
		DestDir:      destDir,
		JobID:        jobID,
		MaxWorkers:   chunks,
		Sequential:   opts.Sequential,
		RateLimiter:  limiter,
		Checksum:     opts.Checksum,
		ChecksumAlgo: opts.ChecksumAlgo,
		ProgressFn: func(chunkIndex int, bytesRead int) {
			tracker.AddChunkBytes(chunkIndex, int64(bytesRead))
		},
	}

	coord := engine.NewCoordinator(client)
	if cpStore, ok := newStandaloneCheckpointStore(destDir); ok {
		defer func() { _ = cpStore.Close() }()
		coord = engine.NewCoordinatorWithStore(client, cpStore)
	}

	// Determine UI mode: interactive TUI vs headless vs JSON vs quiet
	useTUI := isTerminal(os.Stdout) && !opts.Quiet && !opts.Headless && !opts.JSON

	var finalDownloadedPath string
	var downloadErr error

	if useTUI {
		model := ui.NewModel(tracker, probe.Filename)
		p := tea.NewProgram(model)

		downloadCtx, downloadCancel := context.WithCancel(ctx)
		defer downloadCancel()

		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			path, err := coord.DownloadWithOptions(downloadCtx, probe, dlOpts)
			if err != nil {
				downloadErr = err
			} else {
				finalDownloadedPath = path
			}
			time.Sleep(150 * time.Millisecond) // Let TUI display 100%
			p.Quit()
		}()

		if _, runErr := p.Run(); runErr != nil && !errors.Is(runErr, tea.ErrProgramKilled) {
			downloadCancel()
			return runErr
		}

		downloadCancel()
		<-doneCh
	} else if opts.JSON {
		// JSON streaming progress
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		jsonCtx, jsonCancel := context.WithCancel(ctx)
		defer jsonCancel()

		go func() {
			for {
				select {
				case <-ticker.C:
					progressObj := map[string]interface{}{
						"file":         probe.Filename,
						"total_bytes":  tracker.TotalBytes(),
						"downloaded":   tracker.TotalDownloaded(),
						"progress_pct": tracker.Percentage(),
						"speed_bps":    int64(tracker.SpeedBytesPerSec()),
						"eta_seconds":  int64(tracker.ETA().Seconds()),
						"status":       "downloading",
					}
					data, _ := json.Marshal(progressObj)
					_, _ = fmt.Fprintln(out, string(data))
				case <-jsonCtx.Done():
					return
				}
			}
		}()

		finalDownloadedPath, downloadErr = coord.DownloadWithOptions(ctx, probe, dlOpts)
		jsonCancel()

		if downloadErr == nil {
			finalObj := map[string]interface{}{
				"file":         probe.Filename,
				"total_bytes":  tracker.TotalBytes(),
				"downloaded":   tracker.TotalDownloaded(),
				"progress_pct": 100.0,
				"status":       "completed",
			}
			data, _ := json.Marshal(finalObj)
			_, _ = fmt.Fprintln(out, string(data))
		}
	} else if opts.Quiet {
		// Silent execution
		finalDownloadedPath, downloadErr = coord.DownloadWithOptions(ctx, probe, dlOpts)
	} else {
		// Headless formatted text lines
		printer := ui.NewHeadlessPrinter(tracker, out)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		hlCtx, hlCancel := context.WithCancel(ctx)
		defer hlCancel()

		go func() {
			for {
				select {
				case <-ticker.C:
					printer.PrintProgress(probe.Filename)
				case <-hlCtx.Done():
					return
				}
			}
		}()

		finalDownloadedPath, downloadErr = coord.DownloadWithOptions(ctx, probe, dlOpts)
		hlCancel()
		if downloadErr == nil {
			printer.PrintProgress(probe.Filename)
		}
	}

	if downloadErr != nil {
		return fmt.Errorf("download error: %w", downloadErr)
	}

	// 2. Checksum was verified by the coordinator on the staged .part file
	// before the atomic commit; reaching here means it matched.
	if opts.Checksum != "" && !opts.Quiet && !opts.JSON {
		_, _ = fmt.Fprintln(out, "Checksum verified successfully.")
	}

	// 3. Media Classification & Organization
	finalDestination := finalDownloadedPath
	if !opts.NoOrganize {
		router := classifier.NewRouter(destDir)
		if classifiedPath, routeErr := router.Route(finalDownloadedPath); routeErr == nil {
			finalDestination = classifiedPath
		}
	}

	if !opts.Quiet && !opts.JSON {
		_, _ = fmt.Fprintf(out, "Finished: %s\nSaved to: %s\n", probe.Filename, finalDestination)
	}

	return nil
}

func runStandalone(opts standaloneOptions, urls []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	for i, u := range urls {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(urls) > 1 && !opts.Quiet && !opts.JSON {
			_, _ = fmt.Fprintf(opts.Out, "\n[%d/%d] Fetching %s\n", i+1, len(urls), u)
		}

		itemOpts := opts
		itemOpts.URL = u
		if err := runStandaloneDownload(ctx, itemOpts); err != nil {
			return err
		}
	}

	return nil
}

// newStandaloneCheckpointStore opens the user-level checkpoint database used
// by standalone downloads. Failure to open it is non-fatal: the download still
// runs, just without automatic resume.
func newStandaloneCheckpointStore(destDir string) (engine.CheckpointStore, bool) {
	base, err := os.UserCacheDir()
	if err != nil || base == "" {
		base = destDir
	}
	dir := filepath.Join(base, "gofetch")
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, false
	}
	store, err := state.NewBboltStore(filepath.Join(dir, "checkpoints.db"))
	if err != nil {
		return nil, false
	}
	return store, true
}
