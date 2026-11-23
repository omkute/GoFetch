package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/omkute/gofetch/internal/ui"
	"github.com/spf13/cobra"
)

func newStatsCmd() *cobra.Command {
	var daemonAddr string

	cmd := &cobra.Command{
		Use:          "stats",
		Aliases:      []string{"status", "telemetry"},
		Short:        "Display runtime metrics and queue telemetry from the GoFetch daemon",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := &http.Client{Timeout: 5 * time.Second}

			endpoint := fmt.Sprintf("http://%s/api/v1/system/stats", daemonAddr)
			resp, err := client.Get(endpoint) //nolint:gosec // local daemon IPC
			if err != nil {
				return fmt.Errorf("failed communicating with daemon at %s (is 'gofetch daemon' running?): %w", daemonAddr, err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("daemon returned status %s", resp.Status)
			}

			var stats api.SystemStatsResponse
			if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
				return fmt.Errorf("failed decoding telemetry: %w", err)
			}

			speedStr := "0 B/s"
			if stats.TotalSpeedBps > 0 {
				speedStr = ui.FormatBytes(stats.TotalSpeedBps) + "/s"
			}

			rateLimitStr := "Unlimited"
			if stats.GlobalRateLimitBps > 0 {
				rateLimitStr = ui.FormatBytes(stats.GlobalRateLimitBps) + "/s"
			}

			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "GoFetch Daemon Telemetry (%s)\n", daemonAddr)
			_, _ = fmt.Fprintf(out, "--------------------------------------------------\n")
			_, _ = fmt.Fprintf(out, "  Active Downloads:    %d\n", stats.ActiveDownloads)
			_, _ = fmt.Fprintf(out, "  Queued Downloads:    %d\n", stats.QueuedDownloads)
			_, _ = fmt.Fprintf(out, "  Paused Downloads:    %d\n", stats.PausedDownloads)
			_, _ = fmt.Fprintf(out, "  Completed Downloads: %d\n", stats.CompletedDownloads)
			_, _ = fmt.Fprintf(out, "  Failed Downloads:    %d\n", stats.FailedDownloads)
			_, _ = fmt.Fprintf(out, "--------------------------------------------------\n")
			_, _ = fmt.Fprintf(out, "  Aggregate Throughput: %s\n", speedStr)
			_, _ = fmt.Fprintf(out, "  Global Rate Limit:   %s\n", rateLimitStr)
			_, _ = fmt.Fprintf(out, "  Memory Allocated:    %s\n", ui.FormatBytes(int64(stats.MemoryAllocatedBytes)))
			_, _ = fmt.Fprintf(out, "  Daemon Uptime:       %s\n", ui.FormatDuration(time.Duration(stats.UptimeSeconds)*time.Second))
			return nil
		},
	}

	cmd.Flags().StringVar(&daemonAddr, "daemon-addr", api.DefaultAddr, "Address of local daemon")
	return cmd
}
