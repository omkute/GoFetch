package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"text/tabwriter"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/omkute/gofetch/internal/ui"
	"github.com/spf13/cobra"
)

func newListCmd() *cobra.Command {
	var (
		statusFilter string
		daemonAddr   string
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List active, queued, and completed downloads managed by the GoFetch daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := &http.Client{Timeout: 5 * time.Second}

			endpoint := fmt.Sprintf("http://%s/api/v1/downloads", daemonAddr)
			if statusFilter != "" {
				endpoint += "?status=" + url.QueryEscape(statusFilter)
			}

			resp, err := client.Get(endpoint) //nolint:gosec // local daemon IPC
			if err != nil {
				return fmt.Errorf("failed communicating with daemon at %s: %w", daemonAddr, err)
			}
			defer func() {
				_ = resp.Body.Close()
			}()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("daemon returned status %s", resp.Status)
			}

			var listResp api.ListDownloadsResponse
			if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
				return fmt.Errorf("failed decoding response: %w", err)
			}

			if listResp.Total == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No downloads found.")
				return nil
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(w, "ID\tFILENAME\tSTATUS\tPRIORITY\tPROGRESS\tSPEED\tETA")
			for _, item := range listResp.Items {
				pctStr := fmt.Sprintf("%.1f%%", item.ProgressPercent)
				speedStr := "-"
				if item.SpeedBps > 0 {
					speedStr = ui.FormatBytes(item.SpeedBps) + "/s"
				}
				etaStr := "-"
				if item.ETASeconds > 0 {
					etaStr = ui.FormatDuration(time.Duration(item.ETASeconds) * time.Second)
				}
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					item.ID, item.FileName, item.Status, item.Priority, pctStr, speedStr, etaStr)
			}
			return w.Flush()
		},
	}

	cmd.Flags().StringVarP(&statusFilter, "status", "s", "", "Filter downloads by status (queued, downloading, paused, completed, failed)")
	cmd.Flags().StringVar(&daemonAddr, "daemon-addr", api.DefaultAddr, "Address of local daemon")

	return cmd
}
