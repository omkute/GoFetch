package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/spf13/cobra"
)

func newAddCmd() *cobra.Command {
	var (
		priority   string
		chunks     int
		destDir    string
		daemonAddr string
	)

	cmd := &cobra.Command{
		Use:          "add [url]",
		Short:        "Enqueue a new download task via the GoFetch daemon",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			rawURL := args[0]
			payload := api.CreateDownloadRequest{
				URL:            rawURL,
				DestinationDir: destDir,
				Priority:       priority,
				MaxChunks:      chunks,
			}

			body, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("failed encoding request: %w", err)
			}

			client := &http.Client{Timeout: 5 * time.Second}
			apiURL := fmt.Sprintf("http://%s/api/v1/downloads", daemonAddr)
			resp, err := client.Post(apiURL, "application/json", bytes.NewReader(body)) //nolint:gosec // local daemon IPC
			if err != nil {
				return fmt.Errorf("failed communicating with daemon at %s (is 'gofetch daemon' running?): %w", daemonAddr, err)
			}
			defer func() {
				_ = resp.Body.Close()
			}()

			if resp.StatusCode != http.StatusAccepted {
				return fmt.Errorf("daemon returned unexpected status: %s", resp.Status)
			}

			var dlResp api.DownloadResponse
			if err := json.NewDecoder(resp.Body).Decode(&dlResp); err != nil {
				return fmt.Errorf("failed parsing daemon response: %w", err)
			}

			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Enqueued download [%s]: %s (priority: %s)\n",
				dlResp.ID, dlResp.FileName, dlResp.Priority)
			return nil
		},
	}

	cmd.Flags().StringVarP(&priority, "priority", "p", "normal", "Scheduling priority (low, normal, high)")
	cmd.Flags().IntVarP(&chunks, "chunks", "c", 4, "Number of concurrent chunk download workers")
	cmd.Flags().StringVarP(&destDir, "dir", "d", ".", "Target download directory")
	cmd.Flags().StringVar(&daemonAddr, "daemon-addr", api.DefaultAddr, "Address of local daemon")

	return cmd
}
