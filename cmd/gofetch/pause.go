package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/spf13/cobra"
)

func newPauseCmd() *cobra.Command {
	var daemonAddr string

	cmd := &cobra.Command{
		Use:          "pause [id]",
		Short:        "Pause an ongoing or queued download task",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			client := &http.Client{Timeout: 5 * time.Second}

			endpoint := fmt.Sprintf("http://%s/api/v1/downloads/%s/pause", daemonAddr, id)
			resp, err := client.Post(endpoint, "application/json", nil) //nolint:gosec // local daemon IPC
			if err != nil {
				return fmt.Errorf("failed communicating with daemon at %s (is 'gofetch daemon' running?): %w", daemonAddr, err)
			}
			defer func() {
				_ = resp.Body.Close()
			}()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("download with ID %q not found", id)
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("daemon returned status %s", resp.Status)
			}

			var actResp api.ActionResponse
			if err := json.NewDecoder(resp.Body).Decode(&actResp); err != nil {
				return fmt.Errorf("failed decoding response: %w", err)
			}

			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Paused download [%s]\n", actResp.ID)
			return nil
		},
	}

	cmd.Flags().StringVar(&daemonAddr, "daemon-addr", api.DefaultAddr, "Address of local daemon")

	return cmd
}
