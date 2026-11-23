package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/spf13/cobra"
)

func newCancelCmd() *cobra.Command {
	var (
		daemonAddr  string
		deleteFiles bool
	)

	cmd := &cobra.Command{
		Use:          "cancel [id]",
		Aliases:      []string{"delete", "rm"},
		Short:        "Cancel and remove a download task from the daemon queue",
		Long:         "Cancels an active or queued download job, removes it from the task store, and optionally deletes temporary files.",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			client := &http.Client{Timeout: 5 * time.Second}

			endpoint := fmt.Sprintf("http://%s/api/v1/downloads/%s", daemonAddr, id)
			if deleteFiles {
				endpoint += "?delete_files=true"
			}

			req, err := http.NewRequest(http.MethodDelete, endpoint, nil) //nolint:gosec // local daemon IPC
			if err != nil {
				return fmt.Errorf("failed creating request: %w", err)
			}

			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("failed communicating with daemon at %s (is 'gofetch daemon' running?): %w", daemonAddr, err)
			}
			defer func() { _ = resp.Body.Close() }()

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

			if deleteFiles {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Deleted download [%s] and purged local files\n", actResp.ID)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Canceled download [%s]\n", actResp.ID)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&daemonAddr, "daemon-addr", api.DefaultAddr, "Address of local daemon")
	cmd.Flags().BoolVarP(&deleteFiles, "purge", "p", false, "Purge downloaded files and .part files from disk")
	return cmd
}
