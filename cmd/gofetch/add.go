package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/spf13/cobra"
)

func newAddCmd() *cobra.Command {
	var (
		priority     string
		chunks       int
		destDir      string
		daemonAddr   string
		rateLimit    string
		inputFile    string
		checksum     string
		checksumAlgo string
		noOrganize   bool
	)

	cmd := &cobra.Command{
		Use:          "add [url]",
		Short:        "Enqueue a new download task or batch file via the GoFetch daemon",
		Args:         cobra.ArbitraryArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			var urls []string

			if inputFile != "" {
				data, err := os.ReadFile(inputFile)
				if err != nil {
					return fmt.Errorf("failed reading input file %q: %w", inputFile, err)
				}
				lines := strings.Split(string(data), "\n")
				for _, l := range lines {
					trimmed := strings.TrimSpace(l)
					if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
						urls = append(urls, trimmed)
					}
				}
				if len(urls) == 0 {
					return fmt.Errorf("no valid URLs found in input file %q", inputFile)
				}
			} else if len(args) > 0 {
				urls = []string{args[0]}
			} else {
				return fmt.Errorf("please provide a URL argument or specify --input-file")
			}

			client := &http.Client{Timeout: 10 * time.Second}

			if len(urls) == 1 {
				payload := api.CreateDownloadRequest{
					URL:            urls[0],
					DestinationDir: destDir,
					Priority:       priority,
					MaxChunks:      chunks,
					RateLimit:      rateLimit,
					Checksum:       checksum,
					ChecksumAlgo:   checksumAlgo,
					NoOrganize:     noOrganize,
				}

				body, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("failed encoding request: %w", err)
				}

				apiURL := fmt.Sprintf("http://%s/api/v1/downloads", daemonAddr)
				resp, err := client.Post(apiURL, "application/json", bytes.NewReader(body)) //nolint:gosec
				if err != nil {
					return fmt.Errorf("failed communicating with daemon at %s (is 'gofetch daemon' running?): %w", daemonAddr, err)
				}
				defer func() { _ = resp.Body.Close() }()

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
			}

			// Batch enqueue
			batchPayload := api.BatchDownloadRequest{
				URLs:           urls,
				DestinationDir: destDir,
				Priority:       priority,
				MaxChunks:      chunks,
				RateLimit:      rateLimit,
				NoOrganize:     noOrganize,
			}

			body, err := json.Marshal(batchPayload)
			if err != nil {
				return fmt.Errorf("failed encoding batch request: %w", err)
			}

			apiURL := fmt.Sprintf("http://%s/api/v1/downloads/batch", daemonAddr)
			resp, err := client.Post(apiURL, "application/json", bytes.NewReader(body)) //nolint:gosec
			if err != nil {
				return fmt.Errorf("failed communicating with daemon at %s (is 'gofetch daemon' running?): %w", daemonAddr, err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusAccepted {
				return fmt.Errorf("daemon returned unexpected status: %s", resp.Status)
			}

			var batchResp api.BatchDownloadResponse
			if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
				return fmt.Errorf("failed parsing daemon batch response: %w", err)
			}

			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Enqueued batch of %d downloads (priority: %s)\n",
				batchResp.Enqueued, priority)
			return nil
		},
	}

	cmd.Flags().StringVarP(&priority, "priority", "p", "normal", "Scheduling priority (low, normal, high)")
	cmd.Flags().IntVarP(&chunks, "chunks", "c", 4, "Number of concurrent chunk download workers")
	cmd.Flags().StringVarP(&destDir, "dir", "d", ".", "Target download directory")
	cmd.Flags().StringVarP(&rateLimit, "rate-limit", "l", "", "Bandwidth limit (e.g. 500KB, 2MB, 10M)")
	cmd.Flags().StringVar(&checksum, "checksum", "", "Expected file hash (e.g. SHA-256 or MD5 hex digest)")
	cmd.Flags().StringVar(&checksumAlgo, "checksum-algo", "sha256", "Checksum algorithm (sha256 or md5)")
	cmd.Flags().BoolVar(&noOrganize, "no-organize", false, "Disable automatic category directory sorting")
	cmd.Flags().StringVarP(&inputFile, "input-file", "f", "", "Path to file containing download URLs (one per line)")
	cmd.Flags().StringVar(&daemonAddr, "daemon-addr", api.DefaultAddr, "Address of local daemon")

	return cmd
}
