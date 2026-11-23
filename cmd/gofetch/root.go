package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	var (
		chunks       int
		destDir      string
		rateLimit    string
		sequential   bool
		checksum     string
		checksumAlgo string
		noOrganize   bool
		quiet        bool
		headless     bool
		jsonOutput   bool
		inputFile    string
	)

	cmd := &cobra.Command{
		Use:   "gofetch [flags] [url]",
		Short: "GoFetch is a high-performance cross-platform download manager",
		Long: `GoFetch is a concurrent, resilient, chunked download manager CLI and daemon
built for extreme reliability, high throughput, and seamless pause/resume.`,
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
				return cmd.Help()
			}

			opts := standaloneOptions{
				DestDir:      destDir,
				Chunks:       chunks,
				RateLimit:    rateLimit,
				Sequential:   sequential,
				Checksum:     checksum,
				ChecksumAlgo: checksumAlgo,
				NoOrganize:   noOrganize,
				Quiet:        quiet,
				Headless:     headless,
				JSON:         jsonOutput,
				Out:          cmd.OutOrStdout(),
				Client:       &http.Client{Transport: http.DefaultTransport},
			}

			return runStandalone(opts, urls)
		},
	}

	cmd.Flags().IntVarP(&chunks, "chunks", "c", 4, "Number of concurrent chunk download workers")
	cmd.Flags().StringVarP(&destDir, "dir", "d", ".", "Target download directory")
	cmd.Flags().StringVarP(&rateLimit, "rate-limit", "l", "", "Bandwidth limit (e.g. 500KB, 2MB, 10M)")
	cmd.Flags().BoolVar(&sequential, "sequential", false, "Strict sequential chunk execution (FIFO)")
	cmd.Flags().StringVar(&checksum, "checksum", "", "Expected file hash (e.g. SHA-256 or MD5 hex digest)")
	cmd.Flags().StringVar(&checksumAlgo, "checksum-algo", "sha256", "Checksum algorithm (sha256 or md5)")
	cmd.Flags().BoolVar(&noOrganize, "no-organize", false, "Disable automatic category directory sorting")
	cmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Quiet mode: suppress progress output")
	cmd.Flags().BoolVar(&headless, "headless", false, "Headless mode: emit plain text progress lines instead of interactive TUI")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output machine-readable JSON progress events")
	cmd.Flags().StringVarP(&inputFile, "input-file", "f", "", "Path to file containing download URLs (one per line)")

	cmd.AddCommand(newDaemonCmd())
	cmd.AddCommand(newGUICmd())
	cmd.AddCommand(newAddCmd())
	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newPauseCmd())
	cmd.AddCommand(newResumeCmd())
	cmd.AddCommand(newCancelCmd())
	cmd.AddCommand(newStatsCmd())
	return cmd
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
