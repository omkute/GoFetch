package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/omkute/gofetch/config"
	"github.com/omkute/gofetch/internal/api"
	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	var (
		chunks          int
		maxConcurrent   int
		destDir         string
		rateLimit       string
		sequential      bool
		chunkSequential bool
		checksum        string
		checksumAlgo    string
		noOrganize      bool
		quiet           bool
		headless        bool
		jsonOutput      bool
		inputFile       string
		configFile      string
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

			cfg, err := config.LoadConfig(configFile)
			if err != nil {
				return fmt.Errorf("configuration error: %w", err)
			}

			resolved := resolveStandaloneConfig(standaloneFlagState{
				destDir:           destDir,
				destDirChanged:    cmd.Flags().Changed("dir"),
				chunks:            chunks,
				chunksChanged:     cmd.Flags().Changed("chunks"),
				noOrganize:        noOrganize,
				noOrganizeChanged: cmd.Flags().Changed("no-organize"),
			}, cfg)

			opts := standaloneOptions{
				DestDir:         resolved.DestDir,
				Chunks:          resolved.Chunks,
				MaxConcurrent:   maxConcurrent,
				RateLimit:       rateLimit,
				Sequential:      sequential,
				ChunkSequential: chunkSequential,
				Checksum:        checksum,
				ChecksumAlgo:    checksumAlgo,
				NoOrganize:      resolved.NoOrganize,
				Quiet:           quiet,
				Headless:        headless,
				JSON:            jsonOutput,
				Out:             cmd.OutOrStdout(),
				Client:          newStandaloneHTTPClient(cfg.Network.ConnectionTimeout),
				CategoryPaths:   cfg.Storage.CategoryPaths,
				UserAgent:       cfg.Network.UserAgent,
			}

			return runStandalone(opts, urls)
		},
	}

	cmd.Flags().IntVarP(&chunks, "chunks", "c", 4, "Number of concurrent chunk download workers")
	cmd.Flags().IntVarP(&maxConcurrent, "max-concurrent", "j", 3, "Maximum concurrent downloads (1 = strict queue order)")
	cmd.Flags().StringVarP(&destDir, "dir", "d", ".", "Target download directory")
	cmd.Flags().StringVarP(&rateLimit, "rate-limit", "l", "", "Bandwidth limit (e.g. 500KB, 2MB, 10M)")
	cmd.Flags().BoolVar(&sequential, "sequential", false, "Execute downloads strictly in queue order (implies -j 1)")
	cmd.Flags().BoolVar(&chunkSequential, "chunk-sequential", false, "Fetch chunks of each file in head-to-tail order")
	cmd.Flags().StringVar(&checksum, "checksum", "", "Expected file hash (e.g. SHA-256 or MD5 hex digest)")
	cmd.Flags().StringVar(&checksumAlgo, "checksum-algo", "sha256", "Checksum algorithm (sha256 or md5)")
	cmd.Flags().BoolVar(&noOrganize, "no-organize", false, "Disable automatic category directory sorting")
	cmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Quiet mode: suppress progress output")
	cmd.Flags().BoolVar(&headless, "headless", false, "Headless mode: emit plain text progress lines instead of interactive TUI")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output machine-readable JSON progress events")
	cmd.Flags().StringVarP(&inputFile, "input-file", "f", "", "Path to file containing download URLs (one per line)")
	cmd.Flags().StringVar(&configFile, "config", "", "Path to custom configuration file (default: ~/.gofetch/config.yaml)")

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

// standaloneFlagState captures the flags that configuration can supply a
// default for, together with whether the user set each one explicitly.
type standaloneFlagState struct {
	destDir           string
	destDirChanged    bool
	chunks            int
	chunksChanged     bool
	noOrganize        bool
	noOrganizeChanged bool
}

// standaloneResolved holds the effective values used by a standalone download.
type standaloneResolved struct {
	DestDir    string
	Chunks     int
	NoOrganize bool
}

// resolveStandaloneConfig applies flag > config > built-in default precedence.
// Missing or non-positive config values fall back to the standalone defaults.
func resolveStandaloneConfig(flags standaloneFlagState, cfg *config.Config) standaloneResolved {
	resolved := standaloneResolved{
		DestDir:    ".",
		Chunks:     config.DefaultChunksPerDownload,
		NoOrganize: false,
	}
	if cfg != nil {
		if cfg.Storage.DefaultDownloadDir != "" {
			resolved.DestDir = cfg.Storage.DefaultDownloadDir
		}
		if cfg.Concurrency.ChunksPerDownload > 0 {
			resolved.Chunks = cfg.Concurrency.ChunksPerDownload
		}
		resolved.NoOrganize = !cfg.Storage.AutoClassify
	}
	if flags.destDirChanged {
		resolved.DestDir = flags.destDir
	}
	if flags.chunksChanged {
		resolved.Chunks = flags.chunks
	}
	if flags.noOrganizeChanged {
		resolved.NoOrganize = flags.noOrganize
	}
	if resolved.DestDir == "" {
		resolved.DestDir = "."
	}
	if resolved.Chunks <= 0 {
		resolved.Chunks = config.DefaultChunksPerDownload
	}
	return resolved
}

// newStandaloneHTTPClient bounds only the TCP dial phase when the configuration
// supplies a connection timeout. Client.Timeout stays at zero so large bodies
// are never truncated by an overall request deadline.
func newStandaloneHTTPClient(connectionTimeoutSec int) *http.Client {
	if connectionTimeoutSec > 0 {
		return api.NewStreamingClientWithDialTimeout(time.Duration(connectionTimeoutSec) * time.Second)
	}
	return &http.Client{Transport: http.DefaultTransport}
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
