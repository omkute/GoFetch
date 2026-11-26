package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/omkute/gofetch/config"
	"github.com/omkute/gofetch/internal/api"
	"github.com/omkute/gofetch/internal/engine"
	"github.com/omkute/gofetch/internal/state"
	"github.com/spf13/cobra"
)

func newDaemonCmd() *cobra.Command {
	var (
		daemonAddr    string
		rateLimit     string
		configFile    string
		maxConcurrent int
		sequential    bool
	)

	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Start the GoFetch loopback HTTP daemon engine",
		Long:  "Runs the GoFetch engine as a persistent loopback service on 127.0.0.1:12701.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.LoadConfig(configFile)
			if err != nil {
				return fmt.Errorf("configuration error: %w", err)
			}

			addr := daemonAddr
			if !cmd.Flags().Changed("addr") && cfg.Daemon.ListenAddr != "" {
				addr = cfg.Daemon.ListenAddr
			}

			server := api.NewServer(addr)

			autoClassify := cfg.Storage.AutoClassify
			if err := server.SetDaemonConfig(api.DaemonConfig{
				DefaultMaxChunks:     cfg.Concurrency.ChunksPerDownload,
				DefaultDestDir:       cfg.Storage.DefaultDownloadDir,
				DefaultAutoClassify:  &autoClassify,
				CategoryPaths:        cfg.Storage.CategoryPaths,
				UserAgent:            cfg.Network.UserAgent,
				ConnectionTimeoutSec: cfg.Network.ConnectionTimeout,
				EnableWebGUI:         cfg.Daemon.EnableWebGUI,
				PostgresDSN:          cfg.Network.PostgresDSN,
			}); err != nil {
				return fmt.Errorf("configuration error: %w", err)
			}

			limit, fifo := resolveDaemonConcurrency(
				maxConcurrent,
				cmd.Flags().Changed("max-concurrent"),
				cfg.Concurrency.MaxConcurrentDownloads,
				sequential,
			)
			server.SetConcurrency(limit, fifo)

			taskDB := cfg.Daemon.TaskDBPath
			if taskDB != "" {
				if store, storeErr := state.NewTaskStore(taskDB); storeErr == nil {
					server.SetTaskStore(store)
				}
				// Resume checkpoints use a dedicated bbolt file so chunk offset
				// snapshots are isolated from task metadata persistence.
				if cpStore, cpErr := state.NewBboltStore(taskDB + ".checkpoints.db"); cpErr == nil {
					server.SetCheckpointStore(cpStore)
				}
			}

			// Apply rate limits
			if rateLimit != "" {
				if bps, parseErr := engine.ParseRateLimit(rateLimit); parseErr == nil && bps > 0 {
					server.SetGlobalRateLimit(bps)
					log.Printf("Global rate limit applied: %s (%d bytes/sec)", rateLimit, bps)
				}
			} else if cfg.Network.GlobalRateLimitBps > 0 {
				server.SetGlobalRateLimit(cfg.Network.GlobalRateLimitBps)
				log.Printf("Global rate limit from config applied: %d bytes/sec", cfg.Network.GlobalRateLimitBps)
			}

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

			errCh := make(chan error, 1)
			go func() {
				log.Printf("Starting GoFetch loopback daemon on %s (PID: %d)", addr, os.Getpid())
				if err := server.Start(); err != nil {
					errCh <- err
				}
			}()

			select {
			case sig := <-sigCh:
				log.Printf("Received signal %s, shutting down daemon...", sig)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return server.Shutdown(ctx)
			case err := <-errCh:
				return fmt.Errorf("daemon server error: %w", err)
			}
		},
	}

	cmd.Flags().StringVar(&daemonAddr, "addr", api.DefaultAddr, "Address and port for loopback daemon")
	cmd.Flags().StringVarP(&rateLimit, "rate-limit", "l", "", "Global bandwidth rate limit (e.g. 5MB, 10M)")
	cmd.Flags().StringVar(&configFile, "config", "", "Path to custom configuration file (default: ~/.gofetch/config.yaml)")
	cmd.Flags().IntVarP(&maxConcurrent, "max-concurrent", "j", 3, "Maximum concurrent downloads (1 = strict queue order)")
	cmd.Flags().BoolVar(&sequential, "sequential", false, "Execute downloads strictly in queue order (implies -j 1)")
	return cmd
}

// resolveDaemonConcurrency applies flag > config > default precedence and
// derives FIFO mode from --sequential or a limit of one.
func resolveDaemonConcurrency(flagValue int, flagChanged bool, configValue int, sequential bool) (int, bool) {
	limit := flagValue
	if !flagChanged && configValue > 0 {
		limit = configValue
	}
	if limit <= 0 {
		limit = 3
	}
	if sequential {
		limit = 1
	}
	return limit, sequential || limit == 1
}
