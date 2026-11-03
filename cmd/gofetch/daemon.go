package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/spf13/cobra"
)

func newDaemonCmd() *cobra.Command {
	var daemonAddr string

	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Start the GoFetch loopback HTTP daemon engine",
		Long:  "Runs the GoFetch engine as a persistent loopback service on 127.0.0.1:12701.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			server := api.NewServer(daemonAddr)

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

			errCh := make(chan error, 1)
			go func() {
				log.Printf("Starting GoFetch loopback daemon on %s (PID: %d)", daemonAddr, os.Getpid())
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
	return cmd
}
