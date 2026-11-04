package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/omkute/gofetch/internal/api"
	"github.com/omkute/gofetch/internal/state"
	"github.com/spf13/cobra"
)

func newGUICmd() *cobra.Command {
	var (
		addr   string
		noOpen bool
	)

	cmd := &cobra.Command{
		Use:     "gui",
		Aliases: []string{"web", "dashboard"},
		Short:   "Open the GoFetch interactive Web GUI dashboard",
		Long:    "Launches the GoFetch desktop/web GUI and opens your default browser to 127.0.0.1:12701.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			guiURL := fmt.Sprintf("http://%s/", addr)
			healthURL := fmt.Sprintf("http://%s/api/v1/health", addr)

			// 1. Check if daemon is already running
			client := &http.Client{Timeout: 600 * time.Millisecond}
			resp, err := client.Get(healthURL)
			daemonRunning := err == nil && resp.StatusCode == http.StatusOK
			if resp != nil {
				_ = resp.Body.Close()
			}

			if daemonRunning {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "GoFetch daemon already running at %s\n", guiURL)
				if !noOpen {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Opening browser to %s\n", guiURL)
					_ = openBrowser(guiURL)
				}
				return nil
			}

			// 2. Start daemon in-process
			server := api.NewServer(addr)
			home, err := os.UserHomeDir()
			if err == nil {
				dbPath := filepath.Join(home, ".gofetch", "daemon_tasks.db")
				if store, storeErr := state.NewTaskStore(dbPath); storeErr == nil {
					server.SetTaskStore(store)
				}
			}

			errCh := make(chan error, 1)
			go func() {
				if startErr := server.Start(); startErr != nil {
					errCh <- startErr
				}
			}()

			select {
			case <-server.Ready():
			case err := <-errCh:
				return fmt.Errorf("failed starting GUI server: %w", err)
			case <-time.After(2 * time.Second):
			}

			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "GoFetch Web GUI running at %s\n", guiURL)
			if !noOpen {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Opening browser to %s\n", guiURL)
				_ = openBrowser(guiURL)
			}

			// Wait for interrupt signal
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

			select {
			case sig := <-sigCh:
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "\nShutting down GUI server (%s)...\n", sig)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return server.Shutdown(ctx)
			case err := <-errCh:
				return err
			}
		},
	}

	cmd.Flags().StringVar(&addr, "addr", api.DefaultAddr, "Address and port for GUI dashboard")
	cmd.Flags().BoolVar(&noOpen, "no-open", false, "Do not automatically launch default browser")
	return cmd
}

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", url).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	default:
		return exec.Command("xdg-open", url).Start()
	}
}
