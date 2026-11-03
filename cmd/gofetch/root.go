package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gofetch [flags] [url]",
		Short: "GoFetch is a high-performance cross-platform download manager",
		Long: `GoFetch is a concurrent, resilient, chunked download manager CLI and daemon
built for extreme reliability, high throughput, and seamless pause/resume.`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "Fetching: %s\n", args[0])
			return err
		},
	}

	cmd.AddCommand(newDaemonCmd())
	cmd.AddCommand(newAddCmd())
	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newPauseCmd())
	cmd.AddCommand(newResumeCmd())
	return cmd
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
