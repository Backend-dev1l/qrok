package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	agentpkg "qrok/internal/agent"
	"qrok/internal/config"
	"qrok/pkg/logger"
)

func newAgentCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Staging agent: reads broker, streams events to the cloud",
	}
	start := &cobra.Command{
		Use:   "start",
		Short: "Start the agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.LoadAgent(configPath)
			if err != nil {
				return err
			}

			log := logger.MustNew(os.Stdout, logger.Config{
				Level:  cfg.Log.Level,
				Format: cfg.Log.Format,
			})
			log.Info("starting agent",
				"tunnel_id", cfg.Agent.TunnelID,
				"gateway", cfg.Agent.Gateway,
				"topics", cfg.Agent.Topics,
			)

			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			if err := agentpkg.Run(ctx, cfg, log); err != nil {
				return err
			}
			fmt.Println("agent stopped")
			return nil
		},
	}
	start.Flags().StringVar(&configPath, "config", "", "path to agent YAML config")
	cmd.AddCommand(start)
	return cmd
}
