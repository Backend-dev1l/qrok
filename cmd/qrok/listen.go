package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"qrok/internal/config"
	"qrok/internal/devcli"
	"qrok/pkg/logger"
)

func newListenCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "listen",
		Short: "Receive events locally and forward to localhost",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.LoadListen(configPath)
			if err != nil {
				return err
			}

			log := logger.MustNew(os.Stdout, logger.Config{
				Level:  cfg.Log.Level,
				Format: cfg.Log.Format,
			})
			log.Info("starting dev client",
				"tunnel_id", cfg.Listen.TunnelID,
				"gateway", cfg.Listen.Gateway,
				"forward", cfg.Listen.Forward,
			)

			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			if err := devcli.Run(ctx, cfg, log); err != nil {
				return err
			}
			fmt.Println("dev client stopped")
			return nil
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "path to dev client YAML config")
	return cmd
}
