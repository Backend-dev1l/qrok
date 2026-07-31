// qrok — единый CLI: агент на стейджинге (qrok agent) и dev-клиент (qrok listen).
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"qrok/pkg/fault"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, fault.RenderCLI(err))
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "qrok",
		Short: "Туннель и реплеер для асинхронных событий (Kafka/RabbitMQ -> localhost)",
	}

	root.AddCommand(
		newAgentCmd(),
		newListenCmd(),
		newLoginCmd(),
		newReplayCmd(),
		versionCmd(),
	)

	return root
}

func notImplemented(name, short string) *cobra.Command {
	return &cobra.Command{
		Use:   name,
		Short: short,
		RunE: func(cmd *cobra.Command, args []string) error {
			return fault.ErrUnprocessable.
				Newf("команда %q ещё не реализована", name).
				WithOp("cli." + name).
				WithHint("реализация — этап 1, см. docs/TZ.md")
		},
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Версия CLI",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("qrok dev (этап 1, cobra)")
		},
	}
}
