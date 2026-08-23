// qrok — unified CLI: staging agent (qrok agent) and dev client (qrok listen).
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
		Short: "Tunnel and replayer for async events (Kafka/RabbitMQ -> localhost)",
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

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "CLI version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("qrok dev (stage 1, cobra)")
		},
	}
}
