package main

import (
	"flag"
	"fmt"
	"os"

	"qrok/internal/server"
	"qrok/pkg/fault"
)

func main() {
	configPath := flag.String("config", "", "путь к YAML-конфигу (по умолчанию $QROK_CONFIG, затем qrok.yaml)")
	flag.Parse()

	if err := server.Run(*configPath); err != nil {
		fmt.Fprintln(os.Stderr, fault.RenderCLI(err))
		os.Exit(1)
	}
}
