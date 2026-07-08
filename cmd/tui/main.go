// cmd/tui/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/jw4/node-metrics/internal/tuiapp"
	"github.com/jw4/node-metrics/internal/tuinats"
)

func main() {
	natsURL := flag.String("nats-url", "nats://localhost:4222", "nats-prime external endpoint")
	credsFile := flag.String("creds", "", "path to the node-metrics-tui .creds file")
	caFile := flag.String("tls-ca", "", "path to the internal-ca root cert (required unless -nats-url is in-cluster)")
	metric := flag.String("metric", "cpu_temp", "metric to display")
	window := flag.Duration("window", time.Hour, "backfill window on start / host switch")
	logFile := flag.String("log-file", "", "optional path to log backfill/connection errors to (bubbletea takes over the terminal, so errors are never written to stderr while running -- set this to see them)")
	flag.Parse()

	if *logFile != "" {
		f, err := tea.LogToFile(*logFile, "node-metrics-tui ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: log-file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
	}

	if *credsFile == "" {
		fmt.Fprintln(os.Stderr, "error: -creds is required")
		os.Exit(1)
	}

	ctx := context.Background()
	client, err := tuinats.New(ctx, tuinats.Config{Address: *natsURL, CredsFile: *credsFile, RootCAFile: *caFile})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	hosts, err := client.Hosts(ctx, *metric)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: list hosts: %v\n", err)
		os.Exit(1)
	}
	if len(hosts) == 0 {
		fmt.Fprintln(os.Stderr, "error: no hosts currently reporting this metric")
		os.Exit(1)
	}

	model := tuiapp.New(ctx, client, *metric, hosts, *window)
	program := tea.NewProgram(model, tea.WithAltScreen())
	if _, err := program.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
