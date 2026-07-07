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
	metric := flag.String("metric", "cpu_temp", "metric to display")
	window := flag.Duration("window", time.Hour, "backfill window on start / host switch")
	flag.Parse()

	if *credsFile == "" {
		fmt.Fprintln(os.Stderr, "error: -creds is required")
		os.Exit(1)
	}

	ctx := context.Background()
	client, err := tuinats.New(ctx, tuinats.Config{Address: *natsURL, CredsFile: *credsFile})
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
	program := tea.NewProgram(model)
	if _, err := program.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
