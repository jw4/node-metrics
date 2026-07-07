package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jw4/node-metrics/internal/collectnats"
	"github.com/jw4/node-metrics/internal/promclient"
	"github.com/jw4/node-metrics/internal/registry"
)

// Poller runs one poll cycle: discover healthy targets on Job, run every
// registered metric's PromQL/Extract against each, and publish results.
// Ready reports true only once discovery has returned at least one host and
// the most recent poll of all discovered hosts succeeded -- a poll that
// fails, or one over zero discovered hosts, must never look ready.
type Poller struct {
	Prom    *promclient.Client
	Nats    *collectnats.Client
	Job     string
	Metrics []registry.Metric

	ready atomic.Bool
}

func (p *Poller) Ready() bool { return p.ready.Load() }

func (p *Poller) PollOnce(ctx context.Context) error {
	targets, err := p.Prom.Targets(ctx, p.Job)
	if err != nil {
		p.ready.Store(false)
		return err
	}
	if len(targets) == 0 {
		p.ready.Store(false)
		return nil
	}

	allOK := true
	for _, target := range targets {
		for _, metric := range p.Metrics {
			samples, err := p.Prom.InstantQuery(ctx, metric.PromQL(target.Raw))
			if err != nil {
				slog.Warn("poll: query failed", "metric", metric.Name, "host", target.Instance, "err", err)
				allOK = false
				continue
			}
			value, ok := metric.Extract(samples)
			if !ok {
				slog.Warn("poll: no matching samples", "metric", metric.Name, "host", target.Instance)
				continue
			}
			if err := p.Nats.Publish(ctx, metric.Name, target.Instance, value, nil); err != nil {
				slog.Warn("poll: publish failed", "metric", metric.Name, "host", target.Instance, "err", err)
				allOK = false
			}
		}
	}
	p.ready.Store(allOK)
	return nil
}

func main() {
	promURL := envOr("NODE_METRICS_PROM_URL", "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090")
	natsURL := envOr("NODE_METRICS_NATS_URL", "nats://nats.nats-prime.svc.cluster.local:4222")
	credsFile := os.Getenv("NODE_METRICS_NATS_CREDS")
	job := envOr("NODE_METRICS_JOB", "node-exporter-external")
	interval := envDurationOr("NODE_METRICS_INTERVAL", 60*time.Second)
	maxAge := envDurationOr("NODE_METRICS_MAX_AGE", 7*24*time.Hour)
	maxBytes := envInt64Or("NODE_METRICS_MAX_BYTES", 512<<20)
	kvTTL := envDurationOr("NODE_METRICS_KV_TTL", 5*time.Minute)
	healthzAddr := envOr("NODE_METRICS_HEALTHZ_ADDR", ":8080")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := collectnats.New(ctx, collectnats.Config{
		Address: natsURL, CredsFile: credsFile,
		MaxAge: maxAge, MaxBytes: maxBytes, KVTTL: kvTTL,
	})
	if err != nil {
		slog.Error("collector: nats setup failed", "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	p := &Poller{Prom: promclient.New(promURL), Nats: nc, Job: job, Metrics: registry.All}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if p.Ready() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv := &http.Server{Addr: healthzAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("collector: healthz server failed", "err", err)
		}
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := p.PollOnce(ctx); err != nil {
			slog.Warn("collector: poll cycle failed, will retry next tick", "err", err)
		}
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				slog.Warn("collector: healthz server shutdown failed", "err", err)
			}
			return
		case <-ticker.C:
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt64Or(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
