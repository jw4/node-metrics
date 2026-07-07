package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jw4/node-metrics/internal/collectnats"
	"github.com/jw4/node-metrics/internal/promclient"
	"github.com/jw4/node-metrics/internal/registry"
	"github.com/jw4/node-metrics/internal/testutil"
)

func TestPoller_ReadyOnlyAfterFirstFullPollOfAtLeastOneHost(t *testing.T) {
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case contains(r.URL.Path, "/targets"):
			w.Write([]byte(`{"status":"success","data":{"activeTargets":[
				{"labels":{"job":"node-exporter-external","instance":"belfalas.w.jw4.us:9100"},"health":"up"}
			],"droppedTargets":[]}}`))
		default:
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
				{"metric":{"chip":"platform_coretemp_0"},"value":[1720000000,"71"]}
			]}}`))
		}
	}))
	defer prom.Close()

	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()
	nats, err := collectnats.New(ctx, collectnats.Config{Address: s.ClientURL(), MaxAge: time.Hour, MaxBytes: 1 << 20, KVTTL: 5 * time.Minute})
	if err != nil {
		t.Fatalf("collectnats.New: %v", err)
	}
	defer nats.Close()

	p := &Poller{
		Prom:    promclient.New(prom.URL),
		Nats:    nats,
		Job:     "node-exporter-external",
		Metrics: registry.All,
	}

	if p.Ready() {
		t.Fatal("expected not-ready before any poll")
	}
	if err := p.PollOnce(ctx); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if !p.Ready() {
		t.Fatal("expected ready after a successful poll of >=1 host")
	}
}

func TestPoller_NotReadyOnZeroTargets(t *testing.T) {
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","data":{"activeTargets":[],"droppedTargets":[]}}`))
	}))
	defer prom.Close()

	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()
	nats, err := collectnats.New(ctx, collectnats.Config{Address: s.ClientURL(), MaxAge: time.Hour, MaxBytes: 1 << 20, KVTTL: 5 * time.Minute})
	if err != nil {
		t.Fatalf("collectnats.New: %v", err)
	}
	defer nats.Close()

	p := &Poller{Prom: promclient.New(prom.URL), Nats: nats, Job: "node-exporter-external", Metrics: registry.All}
	if err := p.PollOnce(ctx); err != nil {
		t.Fatalf("PollOnce with zero targets should not itself error: %v", err)
	}
	if p.Ready() {
		t.Fatal("expected not-ready when discovery returns zero targets -- must not be vacuously ready")
	}
}

func TestPoller_ColdStartUnreachableStaysNotReady(t *testing.T) {
	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()
	nats, err := collectnats.New(ctx, collectnats.Config{Address: s.ClientURL(), MaxAge: time.Hour, MaxBytes: 1 << 20, KVTTL: 5 * time.Minute})
	if err != nil {
		t.Fatalf("collectnats.New: %v", err)
	}
	defer nats.Close()

	p := &Poller{Prom: promclient.New("http://127.0.0.1:1"), Nats: nats, Job: "node-exporter-external", Metrics: registry.All}
	if err := p.PollOnce(ctx); err == nil {
		t.Fatal("expected PollOnce to surface the Prometheus-unreachable error")
	}
	if p.Ready() {
		t.Fatal("expected not-ready when Prometheus is unreachable at cold start")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(s) > len(sub) && stringsContains(s, sub)))
}

func stringsContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
