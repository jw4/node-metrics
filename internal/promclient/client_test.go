package promclient

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTargets_HealthyOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{
			"status": "success",
			"data": {
				"activeTargets": [
					{"labels": {"job": "node-exporter-external", "instance": "belfalas.w.jw4.us:9100"}, "health": "up"},
					{"labels": {"job": "node-exporter-external", "instance": "flaky.w.jw4.us:9100"}, "health": "down"},
					{"labels": {"job": "coredns-external", "instance": "eomer.w.jw4.us:9153"}, "health": "up"}
				],
				"droppedTargets": []
			}
		}`))
	}))
	defer srv.Close()

	c := New(srv.URL)
	targets, err := c.Targets(t.Context(), "node-exporter-external")
	if err != nil {
		t.Fatalf("Targets: %v", err)
	}
	if len(targets) != 1 {
		t.Fatalf("got %d targets, want 1 (unhealthy and wrong-job targets must be filtered): %+v", len(targets), targets)
	}
	if targets[0].Raw != "belfalas.w.jw4.us:9100" || targets[0].Instance != "belfalas-w-jw4-us" {
		t.Fatalf("unexpected target: %+v", targets[0])
	}
}

func TestTargets_ZeroTargets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","data":{"activeTargets":[],"droppedTargets":[]}}`))
	}))
	defer srv.Close()

	c := New(srv.URL)
	targets, err := c.Targets(t.Context(), "node-exporter-external")
	if err != nil {
		t.Fatalf("Targets: %v", err)
	}
	if len(targets) != 0 {
		t.Fatalf("got %d targets, want 0", len(targets))
	}
}

func TestTargets_ChangesBetweenPolls(t *testing.T) {
	var call int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		if call == 1 {
			w.Write([]byte(`{"status":"success","data":{"activeTargets":[
				{"labels":{"job":"node-exporter-external","instance":"r710.w.jw4.us:9100"},"health":"up"}
			],"droppedTargets":[]}}`))
			return
		}
		w.Write([]byte(`{"status":"success","data":{"activeTargets":[
			{"labels":{"job":"node-exporter-external","instance":"r710.w.jw4.us:9100"},"health":"up"},
			{"labels":{"job":"node-exporter-external","instance":"belfalas.w.jw4.us:9100"},"health":"up"}
		],"droppedTargets":[]}}`))
	}))
	defer srv.Close()

	c := New(srv.URL)
	first, err := c.Targets(t.Context(), "node-exporter-external")
	if err != nil || len(first) != 1 {
		t.Fatalf("first poll: %v, %+v", err, first)
	}
	second, err := c.Targets(t.Context(), "node-exporter-external")
	if err != nil || len(second) != 2 {
		t.Fatalf("second poll (host added mid-run): %v, %+v", err, second)
	}
}

func TestTargets_ColdStartUnreachable(t *testing.T) {
	c := New("http://127.0.0.1:1") // nothing listening -- connection refused
	_, err := c.Targets(t.Context(), "node-exporter-external")
	if err == nil {
		t.Fatal("expected error when Prometheus is unreachable, got nil")
	}
}

func TestTargets_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer srv.Close()

	c := New(srv.URL)
	targets, err := c.Targets(t.Context(), "node-exporter-external")
	if err == nil {
		t.Fatalf("expected error for non-2xx status, got nil (targets: %+v)", targets)
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected error to mention status code 500, got: %v", err)
	}
}

func TestInstantQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "query=") {
			t.Errorf("expected query param in request, got %s", r.URL.RawQuery)
		}
		w.Write([]byte(`{
			"status": "success",
			"data": {
				"resultType": "vector",
				"result": [
					{"metric": {"chip": "platform_coretemp_0", "sensor": "temp1"}, "value": [1720000000, "71"]},
					{"metric": {"chip": "platform_applesmc_768", "sensor": "temp1"}, "value": [1720000000, "24.25"]}
				]
			}
		}`))
	}))
	defer srv.Close()

	c := New(srv.URL)
	samples, err := c.InstantQuery(t.Context(), `node_hwmon_temp_celsius{instance="belfalas-w-jw4-us:9100"}`)
	if err != nil {
		t.Fatalf("InstantQuery: %v", err)
	}
	if len(samples) != 2 {
		t.Fatalf("got %d samples, want 2", len(samples))
	}
	if samples[0].ChipName != "platform_coretemp_0" || samples[0].Value != 71 {
		t.Fatalf("unexpected sample[0]: %+v", samples[0])
	}
	if samples[1].ChipName != "platform_applesmc_768" || samples[1].Value != 24.25 {
		t.Fatalf("unexpected sample[1]: %+v", samples[1])
	}
}
