// Package promclient is a minimal Prometheus HTTP API client: target
// discovery (for dynamic host discovery, filtered to healthy targets on one
// job) and instant queries (for polling a metric's current value).
package promclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/jw4/node-metrics/internal/registry"
	"github.com/jw4/node-metrics/internal/subject"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{}}
}

// Target is one healthy scrape target: Raw is the untouched Prometheus
// instance label; Instance is its sanitized single-token form, safe to use in
// a NATS subject or KV key.
type Target struct {
	Raw      string
	Instance string
}

type targetsResponse struct {
	Data struct {
		ActiveTargets []struct {
			Labels map[string]string `json:"labels"`
			Health string            `json:"health"`
		} `json:"activeTargets"`
	} `json:"data"`
}

// Targets returns the sanitized, healthy ("health"=="up") targets for the
// given job.
func (c *Client) Targets(ctx context.Context, job string) ([]Target, error) {
	body, err := c.get(ctx, "/api/v1/targets", nil)
	if err != nil {
		return nil, fmt.Errorf("promclient: targets: %w", err)
	}
	var resp targetsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("promclient: decode targets: %w", err)
	}
	var out []Target
	for _, t := range resp.Data.ActiveTargets {
		if t.Labels["job"] != job || t.Health != "up" {
			continue
		}
		raw := t.Labels["instance"]
		out = append(out, Target{Raw: raw, Instance: subject.Sanitize(raw)})
	}
	return out, nil
}

type queryResponse struct {
	Data struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// InstantQuery runs a PromQL instant query and returns each result series as
// a registry.Sample, keyed by its "chip" label.
func (c *Client) InstantQuery(ctx context.Context, promql string) ([]registry.Sample, error) {
	body, err := c.get(ctx, "/api/v1/query", url.Values{"query": {promql}})
	if err != nil {
		return nil, fmt.Errorf("promclient: query: %w", err)
	}
	var resp queryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("promclient: decode query: %w", err)
	}
	samples := make([]registry.Sample, 0, len(resp.Data.Result))
	for _, r := range resp.Data.Result {
		valStr, _ := r.Value[1].(string)
		v, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		samples = append(samples, registry.Sample{ChipName: r.Metric["chip"], Value: v})
	}
	return samples, nil
}

func (c *Client) get(ctx context.Context, path string, query url.Values) ([]byte, error) {
	u := c.baseURL + path
	if query != nil {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("promclient: unexpected status %d from %s", resp.StatusCode, path)
	}
	return body, nil
}
