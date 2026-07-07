// Package collectnats is the collector's JetStream client: it owns creating
// the NODE_METRICS stream and node-metrics-latest KV bucket, and publishing
// readings to both.
package collectnats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamName        = "NODE_METRICS"
	KVBucket          = "node-metrics-latest"
	LiveSubjectPrefix = "live.node-metrics."
)

// Payload is the JSON shape written to both the stream and the KV bucket.
// internal/tuinats decodes this same shape.
type Payload struct {
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

type Config struct {
	Address   string
	CredsFile string
	MaxAge    time.Duration
	MaxBytes  int64
	KVTTL     time.Duration
}

type Client struct {
	nc *nats.Conn
	js jetstream.JetStream
	kv jetstream.KeyValue
}

// New connects to NATS and idempotently ensures the stream and KV bucket
// exist with this config. The KV's RePublish is fixed at first creation --
// see the plan's Global Constraints for why it cannot be changed later.
func New(ctx context.Context, cfg Config) (*Client, error) {
	opts := []nats.Option{
		nats.Name("node-metrics-collector"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("nats: disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			slog.Info("nats: reconnected")
		}),
	}
	if cfg.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredsFile))
	}

	nc, err := nats.Connect(cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("collectnats: connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("collectnats: jetstream: %w", err)
	}

	c := &Client{nc: nc, js: js}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      StreamName,
		Subjects:  []string{"metrics.node.>"},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    cfg.MaxAge,
		MaxBytes:  cfg.MaxBytes,
		Storage:   jetstream.FileStorage,
	}); err != nil {
		nc.Close()
		return nil, fmt.Errorf("collectnats: ensure stream: %w", err)
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  KVBucket,
		TTL:     cfg.KVTTL,
		Storage: jetstream.FileStorage,
		RePublish: &jetstream.RePublish{
			Source:      fmt.Sprintf("$KV.%s.>", KVBucket),
			Destination: LiveSubjectPrefix + ">",
		},
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("collectnats: ensure kv: %w", err)
	}
	c.kv = kv

	return c, nil
}

// Publish writes one reading to the history stream and the latest-value KV
// (the KV Put triggers the RePublish fan-out to the live subject
// automatically -- this method never publishes to the live subject itself).
func (c *Client) Publish(ctx context.Context, metric, host string, value float64, labels map[string]string) error {
	payload := Payload{Value: value, Labels: labels, Timestamp: time.Now().UTC()}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("collectnats: marshal: %w", err)
	}

	subj := fmt.Sprintf("metrics.node.%s.%s", metric, host)
	if _, err := c.js.Publish(ctx, subj, data); err != nil {
		return fmt.Errorf("collectnats: publish stream: %w", err)
	}

	key := metric + "." + host
	if _, err := c.kv.Put(ctx, key, data); err != nil {
		return fmt.Errorf("collectnats: kv put: %w", err)
	}
	return nil
}

func (c *Client) Close() {
	c.nc.Drain()
}
