// Package tuinats is the TUI's JetStream client. It never talks to
// Prometheus: host discovery comes from the node-metrics-latest KV bucket's
// keys, backfill comes from an ephemeral ordered consumer on NODE_METRICS,
// and live updates come from a plain core-NATS subscription fed by the KV's
// RePublish (not kv.Watch() and not a long-lived JetStream consumer).
package tuinats

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jw4/node-metrics/internal/collectnats"
)

type Config struct {
	Address   string
	CredsFile string
}

type Point struct {
	Value     float64
	Timestamp time.Time
}

type Client struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	opts := []nats.Option{
		nats.Name("node-metrics-tui"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}
	if cfg.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredsFile))
	}
	nc, err := nats.Connect(cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("tuinats: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("tuinats: jetstream: %w", err)
	}
	return &Client{nc: nc, js: js}, nil
}

func (c *Client) Close() { c.nc.Drain() }

// Hosts returns the sanitized host tokens currently reporting the given
// metric, derived from the KV's key directory -- no Prometheus dependency.
func (c *Client) Hosts(ctx context.Context, metric string) ([]string, error) {
	kv, err := c.js.KeyValue(ctx, collectnats.KVBucket)
	if err != nil {
		return nil, fmt.Errorf("tuinats: kv: %w", err)
	}
	lister, err := kv.ListKeysFiltered(ctx, metric+".*")
	if err != nil {
		return nil, fmt.Errorf("tuinats: list keys: %w", err)
	}
	prefix := metric + "."
	var hosts []string
	for key := range lister.Keys() {
		hosts = append(hosts, strings.TrimPrefix(key, prefix))
	}
	sort.Strings(hosts)
	return hosts, nil
}

// StreamHost subscribes live *before* backfilling, so a point published
// during the backfill window is buffered, not lost -- core NATS (the live
// path) has no replay, so ordering the naive way (backfill, then subscribe)
// leaves a gap nothing can recover. Points are deduplicated by payload
// timestamp before being emitted on the returned channel. Call the returned
// cancel func to tear down both the live subscription and the backfill
// consumer.
func (c *Client) StreamHost(ctx context.Context, metric, host string, window time.Duration) (<-chan Point, func(), error) {
	liveSubj := collectnats.LiveSubjectPrefix + metric + "." + host
	filterSubj := "metrics.node." + metric + "." + host

	liveBuf := make(chan *nats.Msg, 256)
	sub, err := c.nc.ChanSubscribe(liveSubj, liveBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("tuinats: subscribe live: %w", err)
	}

	out := make(chan Point, 256)
	stop := make(chan struct{})
	var stopped bool
	cancel := func() {
		if stopped {
			return
		}
		stopped = true
		close(stop)
		sub.Unsubscribe()
	}

	go func() {
		defer close(out)
		seen := map[int64]struct{}{}
		emit := func(data []byte) {
			var p collectnats.Payload
			if err := json.Unmarshal(data, &p); err != nil {
				return
			}
			key := p.Timestamp.UnixNano()
			if _, ok := seen[key]; ok {
				return
			}
			seen[key] = struct{}{}
			select {
			case out <- Point{Value: p.Value, Timestamp: p.Timestamp}:
			case <-stop:
			}
		}

		start := time.Now().Add(-window)
		cons, err := c.js.OrderedConsumer(ctx, collectnats.StreamName, jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{filterSubj},
			DeliverPolicy:  jetstream.DeliverByStartTimePolicy,
			OptStartTime:   &start,
		})
		if err == nil {
			if msgs, mErr := cons.Messages(); mErr == nil {
				for {
					msg, nErr := msgs.Next()
					if nErr != nil {
						break
					}
					emit(msg.Data())
					meta, mdErr := msg.Metadata()
					msg.Ack()
					if mdErr == nil && meta.NumPending == 0 {
						break
					}
				}
				msgs.Stop()
			}
		}

		for {
			select {
			case m, ok := <-liveBuf:
				if !ok {
					return
				}
				emit(m.Data)
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, cancel, nil
}
