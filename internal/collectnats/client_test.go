package collectnats

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/jw4/node-metrics/internal/testutil"
)

func TestNew_CreatesStreamAndKV(t *testing.T) {
	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()

	c, err := New(ctx, Config{
		Address:  s.ClientURL(),
		MaxAge:   7 * 24 * time.Hour,
		MaxBytes: 512 << 20,
		KVTTL:    5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	si, err := c.js.Stream(ctx, StreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cfg := si.CachedInfo().Config
	if cfg.MaxBytes != 512<<20 {
		t.Errorf("MaxBytes = %d, want %d", cfg.MaxBytes, 512<<20)
	}

	kvStatus, err := c.js.KeyValue(ctx, KVBucket)
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	status, err := kvStatus.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.TTL() != 5*time.Minute {
		t.Errorf("TTL = %v, want 5m", status.TTL())
	}
}

func TestNew_IdempotentAgainstPopulatedResources(t *testing.T) {
	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()

	cfg := Config{Address: s.ClientURL(), MaxAge: 7 * 24 * time.Hour, MaxBytes: 512 << 20, KVTTL: 5 * time.Minute}

	c1, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	if err := c1.Publish(ctx, "cpu_temp", "belfalas", 71.0, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	c1.Close()

	// Simulate a collector restart against a stream/KV that already has data.
	c2, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("second New against populated resources: %v", err)
	}
	defer c2.Close()

	stream, err := c2.js.Stream(ctx, StreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	msg, err := stream.GetLastMsgForSubject(ctx, "metrics.node.cpu_temp.belfalas")
	if err != nil {
		t.Fatalf("expected prior message to survive restart: %v", err)
	}
	if msg == nil {
		t.Fatal("expected a message, got nil")
	}
}

func TestPublish_WritesStreamAndKVAndRepublishes(t *testing.T) {
	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()

	c, err := New(ctx, Config{Address: s.ClientURL(), MaxAge: time.Hour, MaxBytes: 1 << 20, KVTTL: 5 * time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	nc2, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	defer nc2.Close()
	sub, err := nc2.SubscribeSync(LiveSubjectPrefix + ">")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	if err := c.Publish(ctx, "cpu_temp", "belfalas", 71.5, map[string]string{"chip": "platform_coretemp_0"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected a republished live message: %v", err)
	}
	if msg.Subject != "live.node-metrics.cpu_temp.belfalas" {
		t.Errorf("republished subject = %q, want %q", msg.Subject, "live.node-metrics.cpu_temp.belfalas")
	}

	kv, err := c.js.KeyValue(ctx, KVBucket)
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	entry, err := kv.Get(ctx, "cpu_temp.belfalas")
	if err != nil {
		t.Fatalf("kv.Get: %v", err)
	}
	var p Payload
	if err := json.Unmarshal(entry.Value(), &p); err != nil {
		t.Fatalf("unmarshal KV entry: %v", err)
	}
	if p.Value != 71.5 {
		t.Errorf("KV payload value = %v, want 71.5", p.Value)
	}

	stream, err := c.js.Stream(ctx, StreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	streamMsg, err := stream.GetLastMsgForSubject(ctx, "metrics.node.cpu_temp.belfalas")
	if err != nil {
		t.Fatalf("expected message on NODE_METRICS stream: %v", err)
	}
	var sp Payload
	if err := json.Unmarshal(streamMsg.Data, &sp); err != nil {
		t.Fatalf("unmarshal stream message: %v", err)
	}
	if sp.Value != 71.5 {
		t.Errorf("stream payload value = %v, want 71.5", sp.Value)
	}
}
