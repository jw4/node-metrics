package tuinats

import (
	"context"
	"testing"
	"time"

	"github.com/jw4/node-metrics/internal/collectnats"
	"github.com/jw4/node-metrics/internal/testutil"
)

func TestHosts_ListsFromKVFilteredByMetric(t *testing.T) {
	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()

	collector, err := collectnats.New(ctx, collectnats.Config{Address: s.ClientURL(), MaxAge: time.Hour, MaxBytes: 1 << 20, KVTTL: 5 * time.Minute})
	if err != nil {
		t.Fatalf("collectnats.New: %v", err)
	}
	defer collector.Close()
	must(t, collector.Publish(ctx, "cpu_temp", "belfalas", 71, nil))
	must(t, collector.Publish(ctx, "cpu_temp", "r710", 55, nil))

	tc, err := New(ctx, Config{Address: s.ClientURL()})
	if err != nil {
		t.Fatalf("tuinats.New: %v", err)
	}
	defer tc.Close()

	hosts, err := tc.Hosts(ctx, "cpu_temp")
	if err != nil {
		t.Fatalf("Hosts: %v", err)
	}
	if len(hosts) != 2 {
		t.Fatalf("got %d hosts, want 2: %v", len(hosts), hosts)
	}
}

func TestStreamHost_BackfillThenLive(t *testing.T) {
	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()

	collector, err := collectnats.New(ctx, collectnats.Config{Address: s.ClientURL(), MaxAge: time.Hour, MaxBytes: 1 << 20, KVTTL: 5 * time.Minute})
	if err != nil {
		t.Fatalf("collectnats.New: %v", err)
	}
	defer collector.Close()
	must(t, collector.Publish(ctx, "cpu_temp", "belfalas", 65, nil))
	must(t, collector.Publish(ctx, "cpu_temp", "belfalas", 66, nil))

	tc, err := New(ctx, Config{Address: s.ClientURL()})
	if err != nil {
		t.Fatalf("tuinats.New: %v", err)
	}
	defer tc.Close()

	points, cancel, err := tc.StreamHost(ctx, "cpu_temp", "belfalas", time.Hour)
	if err != nil {
		t.Fatalf("StreamHost: %v", err)
	}
	defer cancel()

	seen := map[float64]bool{}
	timeout := time.After(3 * time.Second)
	for len(seen) < 2 {
		select {
		case p := <-points:
			seen[p.Value] = true
		case <-timeout:
			t.Fatalf("timed out waiting for backfilled points, got %v", seen)
		}
	}
	if !seen[65] || !seen[66] {
		t.Fatalf("expected backfilled values 65 and 66, got %v", seen)
	}

	// Now publish a live point and confirm it arrives exactly once (this is
	// the must-fix from adversarial review: a message published during the
	// backfill/live handoff must not be lost or duplicated).
	must(t, collector.Publish(ctx, "cpu_temp", "belfalas", 67, nil))
	select {
	case p := <-points:
		if p.Value != 67 {
			t.Fatalf("live point = %v, want 67", p.Value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for live point")
	}

	// No duplicate of 67 should follow.
	select {
	case p := <-points:
		t.Fatalf("unexpected extra point after the live 67: %+v", p)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestStreamHost_EmptyBackfillDoesNotHang covers a host whose filter subject
// has zero messages on NODE_METRICS at the moment StreamHost is called --
// e.g. a freshly-discovered host whose stream data hasn't landed yet, or a
// stream MaxAge shorter than the KV bucket's TTL so the KV directory still
// lists a host the stream has already trimmed. Before the fix, the backfill
// loop's msgs.Next() call (no NextOpts) could block indefinitely on an
// ordered consumer with nothing to deliver, and cancel() -- which only
// closes stop and unsubscribes the live NATS subscription -- had no way to
// interrupt it. This test bounds the wait so a regression fails fast instead
// of hanging the suite.
func TestStreamHost_EmptyBackfillDoesNotHang(t *testing.T) {
	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()

	collector, err := collectnats.New(ctx, collectnats.Config{Address: s.ClientURL(), MaxAge: time.Hour, MaxBytes: 1 << 20, KVTTL: 5 * time.Minute})
	if err != nil {
		t.Fatalf("collectnats.New: %v", err)
	}
	defer collector.Close()
	// Publish for a different host only, so NODE_METRICS has messages but
	// none matching "empty-host"'s filter subject.
	must(t, collector.Publish(ctx, "cpu_temp", "other-host", 42, nil))

	tc, err := New(ctx, Config{Address: s.ClientURL()})
	if err != nil {
		t.Fatalf("tuinats.New: %v", err)
	}
	defer tc.Close()

	points, cancel, err := tc.StreamHost(ctx, "cpu_temp", "empty-host", time.Hour)
	if err != nil {
		t.Fatalf("StreamHost: %v", err)
	}

	// "empty-host" was never published, so it has zero matching stream
	// messages. cancel() right away and confirm the out channel closes
	// promptly instead of the backfill goroutine sitting in msgs.Next().
	cancel()
	select {
	case p, ok := <-points:
		if ok {
			t.Fatalf("expected out channel to close with no points, got %+v", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("StreamHost did not shut down within 3s after cancel(); backfill loop likely hung in msgs.Next()")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestHosts_AgesOutAfterKVTTLElapsesWithoutRefresh(t *testing.T) {
	s := testutil.StartJetStreamServer(t)
	ctx := context.Background()

	// A short TTL (not the production 5m default) so the test observes
	// expiry in real time without a fake clock. The TTL and sleep margin
	// are wide relative to each other (3x) to absorb nats-server's
	// internal timer jitter on MaxAge/TTL expiry.
	collector, err := collectnats.New(ctx, collectnats.Config{Address: s.ClientURL(), MaxAge: time.Hour, MaxBytes: 1 << 20, KVTTL: 600 * time.Millisecond})
	if err != nil {
		t.Fatalf("collectnats.New: %v", err)
	}
	defer collector.Close()
	must(t, collector.Publish(ctx, "cpu_temp", "belfalas", 71, nil))

	tc, err := New(ctx, Config{Address: s.ClientURL()})
	if err != nil {
		t.Fatalf("tuinats.New: %v", err)
	}
	defer tc.Close()

	hosts, err := tc.Hosts(ctx, "cpu_temp")
	if err != nil || len(hosts) != 1 {
		t.Fatalf("expected 1 host before TTL elapses: %v, %v", hosts, err)
	}

	time.Sleep(1800 * time.Millisecond) // TTL elapsed, no refreshing Publish in between

	hosts, err = tc.Hosts(ctx, "cpu_temp")
	if err != nil {
		t.Fatalf("Hosts after TTL: %v", err)
	}
	if len(hosts) != 0 {
		t.Fatalf("expected the stale host to have aged out of the KV directory, got %v", hosts)
	}
}
