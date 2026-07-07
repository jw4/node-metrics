// Package testutil provides a real, in-process JetStream-enabled NATS server
// for integration tests -- no external nats-server binary or Docker needed.
package testutil

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// StartJetStreamServer starts an in-process NATS server with JetStream
// enabled, bound to an ephemeral localhost port, and registers t.Cleanup to
// shut it down. Returns the server; use s.ClientURL() to connect.
//
// Optional optFns can mutate the server.Options before the server starts --
// e.g. to configure Users/Permissions for tests that need to force a real,
// non-"not found" JetStream API error (such as a permissions violation)
// against a real embedded server instead of mocking the client.
func StartJetStreamServer(t *testing.T, optFns ...func(*server.Options)) *server.Server {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	for _, fn := range optFns {
		fn(opts)
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("testutil: new server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(4 * time.Second) {
		t.Fatal("testutil: server not ready for connections")
	}
	t.Cleanup(s.Shutdown)
	return s
}
