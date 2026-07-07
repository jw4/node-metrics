// Package subject sanitizes Prometheus instance labels into single NATS
// subject/KV-key tokens. A raw instance like "david.local:9100" would
// otherwise add extra "."-separated tokens to every subject it's used in,
// silently breaking single-"*" wildcard matches (KV ListKeysFiltered,
// RePublish's destination mapping, TUI consumer filters).
package subject

import "strings"

// Sanitize strips the ":<port>" suffix (if present) and replaces every
// remaining "." with "-", producing a single dot-free token.
func Sanitize(instance string) string {
	host := instance
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	return strings.ReplaceAll(host, ".", "-")
}
