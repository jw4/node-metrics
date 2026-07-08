# node-metrics

A small collector + TUI pair for watching Prometheus host metrics as a live
terminal graph, over NATS JetStream.

```
Prometheus  --poll-->  collector  --publish-->  NATS JetStream  <--read-only--  tui
```

- **collector** polls a Prometheus instance for each host in a scrape job,
  and publishes readings to a JetStream stream (history) and a KV bucket
  (latest value + host directory + live republish).
- **tui** never talks to Prometheus. It lists currently-reporting hosts from
  the KV, backfills a window of history from the stream, then switches to a
  live subscription. Hosts and metrics can be switched at runtime.

Adding a host requires no code change, just a Prometheus scrape config
change. Adding a metric is one new entry in `internal/registry`.

## Install

```
go install github.com/jw4/node-metrics/cmd/collector@latest
go install github.com/jw4/node-metrics/cmd/tui@latest
```

Or run the collector as a container: `ghcr.io/jw4/node-metrics-collector`.

## Collector

Configured entirely by environment variables:

| Variable                    | Default                                                                     |
| --------------------------- | --------------------------------------------------------------------------- |
| `NODE_METRICS_PROM_URL`     | `http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090` |
| `NODE_METRICS_NATS_URL`     | `nats://nats.nats-prime.svc.cluster.local:4222`                             |
| `NODE_METRICS_NATS_CREDS`   | (required) path to a `.creds` file                                          |
| `NODE_METRICS_NATS_TLS_CA`  | (optional) path to a root CA cert                                           |
| `NODE_METRICS_JOB`          | `node-exporter-external`                                                    |
| `NODE_METRICS_INTERVAL`     | `60s`                                                                       |
| `NODE_METRICS_MAX_AGE`      | `168h` (7d), JetStream stream retention                                    |
| `NODE_METRICS_MAX_BYTES`    | `512MiB`, JetStream stream size limit                                      |
| `NODE_METRICS_KV_TTL`       | `5m`                                                                        |
| `NODE_METRICS_HEALTHZ_ADDR` | `:8080`, serves `/healthz` and `/readyz`                                   |

## TUI

```
node-metrics-tui -creds tui.creds -nats-url tls://nats.example.com:4222 -tls-ca ca.crt
```

| Flag        | Default                 | Meaning                                                     |
| ----------- | ----------------------- | ----------------------------------------------------------- |
| `-nats-url` | `nats://localhost:4222` | NATS endpoint                                               |
| `-creds`    | (required)              | path to a `.creds` file                                     |
| `-tls-ca`   | (empty)                 | root CA cert; required unless connecting in-cluster         |
| `-metric`   | `cpu_temp`              | metric to display                                           |
| `-window`   | `1h`                    | backfill window on start / host switch                      |
| `-log-file` | (empty)                 | log connection/backfill errors here (bubbletea owns stderr) |

Keys: tab/arrows switch hosts.

## Requirements

A NATS server with JetStream enabled, and a stream + KV bucket the
collector creates on first run. Both binaries connect with NKey/JWT
credentials (`.creds`), set up an account and users however you manage
NATS auth.

## License

MIT, see [LICENSE](LICENSE).
