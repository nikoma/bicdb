# BicDB Replication Security

Network replication must be encrypted and authenticated.

## Required Defaults

- TLS is mandatory for non-localhost replication.
- mTLS is required for node-to-node replication.
- Plaintext replication is only accepted for explicit localhost development
  mode.
- Private keys, shared secrets, raw frame payloads, and sensitive record bytes
  must not be logged.
- SQL diagnostics expose only safe status, lag, node, and error metadata.

## Configuration Shape

```toml
[replication]
enabled = true
mode = "primary" # primary | standby | disabled
listen_addr = "10.0.0.10:9443"
advertise_addr = "db-a.example.internal:9443"
cluster_id = "prod-cluster-01"
node_id = "db-a"
allowed_node_ids = ["db-b"]
compression = true
max_frame_bytes = 134217728
heartbeat_interval_ms = 5000
connect_timeout_ms = 10000
reconnect_backoff_ms = 1000
retention_bytes = 1073741824
retention_commits = 1000000

[replication.tls]
cert_path = "/etc/bicdb/replication/node.crt"
key_path = "/etc/bicdb/replication/node.key"
ca_path = "/etc/bicdb/replication/ca.crt"
require_client_cert = true
```

Local development may use explicit localhost plaintext only:

```toml
[replication]
enabled = true
mode = "standby"
listen_addr = "127.0.0.1:9443"

[replication.tls]
dev_localhost_plaintext = true
require_client_cert = true
```

This mode must never bind to a non-localhost address.

## Certificate Rules

- Certificates must chain to the configured CA. Clients validate the primary
  hostname/SAN through `--server-name`.
- Followers declare their replication `node_id` in the protocol `Hello`; the
  primary rejects nodes that are not listed by `--allowed-node-id` when that
  allow-list is configured.
- Expired certificates must be rejected.
- Unknown client certificates must be rejected.
- CA trust should be explicit and narrow.
- Rotation should overlap old/new certs briefly, then remove old node identity
  trust.

## Operational Notes

- Audit connect, disconnect, authentication failure, replay failure, and
  promotion events.
- Redact payloads in logs. Log commit sequence and frame type, not full frame
  contents.
- Rate-limit failed handshakes.
- Treat checksum mismatch, cluster mismatch, and schema incompatibility as
  fatal replication errors requiring operator action.
