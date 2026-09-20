# Aborted local 16-VU baseline screen

The first discarded `default-durable` warmup was stopped by the configured host
memory guard before measured trials began.

- Server RSS: approximately 17.4 GB
- WAL: approximately 4.53 GB
- Host `MemAvailable`: 7,730 MB
- Guard: 8,192 MB
- Result: invalid infrastructure run; no NOPM recorded

The guard was not lowered. The local repeated screen was moved to four VUs. The
canonical 32-VU gate remains blocked on `benchmark-primary` SSH access.
