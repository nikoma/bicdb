# Aborted local 32-VU baseline screen

The first discarded `default-durable` warmup was stopped by the configured host
memory guard before any measured trial completed.

- Server RSS: approximately 17.6 GB
- WAL: approximately 4.48 GB
- Host `MemAvailable`: 7,821 MB
- Guard: 8,192 MB
- Result: invalid infrastructure run; no NOPM recorded

The guard was not lowered. This host will use a separate 16-VU screening
campaign. The canonical 32-VU gate remains blocked on `benchmark-primary` SSH access.
