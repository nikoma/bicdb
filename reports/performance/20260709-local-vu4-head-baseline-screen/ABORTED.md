# Aborted local 4-VU baseline screen

The first discarded `default-durable` warmup was stopped by the configured host
memory guard when the 4 GB auto-checkpoint began.

- Server RSS before checkpoint pressure: approximately 13.8 GB
- Server RSS at guard: approximately 16.6 GB
- WAL: approximately 4.24 GB
- Host `MemAvailable`: 7,936 MB
- Guard: 8,192 MB
- Result: invalid infrastructure run; no NOPM recorded

The local repeated screen was moved to two VUs, which does not cross the
checkpoint threshold in a three-minute trial. Local checkpoint cost is therefore
reported as unexercised. The canonical 32-VU checkpoint gate remains blocked on
`benchmark-primary` SSH access.
