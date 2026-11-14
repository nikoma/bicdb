# Memo511 handoff-observation failure diagnostics

This leaf is a test-only observation change based on frozen source
`2ec5f7545b5bb3706b2e229151a17c2569103bfe` (1.0.511-beta). It does not repair or
rerun the failed test, change acquisition budgets or retry a failed request.

## Preserved original result

Run `43580010702`, Core job `132402546409`, failed with **901 passed, 1 failed
and 8 ignored** in 161.52 seconds. A nested targeted child passed separately;
it is not additive to that outer result. The actual checkout was
`62d4f066af62945747ead2c0c9fa43fb10c30ae0`, tree
`87e34c5d45cf423914af4bdd500464279c0d6a15`.

The failed test was
`distribution_range_handoff::tests::remote::transport_observation_rejects_wrong_uuid_other_range_and_expiration`.
Its shared `Fixture::observation` acquisition unwrap at
`distribution_range_handoff.rs:1457` received:

```text
cluster node n1 rejected RPC cluster_data_operation_failed: cluster error: metadata read acquisition: fresh joint quorum unavailable within bounded probe attempts
```

The complete original Core log has 1,520,964 decompressed bytes and SHA-256
`643c6ad463fc90f4ac4d80989f9760ad89f6029d338cf79d292d6063a24f1b0e`.
The log does not retain the helper call ordinal, read-request UUID, range or
per-stage timings. Therefore which earlier assertions completed is unknown:

| Helper call | Original acquisition budget | Following intended control |
| --- | --- | --- |
| 1 | 1 second | Wrong business/request UUID |
| 2 | 1 second | Observation from the other range |
| 3 | 500 ms | Expiry before cut construction |
| 4 | 500 ms | Expiry before recording an ACK |
| 5 | 500 ms | Expiry before finishing the cut |

The failure precedes the intended control for its unknown call. The test name
alone does not establish that all five controls executed.

## Source and cause boundaries

The whole handoff source file is byte-identical in 511, delivered main509
`7ad192a7d01ba1e0c1967bc4f1bc79d176a4c385`, f69
`f69b242c9db3afe0d24e83520f94a853093d4b98` and E3
`e3ccc906ad961e6c83d29b07763fa67393731f3c`: SHA-256
`dc3f077f79c587f3955b29c357b19f03cfd3e20d4c829d3c946c24c8bb6b6cf8`.
The actual acquisition provider, consensus and transport code on this path is
unchanged; the provider file has a separate test-only deadline diagnostic added
since f69/E3.

The exact same named test passed in f69 run `43571509341`, Core job
`132376807658`, at `2026-11-14T07:43:00.9263850Z`, and E3 run `43571466889`,
Core job `132376600724`, at `2026-11-14T07:43:56.2201910Z`. These retained runs
have different complete source and execution contexts; they do not identify a
cause for the 511 failure.

The literal error is produced by `MetadataReadProvider::acquire_authority`
after no fresh quorum is ready and its candidate/active-probe sets are
exhausted. Probe transport errors and unaccepted ACKs can consume the bounded
attempts. Their individual outcomes are not preserved in this original report.
The error does not establish which peer failed, a specific deadline expiry,
slow fsync, TLS failure, scheduling delay or a memo defect. The later broken-pipe
messages also do not establish the cause of the earlier quorum refusal. The
fixture uses real local TCP/TLS and metadata stores; this is not a physical HA
or throughput experiment.

## Narrow diagnostic successor

The actual fixture owns a private in-memory call counter. Each original
acquisition still generates its real UUID, looks up the same range/epoch, uses
the original client and metadata destination, and receives the unchanged
1-second or 500-ms budget. It records helper-entry, call-entry and call-return
`Instant`s in memory. Only an actual API error formats and writes the ordinal,
actual UUID/range/epoch/participant, requested budget, elapsed times and exact
error before the original unwrap. Diagnostic stderr failure does not replace
the primary error. Successful calls produce no new diagnostic I/O.

These wall-clock observations do not expose the transport's internal deadline
or per-voter traces and cannot distinguish I/O from scheduling. All original
wrong-UUID, other-range, ACK and expiration operations/assertions remain. No
request is renewed or retried. Formatting and source-preservation checks are
static only; no build, test, CI, push, version bump or qualification was run for
this leaf. The original failed run remains failed.
