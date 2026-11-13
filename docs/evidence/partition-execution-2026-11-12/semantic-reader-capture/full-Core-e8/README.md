# Completed Core qualification of the partition-only reader fix

Exact `e8c426a73a14c1d97851082b90b815da60d4ef82` passed its explicit
current helper build, custody and full Core library/integration run: 1,707 passed,
zero failed, seventeen ignored, 127 Cargo targets. The canonical count excludes
one nested child summary and retains original telemetry-interleaved logs.
Original assertions and timeouts are unchanged. The failed v1 custody wrapper
is preserved separately; this completed v2 changes only that wrapper.

The same exact source built the frozen debug oracle_server diagnostic artifact
recorded in the build receipt, SHA256
`f296acc1c432256047aca0e91ed85767af753c17441ff2cc14d6ed4b942ec4aa`.
It is for the unchanged original95 PG batch and is not a production CLI,
release, deployment or passing application result. Its actual path is
`/root/bicdb-durability-s3/qualification-artifacts-debug/e8c426a73a14c1d97851082b90b815da60d4ef82/oracle_server`
on build host `74.50.69.58`.

This is the partition-only branch before the combined rename/catalog work and
the live cache/atomic session-certificate integration fixes. It does not qualify
those later changes or establish portable-origin promotion, full Oracle/PG
application acceptance, UI backup/restore, physical HA or automatic capacity.
The full original goal remains active, experimental production stays disabled,
and no new cloud spend or promotion is claimed.
