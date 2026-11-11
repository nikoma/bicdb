# Explicit strong-read P0 characterization

Actual linked core: **1.0.463-beta**, source
`d49a391b3c6157f90f29e9f2f5214143c60f9f4b`, optimized core library SHA-256
`dc714b001a0fe8758473c2c4ef80158474d7426e16630926edb65b6cc35f0356`.
See [the additive design/proof obligations](../../explicit-strong-range-read-design.md)
and [fixture source](../../../scripts/experiments/strong-read-characterization.rs).

Two actual-core characterizations **passed**:

1. Previously recovered n0 retains its old view after scripted minority isolation;
   n1+n2 complete a real core transaction under the newer scripted authority.
   Legacy Get at old n0 successfully returns absence. This demonstrates that
   cached recovery plus the named-leader check is not fresh strong authority.
2. New named n1 returns absence before catch-up, then the exact value after the
   real coordinator's prefix/tail recovery. Its authority epoch is 2 while its
   retained command epoch remains 1 and resolved floor is 1. Requiring command
   epoch to equal routing epoch would incorrectly reject this positive state.

The explicit `strong_get` **healthy-value target failed**, exit **101**. Its legacy
healthy control returns the expected value, but the current server rejects the
unknown strong operation. This is retained red evidence that the additive feature
is absent. An implementation that only returns unavailable cannot pass this gate.
The red target was not deleted, weakened, or reported as a passing engine test.
No strong-read implementation is delivered by this design/characterization work.

These are one-process fixtures using real core database/coordinator/services and
loopback event RPCs, an in-process replica transport, scripted topology publication,
8 MiB paged pools and fsync=false. They exercise neither metadata elections nor
independent machines, physical failures, TLS security or production durability.
They do not violate legacy Get's eventual contract, identify the Azure cause,
or establish strong-read safety/liveness under partitions/reconfiguration.

`build.json` records a 14.0-second standalone `rustc --test` link, without an engine
or Cargo rebuild; active memory monitoring observed at least 21.0 GB available.
The parent's CLI build was preserved. Build arguments, input library/source hashes,
test-binary hash, complete green/red results and both export/original digests are
retained. Absolute task paths and the panic PID were sanitized; these are distinct
exported bytes. The binary/cache and private raw outputs remain outside Git.

Reproduction requires an exact d49 source checkout and its already compiled locked
optimized core/dependency cache. Substitute real paths for placeholders in the
recorded build argv, verify the listed library hashes, and execute the resulting
standalone test binary:

```sh
strong-read-characterization characterize_ --nocapture --test-threads=1
strong-read-characterization target_explicit_strong_get_must_serve_a_healthy_quorum_value --exact --nocapture --test-threads=1
```

The first command currently exits 0; the second intentionally preserves the
unimplemented positive contract as exit 101. These are separate commands/results,
not a green broad regression or release-qualification claim.

To check the retained evidence without executing or opening any database:

```sh
python3 scripts/verify-strong-read-characterization.py
```

Its successful evidence check explicitly reports `strong_healthy_target_passed=false`.
