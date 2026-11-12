# Native store birth and read-voter enrollment components

Exact private source **`ee5cb1880a946b97816a15aa4e0cb12f7d03778d`** /
`1.0.471-beta` passed **32 distinct tests / 32 executions**: 11 metadata,
18 read, and 3 actual-factory tests. These are component and local mTLS results.
They do not deliver a strong-read value/absence API, operator startup enforcement,
automatic four-to-five growth, a release binary, or physical qualification.

This documentation is based on public main
`f3a063902d2fe316c29264fe20549c2fdd270242` / `1.0.478-beta`. That documentation
base is distinct from the tested private source. The retained source patch starts
at public `b0e2498842be7b01e355bb8da1cc249ff16be056` / 471; source reconstruction
covers six exact input states, with 1,372 tracked files per state across
`Cargo.toml`, `Cargo.lock`, `.cargo`, `crates`, and `examples/extensions`.
No later integration revision or public engine/CI result is qualified by this
receipt.

## Final exact-source cohort

| Run | Passing tests | Whole execution | Guard outcome |
|---|---:|---:|---|
| `native-birth-final-metadata` | 11 | 76.04 s | Exit 0; no pause/stop |
| `native-birth-final-reads` | 18 | 60.04 s | Exit 0; no pause/stop |
| `native-birth-final-factory` | 3 | 3.02 s | Exit 0; no pause/stop |

The full test identities are in `claims.json` and independently counted from the
retained stdout. The three groups are disjoint. Earlier runs below overlap these
identities and are **not added** to 32.

The intended worktree was forced to compile for the final metadata run. The read
and factory groups reused that same compiled source, flags and unit-test path.
The command used release dependencies, a Core package override of opt-level 0 /
debug 0, one build job, incremental compilation disabled, two test threads, and
`tls-replication`. The runner records rustc 1.96.0, commands, its captured
environment, source revision and empty tracked diff. Additional ambient build
environment and original execution authenticity are not independently attested.
This is source-input/result integrity, not a bit-reproducible build claim.

Final runs retained at least **21,637,189,632 B available RAM**, with no guard
pause or stop. The successful read group retained **522 BrokenPipe diagnostics**;
these are not hidden or reported as zero errors. The exercised assertions passed
despite those server-side diagnostics, including canceled/expired requests.
Individual diagnostic causes are not all separately classified.

The captured mutable libtest executable was 136,680,992 B, SHA256
`b8ba1c2b7c0d9bcdba51b4b09b6f15359b1757d0edf6b2e6b9d2c5c4462a3fd3`,
at the shared target path recorded in `final-source32-summary.json`. It was not
copied into this bundle, is not an immutable archived executable, and is not an
operator release binary.

## What the components establish

The three factory tests use genuine native birth/genesis producers. They
separate two physically created stores with the same declared specification,
refuse factory enrollment after an actual admitted write, and permit actual new
learner birth enrollment without promoting or assigning it as a voter. Cold
setup actually reattests empty genesis and scans schema; no counter or cached
digest is assigned to satisfy the guard.

The eleven metadata tests preserve legacy omitted-field encoding, validate the
data-only birth record, and fence enrolled topology format 2 / data protocol 21.
They reject committed birth rewrite/removal, live known-unbound-voter enrollment
(including generation 1), same-incarnation demotion/enrollment and deletion/re-add
bypasses. Native replacements require a decommissioned predecessor, a higher
incarnation and an unassigned learner in sequential proposals; separate generic
legacy behavior remains available. Authoritative snapshots may skip a genuinely
new member's learner phase while preserving known committed identities.

Uncommitted proposals are not permanent physical identity decisions. Two actual
three-MetadataStore regressions use real vote/append/probe handlers: one store
alone retains an uncommitted learner birth; the other two elect and commit a
higher-term barrier without it. After one of those two stores closes, the first
store accepts a valid conflicting-term append or committed snapshot and forms a
fresh metadata quorum with the remaining leader. Same-term/index conflicting
entries and invalid sequential incoming birth rewrites still refuse before
persisted mutation. These are in-process metadata regressions, not TCP/physical
fault or automatic-election qualification.

The eighteen read controls include actual TLS request/peer/challenge binding,
current R2 versus an older R1 activation anchor, raw-CQ/native-behind rejection,
original deadlines, expired-attempt retry, and worker/context lifetime. The final
source includes an actual successful-RPC count assertion for the expired healthy
attempt retry, rather than only an inferred second call. New enrollment cases
consume actual fresh metadata and actual peer birth facts: an unenrolled quorum
refuses enrollment checking; matching enrolled births pass; a genuinely different
factory birth placed in the fresh metadata cannot admit the original peer's
observations. Those tests return a birth-checked component result, **not a value
or absence permission**. No public StrongGet, MVCC read barrier, final read/value
producer or operator startup gate is established.

## Preserved earlier attempts

| Run | Exact source | Preserved result |
|---|---|---|
| `native-enrollment-v1` | `06da717949a062319646be72d14b31273321540a` | Compiler E0425: fixture referenced missing `scope`; no tests ran. |
| `native-enrollment-v2` | `256d797860b740884ffea082a6b77f83aaf036b7` | Factory 1 pass / 2 fail at actual unwritten-genesis validation. One PSI-triggered pause resumed; no guard stop. |
| `native-enrollment-v3` | `6641e13bc2a1f95773cc416929a1977b91340ffa` | Factory 1 pass / 2 fail: schema had not been actually scanned for this open. |
| `native-enrollment-v4` | `8a021a49c546d7d762650cf08c82a0c568a7c618` | Factory 2 pass / 1 fail: the other fresh `items` namespace was not yet paged. |
| `native-enrollment-v5` | `0c687a9692667edbabf7b78b87118d839b4d26ce` | Earlier-source factory 3 pass. |
| `native-metadata-v1` | `0c687a9692667edbabf7b78b87118d839b4d26ce` | Earlier-source metadata 9 pass. |
| `native-read-enrolled-v1` | `0c687a9692667edbabf7b78b87118d839b4d26ce` | Read 16 pass / 2 fail during helper recovery-phase interleaving, before the intended enrolled-birth checks; actual persisted recovery-fence scope changed. |

Original stdout/stderr, resource samples, inputs/results and the executed guard
are retained. Failures remain failures at their actual sources. Setup changes
performed real initialization/revalidation instead of weakening those guards.
The source patches also preserve the metadata semantic corrections; this table
is not a claim that all source changes between attempts were test-only.

## Custody and limits

The new factory and new enrollment-TLS fixture roots were not retained. They
have no external ACK ledger. Only the already-existing R2 read fixture's explicitly
printed stopped root was copied to restricted custody: **47 files / 824,217 B**,
manifest SHA256
`63e2da1e0da7e313469c0a4f28e23ab8f06b866620dd4ecccfc8c5dd18a9d056`.
A `/proc` FD/map scan found no source owners; source/destination hashes matched
and remained stable, files/directories were fsynced, and modes are 0600/0700.
The executed copy helper is retained. No database was opened for packaging.
Generated database keys remain private. This R2 archive is not archival evidence
for the new factory-pair or enrolled-TLS cases and does not create an external
ACK-reconciliation claim.

Host/writer startup enforcement for all voter births, the actual strong snapshot
and value/absence producer, cross-process node startup, remote native image
handoff, new data membership activation, broad Core/application regressions,
large-value/compaction qualification, complete audit/queue effects (issue 148),
automatic four-to-five convergence, physical faults and deployment remain open.
The companion [operator integration audit](../../design/native-operator-four-to-five-gap-audit-2026-11-11.md)
is a frozen source gap assessment, not an implemented feature list.

Run `python3 -B verify.py --repo /root/bicdb` for manifest/raw-result/source-input
reconstruction checks. `python3 -B -m unittest discover -s . -p test_verify.py -v`
runs eight semantic tamper cases plus one default-private-archive omission check.
Optional `--private-dir` with the custody path in `private-custody.json` verifies
the private exact file set, hashes and modes without opening a database. Without
that flag the verifier never reads or stats private archives. None of these
commands reruns Rust, independently certifies the original executions, or grants
operator/strong-read qualification.
