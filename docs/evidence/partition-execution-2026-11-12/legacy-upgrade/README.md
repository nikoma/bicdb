# Local partition upgrade evidence

This is private development evidence. Default SQL/startup admission is still
unimplemented. This batch is not merged, released, deployed, or qualified for HA
or distributed placement. The inherited source version is 1.0.487-beta.

The real merged 1.0.482-beta CLI, source
`f43583da4c34209e810c5e6a8f5925831b2949a1`, created six original stores: parent
heap and child acknowledgments, an acknowledged out-of-bound child row, and
independently stored child policies, each in EmbeddedMemory and ServerPaged.
`partition-old-store-fixtures-482-20261112/ack-ledger.jsonl` independently records
fourteen acknowledged rows. The creator records binary hashes, SQL output and
every original store file's hash. Each candidate uses a copy and verifies that
the originals remain unchanged. No application assertions or timeouts changed.

| Source | Result | Scope |
| --- | --- | --- |
| `1f6a25ed` (unchanged `97aabad9` runtime) | 8 mandatory failures | Six default upgrade cases and two actual old-reader controls. Parent upsert fails to preserve logical uniqueness; invalid bounds and ambiguous policy states are admitted; old482 opens and changes candidate-used copies. |
| `b715e546` | 3 focused passes | Core sticky required-reader flag, eight initial activation I/O boundaries across two modes, healthy/quiescent/durable-index prerequisites. |
| `3b0289b2` | 4 focused passes | Same Core runtime; adds already-visible-flag retry synchronization/fencing. Four total tests, not seven distinct tests. |
| `08f42204` | 6 inspector cases pass | Actual old stores, stable reports over two reopens, ACK counts, parent-heap rows, invalid child's destination and ambiguous raw versus historical child policies. |
| `eccbe714` | 8 explicit API cases pass | One atomic migration publication, parent ACK routing/upsert/index verification, reviewed relocation, both policy choices and exact raw archives, two reopens, old482 refusal without file changes. |
| `e45a5118` | 1 focused test and the same 8 API cases pass | Adds preflight for actual PG16 partition primary/unique-key requirements and bounded retained plans. The focused test covers two invalid definitions in both modes before the flag or catalog changes. |
| `f5ee9eec` | 1 focused pass, 3 setup failures | New helper bundled partition DDL and never created children; retry assertions were not reached. The dependent old-store runner refused its failed prerequisite, so no eight-case runtime result exists for this source. |
| `adfa24f3` | 4 focused tests and the same 8 API cases pass | Runtime unchanged from f5, helper corrected at d965, docs only afterward. Adds interrupted activation resume, exact completed-plan receipts without another commit immediately and after two reopens, typed marker preservation through generic saves, stale/wrong-directory/altered-action and unknown/unfenced-certificate refusal. Old482 again refuses every upgraded copy without byte changes. |

The repeated eight-case runs are the same scenarios at successive revisions,
not twenty-four distinct scenarios. The six default admission and two default
reader requirements remain open. Explicit API successes do not qualify startup,
SQL text/AST/COPY/procedure/direct engine access, or replicated-store admission.

The API archives both reviewed policy decisions, including `keep_stored`, with
the exact original raw schema record. It never guesses whether a stored child
policy was copied or independently authored. The historical choice reproduces
the actual old loader's immediate-parent policy state. Original store bytes
remain unchanged; an authorized post-upgrade upsert intentionally updates one
migrated row's payload in the candidate copy.

The latest probe is a debug example, not the standard server or CLI:

- Source: `adfa24f3118c0c2025b5035fdce3f4f69de93241`.
- S3 host path: `/root/bicdb-durability-s3/qualification-artifacts-debug/adfa24f3118c0c2025b5035fdce3f4f69de93241/partition_upgrade_probe`.
- SHA256: `92ca8af528f31f9749cf3d4d682ab4315621d682fad7254fcb332cb8e9640f9f`.
- [Latest actual eight-case result](partition-upgrade-resume-fixture-fixed/result.json).
- [Latest monitored build and focused receipt](report-partition-upgrade-resume-fixture-fixed.json).
- [Raw file hashes and stored compressed names](raw-manifest.json).

Logs are retained as lossless gzip with uncompressed SHA256 and byte counts.
`reproduction/` retains exact creator/runtime controllers and source-pinned build
specifications. The Native PG16 key-constraint reference has its own raw manifest.
All temporary reference containers are removed; original fixtures and required
old/current binaries are retained. Builds use one Cargo job, incremental off,
a 12GiB/10GiB cgroup, and active host/cgroup memory-headroom monitoring. The
initial failed f5 run and its dependent-run refusal are preserved separately.

Remaining gates include automatic entry/startup admission and fresh-definition
publication, all-resulting-row constraints and Native protected authority,
additional stale/extra/duplicate ACK and trigger conflict controls, independent
process/I/O/crash/VM cuts with ACK reconciliation, standby capability propagation,
namespace/rename/FK/hash/ONLY/snapshot/concurrency paths, broad SQL/protocol and
final application acceptance, then fresh versioning, CI, merge/push verification.
The raw multi-statement DDL observation remains unqualified; fixing this helper
does not claim to fix product statement-list semantics.
