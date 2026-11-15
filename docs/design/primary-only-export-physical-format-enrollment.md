# Primary-only export and physical FORMAT enrollment

This source-only correction is based on exact public source
`6de646beb603accf39358017d8e0c83c39c741b1` (tree
`de0b68af8d158f39358604590769afb8237115f0`). It is uncompiled and its new
controls have not run. No public branch, workflow, version or historical
fixture is changed by this isolated source leaf.

## Original failure and preserved contract

The actual Core run reached
`index_disabled_primary_handle_preserves_legacy_predicates_and_refuses_mutations`
and failed its original final FORMAT-byte equality assertion. The added
`physical_backup_gate_v1` feature changed the legacy main482 FORMAT despite
`with_secondary_indexes(false)`. The actual fixture hashes, original test
body, acknowledged full-value read, exact mutation refusal and both final
index/FORMAT assertions remain unchanged. Enrollment is not inserted into
the fixture to evade that assertion.

`DbConfig::load_secondary_indexes` is a bounded primary-record export
profile, not a general filesystem-immutable database opener. Its public
contract refuses foreground mutation because the secondary constraint catalog
is unavailable. `ensure_writable` enforces that refusal;
`decimal_predicate::check_open_admission` and
`finish_decimal_predicate_upgrade` explicitly avoid certifying or rebuilding
the unrelated legacy predicate generation. Actual WAL recovery, Page reader
fencing/checkpoint, transaction-log recovery and close maintenance still run.
Existing format-version migration behavior is outside this narrow correction;
no universal no-I/O or arbitrary old-format preservation claim is added.

## Narrow implementation

At the existing post-root/Page-ownership, root-SH FORMAT boundary, normal
indexed opens still use the unchanged unconditional durable enrollment
producer. A primary-only open reads and validates the actual existing FORMAT
under its original feature lock. If the physical feature is absent, it does
not add it or rewrite FORMAT. Its actual DirectoryLocks, physical locks,
Page recovery and foreground mutation refusal remain intact. No absence
becomes backup permission: the unchanged Standard live-backup producer
requires actual enrollment and must refuse this still-open legacy handle.

An already-enrolled primary-only root remains strict. The original missing
kernel gate inode check runs before this boundary and refuses replacement.
The existing FORMAT file is opened existing-only without truncation and
synced, followed by the storage layer's existing parent-directory barrier,
without rewriting its bytes. This is a new actual barrier, not an inference
that a previously visible flag's earlier sync succeeded. Errors propagate
unchanged and prevent a handle from being returned. Windows retains the
existing file-flush/directory-entry contract; no stronger platform durability
or executed Windows coverage is claimed.

## Added genuine control definitions (not executed)

- The actual unchanged main482 fixture is opened primary-only twice. Both
  original FORMAT and index bytes stay exact through public close. While
  each actual handle remains open, the original Standard backup API must
  return the exact typed unenrolled-gate refusal. The original full row
  remains readable before and after that refusal.
- A normal fsynced writer really enrolls a new paged root and acknowledges a
  full row. With that writer closed, the actual gate inode is moved aside.
  A primary-only open must return the exact missing-enrolled-inode error
  without recreating a gate or changing FORMAT. Restoring the same actual
  inode permits a distinct normal open and full readback.
- A real enrolled root is reopened primary-only with faults at each actual
  existing-FORMAT file/parent-sync boundary. Two attempted opens retain typed
  I/O errors and exact FORMAT bytes; the fault must actually fire. After the
  fault is disarmed, a distinct open, full acknowledged-row read and close
  succeed without changing FORMAT bytes.

These are source definitions pending normal CI. They do not qualify HA,
old-binary execution, a physical backup/RPO campaign or the separate cold
bootstrap association protocol. All original backup, Page, WAL, predicate
and authority assertions remain in place.
