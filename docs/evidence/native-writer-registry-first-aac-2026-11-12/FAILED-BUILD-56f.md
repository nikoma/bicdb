# Separate later source: 56f Core compilation failure

Exact source `56f9a3c999fba329c349d3e6dda664dcfb64e4c6` failed
compilation in both selected Core jobs. **No selected tests executed.** This
neither changes the earlier aac receipts nor establishes a production defect.

| Run | Core job | Requested scope |
| --- | --- | --- |
| [43301067134](https://github.com/nikoma/bicdb/actions/runs/43301067134) | 131472125317 | `distribution_native_host::` |
| [43301074482](https://github.com/nikoma/bicdb/actions/runs/43301074482) | 131472153960 | `native_ready_actual_tls_both_matching_peer_attempts_expire_without_counting_or_mutation` |

Both emitted E0599 for the same three **test-only** snapshot calls to
`db.scan("items")`: expired_ready_delivery_tests.rs:184, and
owned_validation_wait_tests.rs:53 and :140. `BicDb` has no such method; Rust's
iterator-method fallback produced “RwLockReadGuard … is not an iterator”.
There was no selected test list or runtime summary. In particular these jobs
did not execute the strengthened original-generation audit negative or the
new bounded metadata-lock tests.

The raw/index files are retained in `/tmp/bicdb-native-foundations-ci`:

- `56f-host-registry-first-job131472125317.log.gz` and matching `-index.json`:
  965,024 uncompressed bytes, SHA-256
  `d13e6853f5ef1e4f73014178b2c574d61586fd9600d41d243a7a60cac3d4cd2c`.
- `56f-ready-expiry-first-job131472153960.log.gz` and matching `-index.json`:
  966,108 uncompressed bytes, SHA-256
  `37e5ef2b2925178dea7cfe0fd320194510f8faf4d0f44dce389633d425e51cbf`.

The test-only correction `626736598093281a440ca500794846f9373bcb4c`
uses actual `BicDb::scan_collection(&self) -> Result<Vec<Record>>` for all three
snapshots. Formatting/diff checks alone were reported for that correction;
these failed receipts provide **no runtime result for it**. Separate CLI and
workspace jobs were preserved, without cancellation or blind resubmission.
Their eventual results are not asserted by this bounded failed-build note.
Machine-readable identities are in `failed-build-56f.json`.
