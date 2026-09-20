# Large Values and Attachments

BicDB keeps normal SQL `TEXT`, `BYTEA`, JSON metadata, vectors, and record
payloads inline in the append-only collection segment. Values at or below
`DEFAULT_LARGE_VALUE_THRESHOLD_BYTES` (1 MiB) should stay inline unless the
application has a document lifecycle reason to manage them as attachments.

Values above 1 MiB should use the Rust attachment API. Attachments are streamed
into `large_values/` as 1 MiB chunks, addressed by the plaintext SHA-256 content
hash, and referenced from record metadata with a
`__bicdb_large_value_ref` descriptor. The descriptor records the storage model,
hash, size, chunk size, chunk count, encryption state, media type, and creation
time. Reusing the same content hash deduplicates the stored bytes.

## Storage Model

Large values use a content-addressed sidecar store:

- `inline`: recommended for values <= 1 MiB and SQL/pgwire values.
- `content_addressed_blob_store`: recommended for BicDB-owned application
  attachments, scans, PDFs, generated reports, and audit payloads > 1 MiB.
- `external_object_reference`: recommended for very large document estates,
  cross-system retention policies, CDN access, or object-lock requirements.

Each sidecar chunk has a chunk header, plaintext SHA-256 checksum, and optional
database encryption using the same database encryption runtime as collection
segments. Writes go to a temporary file and are renamed into the content-addressed
path only after the full stream succeeds. A failed or interrupted write can leave
an orphan `.tmp` file, but no committed record points at it.

## Rust API

Create a normal metadata record first, then attach the large payload:

```rust
db.insert("documents", Record::new("doc-1"))?;
let reference = db.write_attachment_from_reader(
    "documents",
    "doc-1",
    "body",
    Some("application/pdf"),
    std::fs::File::open("invoice.pdf")?,
)?;

let mut reader = db.open_attachment_reader("documents", "doc-1", "body")?.unwrap();
std::io::copy(&mut reader, &mut std::io::sink())?;
```

Protected collections must use `db.secure(&SecurityContext)`. The secure
attachment methods reuse collection read/write roles and tenant visibility, so
cross-tenant callers cannot attach to or read another tenant's referenced bytes.

## SQL and pgwire

SQL and pgwire `TEXT` and `BYTEA` values are currently inline row values. BicDB
does not expose PostgreSQL large object OIDs, TOAST controls, or SQL-level
streaming attachment functions. PostgreSQL-compatible clients should store
attachment metadata and either:

- call the Rust attachment API in the application service, or
- store an external object reference with hash, size, media type, tenant id, and
  retention metadata.

This is an explicit compatibility boundary for greenfield transactional applications, not
a general PostgreSQL TOAST replacement claim.

## Backup, Restore, Sync, and Compaction

Backups include `large_values/` files in the encrypted archive manifest with
per-file checksums. Restores materialize the sidecar files before integrity
checks. `bicdb verify` and `bicdb check` report large-value blob counts,
decrypted bytes checked, checksum failures, and orphan temporary files.

Record sync and event payloads carry the metadata descriptor, not the attachment
bytes. Deployments that sync records between nodes must replicate the
content-addressed sidecar directory or use an external object store available to
all nodes.

Collection compaction rewrites row frames and preserves sidecar files. Because
content-addressed blobs may be shared by several records, blob garbage collection
is deferred until a reachability pass is added; operators may remove orphan
temporary files after verifying no writer is active.

## Transactional Guidance

Store application documents inside BicDB when the application needs database-key
encryption, backup co-location, tenant policy checks, and deterministic restore
from a single BicDB backup chain. Prefer external object storage when documents
dominate total data size, need object-lock/legal-hold workflows, public serving,
multi-region object replication, or lifecycle tiering independent of rows.

If documents are external, keep safe metadata in BicDB: tenant id, object key,
size, SHA-256 hash, media type, retention policy, encryption key reference,
created/updated timestamps, and authorization state.
