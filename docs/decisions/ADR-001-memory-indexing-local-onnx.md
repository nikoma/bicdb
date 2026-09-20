# ADR-001: Memory Indexing Uses Durable Async Jobs with Local ONNX Embeddings

## Status
Accepted

## Date
2026-07-06

## Context
BicDB can store vectors, but application developers still need an external embedding
pipeline to produce them. For local-first and offline regulated deployments, that pipeline
must not require a network service or expose sensitive data to a remote provider.

Sensitive writes also need database durability semantics. A row insert should not fail
or stall by default because an embedding model is cold, missing, slow, or corrupted.

## Decision
Add memory indexing as a BicDB database capability:

- `CREATE MEMORY INDEX ON table(field) WITH (model = 'embeddinggemma-300m')`
- default `mode = async` and `consistency = eventual`
- committed rows enqueue durable memory-index jobs after the write commits
- `mode = sync` explicitly processes the relevant jobs before returning
- local ONNX model registration is explicit through `bicdb model enable`
- memory job processing runs in-process through BicDB core, not an external server
- semantic search uses BicDB records and stored vectors through `SIMILAR_TO`/memory search

The first local provider is `embeddinggemma-300m` from q4 ONNX files on disk. Remote
embedding providers remain out of scope until they can be explicit opt-in because
they change sensitive data and network behavior.

## Alternatives Considered

### Application-Owned Embedding Pipeline
Pros: Keeps BicDB smaller.

Cons: Every application has to duplicate queueing, retry, model loading, and vector
storage behavior. This is especially poor for offline offline regulated deployments.

Rejected because the database should own semantic indexing once a field is declared
as memory-indexed.

### Synchronous Embeddings by Default
Pros: Newly inserted rows are immediately searchable by meaning.

Cons: Durable writes become coupled to model inference latency and model health.

Rejected as the default. Sync remains available only when callers explicitly request
immediate semantic visibility.

### Remote Provider First
Pros: Easier model lifecycle and often better embedding quality.

Cons: Network dependence and sensitive-data exposure are unacceptable defaults for the intended
local-first offline sensitive-data use case.

Rejected for the first implementation. Remote providers can be added behind explicit
provider configuration later.

## Consequences
- The write path remains durable-first in the default mode.
- Semantic indexing can run offline with no sidecar vector database or model server.
- Query freshness is eventual unless the index or write requests sync behavior.
- The worker batches vector writes by collection, but retry/backoff policy still needs
  more production tuning as volume grows.
- Transaction-held writes, buffered commits, and secure wrapper inserts enqueue memory
  jobs after the commit is durable.
- Large local model payloads are local runtime artifacts in `models/`; normal git
  tracking is reserved for lightweight metadata and instructions.
