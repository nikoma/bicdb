# Cooperative filtered BM25 cancellation

`full_text_bm25_top_k_filtered_cancellable` preserves the existing filtered BM25
contract and accepts a request-owned `CancellationToken`. The previous API still
uses no token. `full_text_bm25_top_k_budgeted` now forwards its existing token into
the native execution path. A pinned `FullTextReadSession` can also use
`with_cancellation`; physical generation ownership is unchanged.

Checks occur at posting-fetch batches (including parallel document ranges and
boundary discovery), impact rounds/candidate batches, write-layer entries,
exhaustive posting blocks and selected-hit primary-key materialization. An optional
impact seed must not swallow cancellation and start another scan. Cancellation is
cooperative: a synchronous storage read, decode batch or sort already in progress
is not preempted. This is not a hard real-time deadline guarantee.

Validation:

```
cargo test -p bicdb-core --no-default-features --features compression --jobs 2 \
  --test fts_cancellation --test fts_storage_generation \
  --test fts_adaptive_partitions --test paged_fts_postings \
  --test fts_segment_equivalence
```

All 42 tests passed. The cancellation test compares complete hit debug values for
single/multi-term, AND/OR, filtered/unfiltered, and multilingual searches. It then
waits until native decoding starts before canceling an exhaustive query, asserts
less decoding than the complete reference, a sub-second grace interval, and a
successful next query. An expired pinned session rejects block-max, impact and
tail-merged execution. The existing packed/keyed, update/delete, generation and
partition-equivalence gates also pass.

The storage-generation compression-size test requires the `compression` feature;
running it with all default features disabled and no compression fails its size
assertion. The cancellation and packed/keyed tests also passed without compression.
No production data was used or changed by these tests.
