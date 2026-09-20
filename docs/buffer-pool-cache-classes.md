# Buffer-pool cache classes

## The failure this prevents

A sequential-ish document-text workload can evict the metadata and postings
that make subsequent search fast — while every individual subsystem still
looks perfectly healthy. Hit rate falls, latency rises, and nothing reports an
error. At 16 TiB that is what makes a machine with 256 GiB of RAM behave like
one with 16 GiB.

The pool already had 2Q scan resistance: pages enter a probationary queue and
promote to protected only on a second touch. That defeats a *single-pass*
scan. It does not defeat the pattern that actually occurs — **read the
document, then read it again to build a snippet**. The second touch is exactly
what promotes a document body into the protected queue, where it displaces the
term dictionary.

Worse, `evict_one` fell through from probationary to protected. Once a scan
exhausted the 25% probationary segment it began evicting postings directly.

## Three classes (1.0.216-beta)

| Class | Holds | May reclaim from |
|---|---|---|
| `Metadata` | the catalog B-tree — small, hot, walked by everything | **any class** |
| `Search` | postings, block metadata, index leaves | probationary → protected → streaming |
| `Streaming` | document bodies, large values, bulk scans | **streaming only** |

The hierarchy is deliberately asymmetric in both directions. **Metadata may
reclaim from anyone; nobody may reclaim from metadata.** A deep posting
traversal is `Search` and may legitimately churn the whole search budget — but
it must not take the catalog with it, because that is the structure every
other read has to walk.

Metadata gets the run of the pool when it needs a frame because a metadata
page that cannot be admitted fails the operation outright. Bounding it to its
own tier deadlocked a small pool, which is how that was found.

Budget is `metadata_fraction`, default 0.10, capped at a quarter of the shard.
A shard with fewer than 8 frames gets **no metadata tier at all** and its
metadata pages are admitted as ordinary pages — parking them in a queue no
other class evicts from would let metadata occupy the whole shard and deadlock
every other admission. That was the second thing this found.

## The rule, made structural

> **A cold document-body read can never evict a search page.**

Not "is unlikely to" — cannot. Pages are admitted in a class:

| Class | Holds | Evicts from |
|---|---|---|
| `Search` | postings, dictionary, block metadata, catalog, B-tree interior | probationary → protected → streaming |
| `Streaming` | document bodies, large stored values, bulk scans, snippet reads | **streaming only** |

A streaming admission reclaims a streaming frame or it fails. It never reaches
probationary or protected. A scan-resistant LRU makes eviction *unlikely*; a
separate budget makes it *impossible*.

The asymmetry is deliberate: search may reclaim from streaming, never the
reverse, so a pure-search workload still uses the whole pool while a document
scan stays in its lane.

**Streaming pages never promote.** However many times one is touched it stays
in the streaming queue — a document read twice is still a document.

## Budgets

`BufferPoolOptions::streaming_fraction`, default `0.15`. Small on purpose:
streaming reads want enough frames to keep a scan moving and to hold a page
across the read/snippet pair, not enough to cache a corpus that will never fit
anyway. Every shard reserves at least one streaming frame whenever it has two,
so a document read can always make progress.

## Instrumentation

```
streaming_evictions               frames reclaimed from the streaming class
streaming_borrowed_probationary   streaming admissions that had to take a
                                  probationary frame because streaming held
                                  nothing evictable
```

`streaming_borrowed_probationary` being non-zero means the pool is too small to
give document reads a lane of their own. It is still never a *protected* frame.

## The acceptance test, and its control

`a_document_scan_cannot_evict_the_search_working_set` warms a working set into
protected, streams 640 document pages through a 64-frame pool with each page
touched twice, then re-reads the working set and requires **every read to be a
hit**.

That test alone proves nothing without
`the_same_scan_without_class_isolation_does_evict_it`, which runs the identical
volume and the identical access pattern through the `Search` class and requires
that it *does* evict.

The control earned its place: the first version of the pair used two separate
passes over the scan range rather than two touches per page, and the control
**passed** — the existing 2Q already handled that pattern, so the isolation
test was not discriminating. Read-then-snippet is the pattern that breaks 2Q,
and it is the one both tests now use.

## Where the class is applied

Overflow pages — document bodies and large stored values — are read as
`Streaming`. Read-ahead stays in the search classes, because it is driven by
search scans.

## What is not done yet

- Only two classes. The three-tier split (hot / warm / cold) would separate
  ordinary posting blocks from dictionary and block metadata, so a deep posting
  traversal cannot displace the structures that make *every* query fast.
- Per-class residency, admission and read-latency counters are not yet broken
  out; only eviction and borrowing are.
- The end-to-end workload benchmark — popular queries, then a large document
  scan, then the same queries — is not automated. The unit-level acceptance
  test proves the mechanism; it does not measure the latency claim on a real
  corpus.
