# Profile of one serial COMMIT on ny8 (diagnostic only)

## Method

- **Workload:** `profile_commit.py` runs a minimal client loop against a fresh disposable copy of `pristine-paged-current-schema-run.json` (deleted afterwards). Settings match the official runs: `oracle-preview-v1`, `BICDB_SYNC_OUTBOX=off`, `MALLOC_ARENA_MAX=2`, 64/64 limits.
  - One connection with autocommit off.
  - Per transaction, one statement: `UPDATE AD_SEQUENCE SET CURRENTNEXT=CURRENTNEXT+1` (iDempiere's id allocation) or `INSERT` into a 3-column table with a primary key.
  - Then `COMMIT`, then a 10 ms pause.
  - 200 commits per kind (plain runs) or 100 (traced runs). The client times each COMMIT (`time.perf_counter`) and records wall-clock marks.
- **Tracing:** `strace -f -tt -T -y` on the server, covering open, write, pwrite, fsync, fdatasync, sync_file_range, rename, fallocate and futex. `analyze_strace.py` assigns every syscall to the client's COMMIT windows. The first 5 commits are skipped.
- **Overhead:** strace barely changes latency here: production p50 is 28.3 ms traced and 28.3 ms plain.
- **Binaries:**
  - A = `decb9d0` (before #55).
  - A2 = main `3f3f9a7` (includes #55 fail-closed); sha256 `8c791626574d3ef0e7fd8e8c09e83c17233f02fb5b735308b3b06cdb921209ab`, built on s3ny like A and B.
  - B = `49d0464` (#43).
- **Gates:** set only on the disposable server.

## Client-side COMMIT latency (ms; p50 / p95 / p99)

| Variant | UPDATE | INSERT |
|---|---|---|
| A (`decb9d0`, production) | 28.1 / 34.4 / 60.7 | 28.3 / 32.4 / 44.5 |
| **A2** (main, production, with #55) | 28.4 / 32.8 / 43.7 | 28.0 / 38.2 / 66.9 |
| **E** (A2 + `BICDB_COMMIT_DURABILITY=transaction-log`) | 18.5 / 35.1 / 39.5 | 18.6 / 25.7 / 45.6 |
| E on the B binary (gate off) | 18.7 / 25.1 / 29.7 | 18.8 / 27.5 / 33.3 |
| E + #43 (B binary, both env vars) | 16.4 / 26.2 / 33.9 | 17.5 / 23.3 / 52.9 |

The probe reproduces the commit latency seen in the application runs: about 28 ms here against about 31 ms inside the job.

## Where the ~28-31 ms goes: production path (A2; A is identical)

Per commit, all on the connection's own thread and strictly serial. The single commit shown is commit #50; the per-commit means are over 195 commits.

| Step | Syscall | Size | Time |
|---|---|---|---|
| 1 | `openat db/.backup-write.lock` | | ~0.04 ms |
| 2 | `write(transactions.log)`, an O_DSYNC append of the `tx_write` record | ~2.1 KB | **8.8 ms** |
| 3 | `write(paged/store.wal)` x 4: three full 8 KiB page images + a 40 B commit record | | 0.13 ms |
| 4 | `fdatasync(paged/store.wal)` | | **9.1 ms** |
| 5 | `openat db/.backup-write.lock` | | ~0.04 ms |
| 6 | `write(transactions.log)`, an O_DSYNC append of the commit marker (`{"type":"tx_comm…`) | 101 B | **8.9 ms** |
| | gaps between syscalls (CPU / user space) | | about 0.5-1 ms |

| Per-commit mean over 195 commits | A2 | A |
|---|---|---|
| `transactions.log` writes | 18.0 ms (2.00 calls) | 18.3 ms (2.00 calls) |
| `store.wal` fdatasync | 9.3 ms (1.00 call) | 9.2 ms (1.00 call) |
| `store.wal` writes | 0.14 ms (12 calls) | 0.13 ms (12 calls) |

- **Answer:** there are **three** serial device syncs per commit, each about 9 ms on md0 (the same as the raw fdatasync median measured earlier). The "extra ~10 ms" is the second synchronous `transactions.log` append, the commit marker. 3 x ~9 ms ≈ 27 ms, plus ~1 ms, gives the 28-31 ms measured.
- **Nothing undocumented:** in the commit windows there is no other fsync, fdatasync or O_DSYNC write. That covers the outbox (off, as in the official runs), any audit or sidecar file, and `store.pages`, which gets 0.18 pwrite calls per commit and no sync.
- **#55 adds nothing:** A (before #55) and A2 (after) have identical syscall profiles. It adds no syscalls, no syncs and no measurable latency on this path.
- **Lock waits and CPU:** on the production path the connection thread does not wait on a lock. The futex time that falls in the windows belongs to idle background threads. CPU and user-space gaps are about 1 ms, and statement execution, outside COMMIT, takes 0.7-1.7 ms.

## Transaction-log-only durability (E): where the remaining ~18 ms goes

- **What changes:** the connection thread now makes **one** synchronous write, the `tx_write` O_DSYNC append to `transactions.log` (~9.6 ms). A background materializer thread does the page-log fdatasync and the marker O_DSYNC write.
- **Why it is still ~18 ms:** those background syncs collide with the *next* commit.
  - The commit's page-log appends wait while the materializer holds the page-log file mutex across its `fdatasync`. In one traced commit, the connection thread's `store.wal` writes start 0.08 ms after the materializer's 12.1 ms fdatasync returns. This is exactly the locked sync that #43 removes.
  - With #43 also on (16-17.5 ms), the remaining wait is on the `transactions.log` lock. The materializer holds it across its O_DSYNC marker write (~7-9 ms), and the next commit's own O_DSYNC `tx_write` queues behind it.
  - So under E a commit ≈ its own ~9 ms sync + one overlapping background sync.
- **Probe caveat:** the probe pauses only 10 ms between commits. In the real job about 40 ms of JVM work separates commits, so the background syncs may overlap less there. The application runs (A2 versus E) measure that directly.

Raw data: `profile/<label>-<mode>/` (`result.json`, `marks.json`, `strace.txt`, `server.log`).
