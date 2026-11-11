# Sparse committed decisions finish a compacted source's tail

**Three distinct local component tests pass** at private source
`0d58359b07c31b38ed432e0a72ef7996ee2544b7`, package `1.0.471-beta`.
This is not public-main CI, automatic orchestration, a physical fault or
production qualification. A separate codec pass belongs to `1e6e3a0a`, not this
three-test cohort. The bundle retains two earlier failures unchanged.

The actual 1025-ACK fixture from the earlier failed recovery experiment is copied
before any engine open. Strict native/metadata loaders verify the new owned copy;
the original and custody roots are not engine-opened. Data participant n2 is
unavailable; metadata still has its real challenged quorum. Delivery faults are
in process, not actual VM/network failures.

Source n0 starts at native/resolved 1025, raw CQ 1026, compaction through 1 and
unmaterialized native pending 1026. n1 starts at raw/native 0. Normal contiguous
certification correctly refuses the gap on n1. A separate receiver-local operation
independently selects the committed command from actual fresh fenced voter
observations, then atomically persists its complete body, original decision
audit and a current-session marker on n1. It creates no raw log entry, no applied
floor, no native row and no serving/readiness permission. Version-2 fence
obligations explicitly retain the sparse index above raw last0.

Actual current certifications on n0+n1 now permit historical native Apply on n0.
The source advances native/resolved to 1026 and obtains actual SourceReady1026.
The behind replica remains genuinely raw/native0 and refuses Ready. All 1025
original external ACK value digests reconcile; command1026 was uncertain in the
original call and is never counted as a new application acknowledgment.

The other two passing cases exercise actual two-range RF1 preparations: activating
range A and raising the global catalog format does not invalidate B's unchanged
Prepared/Completed proof. Deliberately lower/unknown formats still refuse. These
are local storage/protocol cases, not network or HA qualification.

Earlier failures are retained with their real boundaries:

- `b435f68c`: the new sparse catalog codec correctly encountered an omitted native
  maximum-format guard; no sparse protocol case was run. `1e6e3a0a` repairs the
  guard and passes that codec case separately.
- `1e6e3a0a`: the actual sparse decision and source Apply succeeded, but the later
  negative test requested a recipient role with the wrong initiating actor. The
  guard rejected it. Test-only `0d58359b` instead calls the behind recipient's
  Runtime with the real leader actor and preserves the required Ready refusal.

**Image installation and cold sparse reissue are not tested by this cohort.**
Subsequent source review identified that the old target image-history token omitted
sparse decisions; its separate fix and tests are not retrospectively attributed
here. No complete behind-replica repair, activation, new write, strong read,
convergence, throughput, RPO/RTO or p50/p95/p99 follows from these results.

The complete successful and earlier wrong-recipient roots plus raw runs are in
restricted fsynced custody: `private/native-sparse-source-tail-0d58359b`, 137 files,
16,401,076 bytes; manifest SHA-256
`40305c743676d805d2fde2e0e910592c573cd9727052e27ef0add0952d2646e6`.
Files/directories have 0600/0700 permissions and source/destination hash parity.
Generated database keys remain private. No executable was archived here; the
recorded compiler output identifies a mutable shared test-executable path, not a
released operator binary.

Run `python3 verify.py --repo /path/to/bicdb` for manifest/raw-result checks and
exact Core source reconstruction from the pinned public base. Run
`python3 test_verify.py` for integrity negatives. Neither command reruns Rust,
attests external packet authenticity or grants production admission. The manual
1025-ACK test requires the separately retained raw fixture and `--ignored`; its
absence is not a passing test.
