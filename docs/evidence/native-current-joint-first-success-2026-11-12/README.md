# First CurrentJoint recovery success: bounded component receipt

Exact tested source **`eedf23d58e236cf62d61366349ad4de3bd52373b`**,
Core **`1.0.489-beta`**, data wire protocol **28**. These are historical source
identities, not the version of a later documentation delivery or remote main.
This compact receipt includes complete job logs and original workflow artifacts;
it does not reconstruct all compiled source inputs or supply a database image.

| Executed cohort | Run / Core job | Actual result |
| --- | --- | --- |
| Original full post-joint Prepare-only recovery + separate later Runtime idempotence | [43319425823](https://github.com/nikoma/bicdb/actions/runs/43319425823) / 131535932600 | **2 passed, 0 failed, 0 ignored, 955 filtered; 100.89s** |
| Actual image-target n3 historical-anchor/archive fault control | [43319432266](https://github.com/nikoma/bicdb/actions/runs/43319432266) / 131535951702 | **1 passed, 0 failed, 0 ignored, 956 filtered; 98.32s** |

There are **three full unique passing test IDs**, listed in `result.json`.
Shared fixture prerequisites are not additional passing tests. Separate compile
jobs **131535889066** and **131535911651** both passed workspace all-target
checks and isolated consumer checks without Core defaults. Those jobs typecheck;
they do not execute the broad workspace/application regression suites.

The unchanged full positive now completed genuine Prepare-only slot 3 recovery:
actual canonical NoData/Abort3, all three fresh Runtime activations and releases,
real **ACK4 (the third business ACK)**, and StrongR4 value/absence checks, with
inherited ACK1/ACK2, permanent promises and Commit obligations preserved. The
separate idempotence test first completes that acceptance, then reissues the
actual historical activation through independently fresh QQ/role at current R4.
An AnchorR3 remains historical evidence; it is not current ReadyR4 or a read grant.

Actual member diagnostics now directly distinguish the copied target:

| Actual n3 anchor fact | Observed value |
| --- | --- |
| Immutable physical image members | n0, n1, n2 |
| Protected effective NEW members | n1, n2, n3 |
| Fresh readonly observer members | n1, n2, n3 |
| Historical anchor | R3, local B2 |
| Current native point after ACK4 | R4; its own actual local B is preserved |

The CurrentJoint reader uses the actual protected joint bridge and matching
serving joint SHA, exact current/immutable serving archive equality, and the
exact effective voter credentials. NormalRecovery retains its existing physical
membership contract. No metadata member list alone becomes DATA authority.

The new control requires the real fsynced three-Record ACK ledger and the full
StrongR4 prerequisite. It first obtains actual mTLS readonly AnchorR3 from n3 at
current R4. A test-only native metadata-WAL transaction then deletes **only** the
actual serving archive after proving current == archive. The next real readonly
request refuses with **`immutable joint activated serving archive is missing`**,
with **3890ms receiver budget remaining** and original caller still live. It
checks unchanged current pointer, full R4/H/B/UID/rows/slots/raw catalogues and all
three ACK Records. It does **not** qualify Strong reads after deliberate damage.

## The whole candidate is not qualified

The sibling run [43319438217](https://github.com/nikoma/bicdb/actions/runs/43319438217),
same exact source, Core **131535993682**, executed **0 passed, 4 failed, 0 ignored,
953 filtered; 81.00s**. All four reached actual target ACK2 and checked R2/history/
rows, then failed the **StrongR2 prerequisite**, before CurrentJoint or any of
the four requested fault cuts. It produced **zero NativeProbe events/contexts/
spans**, so it supplies no inner-observer cost measurement. Its separate compile
and consumer job 131535934325 passed. The full sibling failure log is retained
here; it is not converted into a passing negative or hidden by these successes.

Consequently these three passes do **not** establish whole-candidate stability,
operator/autonomous 4→5, real remote image streaming, online copy, higher-epoch or
cold rebinding, 8 GiB capacity, physical/VM fault qualification or deployment.
The full fixture uses same-open local opaque image/joint orchestration alongside
real authenticated metadata/data RPCs. Its ordinary policy refresh preconditions
remain distinct from a delivered operational DATA-policy transition producer.

## Retention and verification

`raw/` retains full compressed job logs, including warnings and server EOF/
BrokenPipe diagnostics. Exact byte/hash inventories are in `result.json`.
The positive runtime log contains **102** `unexpected end of file` and **55**
`broken pipe` textual occurrences; the archive control contains **6** and **12**.
Those counts are diagnostics, not an attribution or a zero-error transport claim.
Complete actual member/refusal/terminal lines are in `actual-markers.json`.

Original successful artifacts **13990062815** and **13990905691** are included
byte-for-byte. Each contains **only four workflow files**: rustc identity,
source SHA, selected-test listing and test log. They contain **zero database,
native-WAL, stopped-root or external ACK-ledger custody files**. Successful
fixture database roots were not retained. Test ledger assertions do not become
an independently replayable dataset simply because their logs are retained.

The sibling failure has separate restricted live-owner custody: artifact
13989698674, four manifests / 300 verified payloads / 18,711,064 bytes. Its receipt
is included, but its private database/signing payload is not. Neither that capture
nor this receipt performed engine open, checkpoint, repair or replay; observed
stable copies with live owners are not atomic power-loss/recovery qualification.

Run `python3 verify.py` (optionally `--repo /root/bicdb`) and
`python3 verify.py --self-test`. No Cargo/build is required. The optional repository
check validates exact source version and selected producer/test identities; it
is not full compiled-source reconstruction. This documentation adds no runtime
feature, version reservation, release, binary, main merge or deployment claim.
