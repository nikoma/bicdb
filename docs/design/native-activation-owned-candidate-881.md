# Retained activation catalogue candidate at 881

This unversioned private batch inherits source version `1.0.515-beta`; it reserves no delivery version. It starts at `63dcffad50386f9335600ce7c117cdb5cf6877b1`, the selective runtime port onto `881087806b3a5a9792b3e0ba46ba1ba1f2dd0617`. It is not the external PR206 branch or a delivery of its changed test budgets. The original OWN 1.5s, request 5s, SDK 2s, workflow settings and existing negative assertions remain the qualification contract.

| Evidence scope | Outcome |
| --- | --- |
| Source inspection: typed original/candidate retention before effects; generic health remains fenced during comparison | passed |
| Earlier implementation using only global uncertainty and equal bytes establishes ownership of a later ambiguous write | failed |
| Standalone formatting, diff checks and source preservation | passed |
| Compilation, regression execution, timing improvement and application acceptance of this batch | not tested |
| Bounded production recovery, cold restart and physical HA qualification | not tested |

The actual activation producer retains the original and candidate whole catalogue, encoded candidate, same DB open/StoreUUID/catalogue UUID, original native bridge, actual phase owner, producer-sealed preparation lifetime and weak original Frozen gate/lease identity before memory publication. These are private constructors and fields. No decoded receipt, global flag or UUID creates this owner. Each original/candidate model serialization and the retained candidate envelope uses the EXISTING protected catalogue head bound of 64 MiB (`catalogue_custody.rs`), with derived encoded accounting at most three times that bound. Original/candidate sizes are stream-counted rather than kept as scratch byte vectors. Rust models, the current Store head, temporary clones/checksum buffers and allocation overhead are additional and have not been measured; 192 MiB is NOT a resident-memory hard cap. Wire-fact/context/session/ordinary-entry limits are unchanged. At most eight entered IO outcomes remain on one owner; exhaustion refuses while retaining custody. A constant-size hash chain records earlier actual outcome classifications across completed owner replacement within the same Store open.

A fresh actual Open or HostMQ/OWN/current-MStore admission may use that exact pending owner. It rechecks the current native bridge and physical/provenance checks, original service/State/Registered owner, native open/catalogue bindings, exact original-or-candidate memory and disk heads, original preparation lifetime, and actual Frozen/Controlled lease. The necessary comparison does not make the generic catalogue healthy or confer Serving/APP/Strong permission. The ordinary strict currentness APIs and final scoped native publisher remain strict.

Every Store-owned whole-catalogue IO attempt advances one checked RAM generation immediately after preflight/encoding and before entering the writer, even for identical bytes or an error. Only the original pending activation with that current generation owns its uncertainty. An old verified owner, unrelated entered write, changed head, absent owner, ended original lifetime or changed gate refuses. A successful new replacement/fsync/readback and final actual request/version checks establish new candidate durability. Earlier IO errors remain errors; visible candidate bytes never establish that an earlier failed fsync succeeded. A refused pre-effect attempt can reuse the unchanged retained intent under fresh authority without replacing its origin or candidate.

The protected path additionally requires actual writer-enforced birth, sticky writer and abort-history reader profiles, the protected catalogue UUID and the retained native-catalogue DirectoryLock. This excludes another protected Store or bootstrap writer at that root. The generation is not a cross-process lock. Static NEW/Pending bootstrap writers precede owned Store construction and use create-new/protected directory exclusion; they cannot coexist with this protected owner. Raw file replacement, force restore and external writers outside those contracts are not covered.

A positively identified unprotected legacy profile keeps its original initial healthy activation persistence. It receives no retry owner. Missing protection on an enforced/protected birth is an error, never a constructor-error fallback to legacy. Legacy Unknown remains blocked. On a new Store open, neither counters nor visible bytes reconstruct this RAM owner or its original preparation/lease lifetime; this batch adds no cold continuation.

## Actual catalogue IO entry inventory

| Entry | Generation fence |
| --- | --- |
| `RangeWriteStore::persist_with` including ordinary/recovery persistence and native image adoption | immediately before writer callback |
| `persist_verified_open_catalog` and Registered CLOSED refusal callback | immediately before writer callback |
| Registered/Joined Installing and image-floor adoption | immediately before direct durable replacement |
| Registered native decision catalogue publication | immediately before direct durable replacement |
| Owned activation candidate | immediately before replacement; returned generation retained by its pending owner |
| NEW/Pending static bootstrap | no live activation owner; original actual root/directory exclusion retained |

The separate forthcoming remote native-decision catalogue producer must join this SAME generation and typed pending-ownership scheme at composition. It is absent from base 63dc; an independent counter or unmarked writer would invalidate the ownership contract.

## Regression definitions and remaining gates

Seven new real hot-fixture cases define actual target rename and directory-sync errors followed by fresh same-owner admission; actual foreign owner/gate refusal; cancellation of the original producer child created before native effects; actual same-byte refusal-persistence IO on an installed OLD survivor after completed activation; and a stock original SDK append/full Strong on untouched R2 followed by fresh admitted stale R1 whole-head comparison. The Registered target before FIRST does not impersonate an installed R2 writer. Exact returns and storage/fault evidence are saved before assertions. Catalogue-only retries compare native preparation/bridge records and actual next-XID/LSN without beginning another native transaction. The changed-head case demands an actual successful SDK write/read; a prerequisite refusal cannot become a passed stale-head control. The additional large-head case uses two PUBLIC Core transactions in the already-bootstrapped ordinary `items` collection, through the installed native commit hook and actual quorum/native apply. Each approximately 1.5 MiB record stays under the existing 2 MiB complete-command limit. It does not send oversized events through the event API's 64 KiB limit. It demands actual Applied entries and full records on the untouched R2 voters before R1 owned-intent capture, then injects a real directory-sync failure on a genuine R1 OLD survivor whose original/candidate/envelope total exceeds the rejected 5fdb 8 MiB aggregate, obtains fresh admitted re-fsync and compares the actual R2 records/entries afterwards. A prerequisite refusal or expiry is recorded and fails the intended control; there is no workload reduction or extra deadline. These definitions are uncompiled/unexecuted.

The original 5fdb source checks covered 24 standalone Rust formatting inputs, all 26 original hot-fixture assertions and unchanged duration expressions, the unchanged native publisher body (apart from a read-only version getter), workflows and the preserved RF3/supervisor/eight later negative controls. These are source checks, not compilation.

Two actual gate component controls define same-original Controlled acceptance, foreign gate/lease refusal, and that retained comparison identity does not pin the Frozen permit or accept a later same-owner refreeze. They are not end-to-end owner-stop or cold-reopen qualification. A full close/reopen/control-clone schedule remains not tested.

Whole-head drift is refused, not rebased. The generic global uncertainty fence and synchronous whole-file IO remain; this is not R1 availability during stalled R2 IO. This batch does not complete remote Serving/controller invocation, permanent-dead-OLD replacement, source/operation registry reclamation, 64-range convergence, RPO/RTO, issue193 snapshot safety, or independent-process/VM acceptance.

## Capacity correction of frozen 5fdb

The original `5fdb895ccc5e8bc8130196be44a90e7a89c4a32f` remains preserved. Its 8 MiB sum across original/candidate/envelope was a source-confirmed failed capacity contract: an admissible roughly 3 MiB catalogue would require more than 9 MiB of those serial forms. Native applied logs retain up to 1024 commands per range; just two lawful retained native entries can produce that state, before considering a 64-range campaign. Prior custody readers permit 64 MiB heads. This child removes that narrower activation-only limit, rather than changing wire/session quotas or falling back to legacy for large protected heads. A counting-serializer boundary control uses about 1 KiB of input to exercise the existing 64 MiB serialization boundary without building a scratch catalogue-sized Vec. Compilation, execution, actual memory peaks, 64-range scale-out and large-working-dataset acceptance remain not tested.

Remote main subsequently advanced independently to `1.0.517-beta` at `bde74650c40934096e1cc6c6e75a3ea3c4643cf4`. This capacity child deliberately preserves its 5fdb/881 source base; it is not a port or validation of that main head. Final composition must audit and remove/guard the newly merged generic uncertainty-bool refsync helper using actual owned proof, and independently retain the original-budget qualification gate. No delivery version is assigned here.
