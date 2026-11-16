# Source7e99: arming a retained uncertain catalogue control

This cfg(test)-only correction is based on exact public `7e99c9c96970b78a44402c975627c64e001adffa`. It changes no production method, test assertion, deadline, fault site or IO implementation. No Rust compilation or test has been run for this unit.

## Original reached failure

Native job132883862065/run43737698638 failed overall396PASS/55FAIL/1IGNORE. In `native_first_remote_catalogue_actual_intervening_io_cannot_clear_old_uncertainty`, the complete canonical raw records:

- Line30203: the actual target catalogue call returned named directory-sync EIO.
- Line30205: fault count1, one original failed IO outcome at generation4, original preimage generation3, pending generation4, `owns_uncertainty=true`, no verified generation.
- Lines30207 and30210: visible-byte observation and wrong-original-recipient resolution were refused. Line30212 retains unchanged original native/catalogue custody after those refusals.
- Line30213: the next diagnostic command failed with `recovery catalog persistence is uncertain; re-fsync the exact fenced session before claiming durability`. The intended intervening IO and superseded-owner assertions were not reached.

The failure is a control-admission error. `test_first_native_decision(..., Some(7))` invokes `test_arm_remote_native_decision_cut`, which calls the ordinary `registered_remote_joint_owner`. That getter deliberately revalidates actual native identity and correctly refuses an uncertain catalogue. Adjacent `test_remote_native_decision_evidence` already reads retained receiver/table/phase custody in memory for exactly this state; it issues no owner or permission.

The original failure remains preserved in the frozen7e99 archive. Native raw SHA256 is `b59d517171ecf60921177338f5593bef6422806caeecdb72ecd005f2a866c8db`; gzip SHA256 is `7b7df7c518c498dd6409604b548791ae9fd62c1620865c829d3b906557d9542c`. This does not identify the cause of the historical Azure incident or establish data loss.

## Narrow scheduling-only correction

Only fault codes7 and8 use the memory-only retained receiver owner table and its same existing `remote_new` phase. Both table and phase transition guards are dropped before calling the existing participant's arming method. No new owner/phase is constructed, no original state is revived, and no owner handle, Role, OWN, receipt or Ready grant is returned. Missing/busy original custody still refuses.

Code7 continues into the unchanged `test_arm_intervening_write`: it requires actual owned uncertainty, a pending generation, no verified generation and no prior arm or intervening write. Code8 continues into the unchanged corresponding foreign-disk arm with analogous pending-uncertainty checks. The branch does not bypass these conditions or perform IO itself.

Codes1–6,9 and10, other codes and every ordinary getter retain the original strict route unchanged. The ordinary getter must continue to refuse the uncertain Store. All existing test bodies, original EIO and observation-refusal assertions, native/XID/catalogue comparisons, actual IO counts/generation checks, superseded refusal, and later SDK/Strong assertions remain byte-identical. Code8 receives the same narrowly scoped correction; no successful execution of that case is inferred from another case's evidence.

## Why the later real IO can execute without clearing uncertainty

The subsequent original `ResumeCatalogueMember` remains a genuine fresh authenticated request. Production ingress retains the same original participant and uses the existing distinct catalogue admission with actual Role/OWN/MStore and known original native D. `NativeRemoteCataloguePhysicalPoint` validates the retained physical Registered identity, actual known native result and current metadata; its purpose-specific identity path explicitly does not turn generic catalogue uncertainty into health. No generic getter is weakened.

Only after those unchanged checks does the existing test injection in `NativeRemoteDecisionCataloguePublication::advance` call `store.persist()`. That is the existing full generic persistence method: it validates the actual native writer base/reader fence, builds and serializes the real current catalogue, enters the same actual IO generation, calls the real atomic writer, records the actual successful return, and checks the native writer base again. The native-base predicate is independent of the catalogue uncertainty flag; it still refuses if its actual profile/lifetime/fences are invalid. This is not a generation setter or an IO result invented by the control.

`persist()` borrows the Store immutably and does not clear `recovery_persistence_uncertain`. The old publication retains `owns_uncertainty` and its original pending generation. `intervening_write_count` increments only after that same real IO returns Ok. The immediately following unchanged `require_pending` compares the newer actual Store generation against the old pending generation and refuses `superseded; no stale rewrite`, before the old candidate can replace the file or clear uncertainty. Its original failed IO outcome is retained; the generic intervening write is not misreported as an old-owner successful IO outcome. A failed new persistence call remains an actual failure, not a successful test cut.

The same-generation foreign-disk control retains its original distinct physical-preimage checks. Neither test route manufactures healthy currentness from visible bytes.

## Validation and limits

Standalone formatting, diff checks and source-preservation checks are the only validations. The correction changes one method inside an existing cfg(test) impl plus this document. Existing production bodies, the entire FIRST test file, participant arming checks, actual persistence/generation methods, physical native scope, ordinary getter and sink are unchanged. No new retry, permission path, uncertainty reset, larger deadline or weaker expected result is introduced.

The integrated normal CI must still reach the exact original EIO, memory-only arm, real intervening IO, and superseded refusal under the original windows. This source-only control correction is not runtime, physical HA, cold recovery or complete protocol qualification.
