# Registered native-base real authority expiry controls

This test-only leaf starts at `e6b429aebb94fa2e784121c54891e4ea22bb6c8a`
(unmerged source 1.0.494-beta). It has not been compiled or run. It does not fix
runtime recovery, explain the original `e1ec` four-case failure, or qualify HA.
The retained original failed cohort and later diagnostic passes stay separate.

The selector is `native_registered_fifth_remote_native_base_actual_expiry_`.
Both controls use the original five-second parent budget, actual Role/OWN
metadata windows and Role → OWN → Finished → host request order. A test cut
waits for the captured **real original Role deadline**, without cancellation,
deadline renewal, a fake clock, new metadata authority or engine I/O. Waiting
also stops on unexpected parent cancellation. The report distinguishes whether
the intended cut was reached and whether the parent remained live.

The before-put control fires inside the actual native transaction after the
original publication has retained its exact bytes, boundary and issued native
xid, before `owned.require_current` and either native put. The resulting
transaction outcome is observed through the same live database's native status
API and exact base/archive keys, with diagnostic errors kept separate. Absence
alone is not treated as an abort. It then requests a genuine fresh Role, OWN
and authenticated Finished frame while retaining the original A lease, MF and
image owner. Its desired acceptance requires that original A can finish after
a proved same-open abort. The current source appears to retain the aborted
attempt and refuse for missing committed archive; that is an expected **red
acceptance regression**, not an accepted recovery contract or runtime finding
established by an executed test.

The after-commit control uses the existing actual post-WAL commit-return cut.
It expires the original Role after the native commit has returned, then checks
that the fresh request reconciles the exact same native xid and receipt. It
must not allocate another native transaction for a committed owner.

Both cases write `registered-native-base-real-role-expiry-outcome.json` before
acceptance assertions. It contains the primary and fresh-request outcomes,
original and fresh request windows, cut observations, same-open native status,
base/archive contents, original A, raw catalogue and full local payload mapping.
The original Manifest/OLD/source-frame journal is preserved separately. A local
event-record observation is not an ordinary SQL or strong-read Serving claim.

Unknown native outcomes remain unknown and fenced. These controls never open
an archived engine, reconstruct a source process, issue a new row transaction,
change a protocol deadline or grant catalogue adoption, NEW, Open, Ready or
Serving. Native status observations here are not physical power-loss or cold
recovery qualification. Any bounded runtime retry after a proved abort remains
a separate implementation and qualification task.
