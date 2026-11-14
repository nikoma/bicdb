//! Actual born-database registration lifecycle components. These do not
//! construct a Registered image/NEW grant or qualify a relocation.
use super::*;

#[test]
fn native_registered_empty_hook_current_memory_check_preserves_inactive_semantics() {
    let (_root, db, _, _) = fresh();
    let database = db.read();
    let original = database.capture_registered_receiver_hook().unwrap();
    let registration = original.registration.upgrade().unwrap();
    assert!(!registration.is_active());
    assert!(!registration.is_revoked());
    original.require_current(&database).unwrap();
    original.require_remote_new_registration_memory().unwrap();

    // The real enrollment path fences in place before fallible native work.
    // On an empty slot this must not retire the original empty identity.
    database.commit_admission.read().deactivate_native_writer();
    assert!(!registration.is_active());
    assert!(!registration.is_revoked());
    original.require_current(&database).unwrap();
    original.require_remote_new_registration_memory().unwrap();
}

#[test]
fn native_registered_empty_hook_none_replacement_cannot_reactivate_original() {
    let (_root, db, _, _) = fresh();
    let database = db.read();
    let original = database.capture_registered_receiver_hook().unwrap();
    // Keep the ACTUAL original registration alive: a stale Weak must not pass
    // merely because a fixture/retained owner still holds this allocation.
    let registration = original.registration.upgrade().unwrap();
    let generation = registration.generation();
    for _ in 0..2 {
        database.clear_commit_admission();
        assert!(original.registration.upgrade().is_some());
        assert!(registration.is_revoked());
        assert!(!registration.is_active());
        assert_eq!(registration.generation(), generation);
        assert!(original.require_current(&database).is_err());
        assert!(original.require_remote_new_registration_memory().is_err());
        let current = database.capture_registered_receiver_hook().unwrap();
        assert_ne!(
            current.registration.upgrade().unwrap().generation(),
            generation
        );
        current.require_current(&database).unwrap();
        current.require_remote_new_registration_memory().unwrap();
    }
}

#[test]
fn native_registered_empty_hook_install_then_clear_keeps_original_revoked() {
    let (_root, db, _, _) = fresh();
    let database = db.read();
    let original = database.capture_registered_receiver_hook().unwrap();
    let registration = original.registration.upgrade().unwrap();
    // Public trusted-hook installation is a real slot replacement, not native
    // writer enrollment and never a source of a native writer capability.
    database.install_commit_admission(Arc::new(NeverAdmits));
    assert!(database.commit_admission.read().registration().is_active());
    assert!(database.capture_registered_receiver_hook().is_err());
    assert!(registration.is_revoked());
    assert!(original.require_current(&database).is_err());
    assert!(original.require_remote_new_registration_memory().is_err());

    database.clear_commit_admission();
    let current = database.capture_registered_receiver_hook().unwrap();
    current.require_current(&database).unwrap();
    current.require_remote_new_registration_memory().unwrap();
    assert!(registration.is_revoked());
    assert!(original.require_current(&database).is_err());
    assert!(original.require_remote_new_registration_memory().is_err());
}

#[test]
fn native_registered_empty_hook_database_drop_ends_open_and_revokes_retained_slot() {
    let (_root, db, _, _) = fresh();
    let (original, registration) = {
        let database = db.read();
        let original = database.capture_registered_receiver_hook().unwrap();
        let registration = original.registration.upgrade().unwrap();
        original.require_remote_new_registration_memory().unwrap();
        (original, registration)
    };
    assert_eq!(Arc::strong_count(&db), 1);
    drop(db);
    // These are two distinct actual-lifecycle facts, not a manufactured open
    // flag: slot Drop is monotonic even while the old registration survives.
    assert!(original.actual_open.upgrade().is_none());
    assert!(original.registration.upgrade().is_some());
    assert!(registration.is_revoked());
    assert!(!registration.is_active());
    assert!(original.require_remote_new_registration_memory().is_err());
}
