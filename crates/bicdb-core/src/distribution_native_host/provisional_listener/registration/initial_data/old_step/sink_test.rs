//! Keyed actual pre-native-WAL cancellation; no IO/network/engine reads.
use super::*;
use std::{cell::RefCell, marker::PhantomData, rc::Rc};
enum Action {
    CancelOwn(CancellationToken),
    RevokeHook,
}
struct Pending {
    database: std::sync::Weak<RwLock<BicDb>>,
    range: crate::RangeId,
    nonce: Uuid,
    role_context: Uuid,
    action: Action,
    fired: Arc<AtomicBool>,
}
thread_local! { static PENDING:RefCell<Option<Pending>>=const{RefCell::new(None)}; }
pub(crate) struct CancelOldOwnBeforeNativeWal {
    fired: Arc<AtomicBool>,
    _thread: PhantomData<Rc<()>>,
}
impl CancelOldOwnBeforeNativeWal {
    pub(crate) fn install(
        database: &Arc<RwLock<BicDb>>,
        role: &RecoveryRoleAdmission,
        own: &CancellationToken,
    ) -> Self {
        let fired = Arc::new(AtomicBool::new(false));
        PENDING.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "nested actual OLD sink cancellation hook"
            );
            *slot.borrow_mut() = Some(Pending {
                database: Arc::downgrade(database),
                range: role.current_range().id,
                nonce: role.recovery_nonce(),
                role_context: role.context_id(),
                action: Action::CancelOwn(own.clone()),
                fired: fired.clone(),
            });
        });
        Self {
            fired,
            _thread: PhantomData,
        }
    }
    pub(crate) fn revoke_hook(database: &Arc<RwLock<BicDb>>, role: &RecoveryRoleAdmission) -> Self {
        let fired = Arc::new(AtomicBool::new(false));
        PENDING.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "nested actual OLD sink revocation hook"
            );
            *slot.borrow_mut() = Some(Pending {
                database: Arc::downgrade(database),
                range: role.current_range().id,
                nonce: role.recovery_nonce(),
                role_context: role.context_id(),
                action: Action::RevokeHook,
                fired: fired.clone(),
            });
        });
        Self {
            fired,
            _thread: PhantomData,
        }
    }
    pub(crate) fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}
impl Drop for CancelOldOwnBeforeNativeWal {
    fn drop(&mut self) {
        PENDING.with(|slot| {
            if slot
                .borrow()
                .as_ref()
                .is_some_and(|p| Arc::ptr_eq(&p.fired, &self.fired))
            {
                slot.borrow_mut().take();
            }
        });
    }
}
pub(crate) fn before_native_old_wal(
    database: &Arc<RwLock<BicDb>>,
    actual_database: &BicDb,
    role: &RecoveryRoleAdmission,
) {
    let action = PENDING.with(|slot| {
        let matches = slot.borrow().as_ref().is_some_and(|p| {
            p.range == role.current_range().id
                && p.nonce == role.recovery_nonce()
                && p.role_context == role.context_id()
                && std::sync::Weak::ptr_eq(&p.database, &Arc::downgrade(database))
        });
        if matches {
            slot.borrow_mut().take()
        } else {
            None
        }
    });
    if let Some(action) = action {
        match action.action {
            Action::CancelOwn(token) => token.cancel(),
            Action::RevokeHook => actual_database.clear_commit_admission(),
        }
        action.fired.store(true, Ordering::Release);
    }
}
