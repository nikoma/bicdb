//! Atomic hook identity; a registration is never quorum or write authority.
use super::*;

#[derive(Debug)]
pub(in crate::db) struct CommitAdmissionRegistration {
    generation: uuid::Uuid,
    authority: Option<std::sync::Weak<dyn CommitAdmission>>,
    active: std::sync::atomic::AtomicBool,
}
impl CommitAdmissionRegistration {
    pub(in crate::db) fn new(authority: Option<&Arc<dyn CommitAdmission>>) -> Arc<Self> {
        Arc::new(Self {
            generation: uuid::Uuid::now_v7(),
            authority: authority.map(Arc::downgrade),
            active: std::sync::atomic::AtomicBool::new(false),
        })
    }
    pub(in crate::db) fn is_active(&self) -> bool {
        self.active.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(in crate::db) fn generation(&self) -> uuid::Uuid {
        self.generation
    }
    pub(in crate::db) fn authority(&self) -> Option<Arc<dyn CommitAdmission>> {
        self.authority.as_ref().and_then(std::sync::Weak::upgrade)
    }
}

#[derive(Debug)]
pub(super) struct CommitAdmissionSlot {
    authority: Option<Arc<dyn CommitAdmission>>,
    registration: Arc<CommitAdmissionRegistration>,
    native_writer: Option<Arc<super::range_image_native::NativeWriterHostState>>,
}
impl CommitAdmissionSlot {
    pub(super) fn empty() -> Self {
        Self {
            authority: None,
            registration: CommitAdmissionRegistration::new(None),
            native_writer: None,
        }
    }
    pub(super) fn install(&mut self, authority: Option<Arc<dyn CommitAdmission>>) {
        let registration = CommitAdmissionRegistration::new(authority.as_ref());
        self.replace(authority, registration, None);
    }
    pub(in crate::db) fn install_enrolled(
        &mut self,
        authority: Arc<dyn CommitAdmission>,
        registration: Arc<CommitAdmissionRegistration>,
        state: Arc<super::range_image_native::NativeWriterHostState>,
    ) {
        self.replace(Some(authority), registration, Some(state));
    }
    /// Sealed installer publication keeps every retired strong owner alive
    /// until AFTER its final metadata guard. This method only swaps memory.
    pub(in crate::db) fn replace_enrolled_retaining_old(
        &mut self,
        authority: Arc<dyn CommitAdmission>,
        registration: Arc<CommitAdmissionRegistration>,
        state: Arc<super::range_image_native::NativeWriterHostState>,
    ) -> Self {
        self.registration
            .active
            .store(false, std::sync::atomic::Ordering::Release);
        let next = Self {
            authority: Some(authority),
            registration,
            native_writer: Some(state),
        };
        next.registration
            .active
            .store(true, std::sync::atomic::Ordering::Release);
        std::mem::replace(self, next)
    }
    fn replace(
        &mut self,
        authority: Option<Arc<dyn CommitAdmission>>,
        registration: Arc<CommitAdmissionRegistration>,
        state: Option<Arc<super::range_image_native::NativeWriterHostState>>,
    ) {
        self.registration
            .active
            .store(false, std::sync::atomic::Ordering::Release);
        self.authority = authority;
        self.registration = registration;
        self.native_writer = state;
        self.registration.active.store(
            self.authority.is_some(),
            std::sync::atomic::Ordering::Release,
        );
    }
    pub(in crate::db) fn deactivate_native_writer(&self) {
        self.registration
            .active
            .store(false, std::sync::atomic::Ordering::Release);
    }
    pub(in crate::db) fn native_writer(
        &self,
    ) -> Option<&Arc<super::range_image_native::NativeWriterHostState>> {
        self.native_writer.as_ref()
    }
    pub(in crate::db) fn registration(&self) -> &Arc<CommitAdmissionRegistration> {
        &self.registration
    }
}
impl std::ops::Deref for CommitAdmissionSlot {
    type Target = Option<Arc<dyn CommitAdmission>>;
    fn deref(&self) -> &Self::Target {
        &self.authority
    }
}
