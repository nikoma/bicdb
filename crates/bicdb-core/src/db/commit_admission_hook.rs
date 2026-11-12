//! Atomic hook identity; a registration is never quorum or write authority.
use super::*;

#[derive(Debug)]
pub(in crate::db) struct CommitAdmissionRegistration {
    generation: uuid::Uuid,
    authority: Option<std::sync::Weak<dyn CommitAdmission>>,
}
impl CommitAdmissionRegistration {
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
}
impl CommitAdmissionSlot {
    pub(super) fn empty() -> Self {
        Self {
            authority: None,
            registration: Arc::new(CommitAdmissionRegistration {
                generation: uuid::Uuid::now_v7(),
                authority: None,
            }),
        }
    }
    pub(super) fn install(&mut self, authority: Option<Arc<dyn CommitAdmission>>) {
        // Each call creates a distinct generation, including same-Arc reinstall
        // or repeated clear. Old weak guards can never become current again.
        self.registration = Arc::new(CommitAdmissionRegistration {
            generation: uuid::Uuid::now_v7(),
            authority: authority.as_ref().map(Arc::downgrade),
        });
        self.authority = authority;
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
