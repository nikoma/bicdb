//! Distinct actual ownership identities at CLOSED joint boundaries. The
//! Registered alternative does not contain or fabricate a WriterHostState.
use super::*;
#[derive(Clone)]
pub(crate) enum NativeJointPhaseOwner {
    Writer(Arc<NativeWriterHostState>),
    #[cfg(feature = "tls-replication")]
    Registered(std::sync::Weak<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>),
}
impl NativeJointPhaseOwner {
    pub(crate) fn same_actual(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Writer(a), Self::Writer(b)) => Arc::ptr_eq(a, b),
            #[cfg(feature = "tls-replication")]
            (Self::Registered(a), Self::Registered(b)) => std::sync::Weak::ptr_eq(a, b),
            #[cfg(feature = "tls-replication")]
            _ => false,
        }
    }
    /// Digest of exactly the identity `same_actual` compares: the kind of
    /// owner and the identity minted with that one registration object. Two
    /// owners of one process report the same digest exactly when
    /// `same_actual` holds; a replaced registration (or a restarted process)
    /// reports another. Historical DATA for a peer that cannot compare
    /// pointers; it proves nothing current and grants nothing.
    pub(crate) fn identity_sha256(&self) -> Result<String> {
        let (kind, identity) = match self {
            Self::Writer(writer) => ("writer", writer.registration_identity()),
            #[cfg(feature = "tls-replication")]
            Self::Registered(owner) => (
                "registered",
                owner
                    .upgrade()
                    .ok_or_else(|| invalid("original Registered phase owner ended"))?
                    .registration_identity(),
            ),
        };
        Ok(hex::encode(sha2::Sha256::digest(
            format!("native-joint-phase-owner-identity-v1\n{kind}\n{identity}").as_bytes(),
        )))
    }
    /// Legacy getter remains strict. Receiver identity is never a writer.
    pub(crate) fn actual_writer(&self) -> Result<&Arc<NativeWriterHostState>> {
        match self {
            Self::Writer(writer) => Ok(writer),
            #[cfg(feature = "tls-replication")]
            Self::Registered(_) => Err(invalid(
                "original Registered joint owner is not an installed writer",
            )),
        }
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn borrow_registered_identity<'a>(
        &self,
        db: &'a BicDb,
    ) -> Result<Option<NativeRegisteredJointIdentityBorrow<'a>>> {
        match self {
            Self::Writer(_) => Ok(None),
            Self::Registered(owner) => {
                let owner = owner
                    .upgrade()
                    .ok_or_else(|| invalid("original Registered phase owner ended"))?;
                db.borrow_registered_joint_identity(owner.target().clone())
                    .map(Some)
            }
        }
    }
    pub(crate) fn require_store(&self, root: &Path, store: Uuid) -> Result<()> {
        match self {
            Self::Writer(writer) => writer.require_store(root, store),
            #[cfg(feature = "tls-replication")]
            Self::Registered(owner) => {
                let owner = owner
                    .upgrade()
                    .ok_or_else(|| invalid("original Registered phase owner ended"))?;
                owner.target().require_joint_lifetime(
                    root,
                    store,
                    owner.target().base().intended_activation_range(),
                )
            }
        }
    }
}
