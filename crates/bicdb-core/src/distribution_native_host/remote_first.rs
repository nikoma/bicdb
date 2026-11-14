//! Finite FIRST protocol DATA. A request never constructs a host, Role, OLD
//! certificate, image lease, quorum or Serving permission.
use super::*;
use crate::distribution::{RangeId, RelocationId};
use crate::distribution_range_image::NativeRemoteImageSourceOperationV1;
use parking_lot::Mutex;
use std::time::Instant;

mod ingress;
pub(crate) use ingress::{NativeFirstReceiveIngress, NativeFirstReceiveQueue};

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) enum NativeFirstReceiveOperationV1 {
    ObserveLast { sequence: u64 },
    Enroll,
    Start,
    Describe,
    Transfer,
    Proposal,
    Installing,
    BeginHistory,
    AppendHistory,
    AuditHistory { max_entries: usize },
    ApplyRows,
    BeginVerification,
    VerifyTarget { max_scanned_records: usize },
    PublishFinalImage,
    PublishNativeBase,
    AdoptCatalogueFloor,
    Materialize,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeFirstReceiveRequestV1 {
    pub version: u32,
    pub relocation: RelocationId,
    pub range: RangeId,
    pub epoch: u64,
    pub session: Uuid,
    pub nonce: Uuid,
    /// Correlation allocated ONLY by the real coordinator host factory. The
    /// UUID is not cryptographic OS-process evidence or mutation authority.
    pub original_operation: Uuid,
    pub sequence: u64,
    pub source: ClusterNodeId,
    pub target_attempt: Option<Uuid>,
    pub operation: NativeFirstReceiveOperationV1,
    pub source_operation: Option<NativeRemoteImageSourceOperationV1>,
    pub business: Uuid,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeFirstReceiveRequestV1 {
    pub(crate) fn require_shape(&self) -> Result<()> {
        crate::distribution_range_image::bounded_json(self, 16 * 1024)?;
        if self.version != 1
            || self.relocation.get() == 0
            || self.range.get() == 0
            || self.epoch == 0
            || self.session.is_nil()
            || self.nonce.is_nil()
            || self.original_operation.is_nil()
            || self.sequence == 0
            || self.business.is_nil()
            || self.challenge.is_nil()
            || self.budget_ms == 0
            || self.budget_ms > 5000
            || self.target_attempt.is_some_and(|a| a.is_nil())
            || (self.target_attempt.is_none()
                && !matches!(
                    self.operation,
                    NativeFirstReceiveOperationV1::Enroll
                        | NativeFirstReceiveOperationV1::ObserveLast { .. }
                ))
            || matches!(self.operation, NativeFirstReceiveOperationV1::ObserveLast { sequence } if sequence==0 || sequence>=self.sequence)
            || matches!(self.operation, NativeFirstReceiveOperationV1::AuditHistory { max_entries } if max_entries==0 || max_entries>4096)
            || matches!(self.operation, NativeFirstReceiveOperationV1::VerifyTarget { max_scanned_records } if max_scanned_records==0 || max_scanned_records>4096)
            || matches!(
                self.source_operation,
                Some(NativeRemoteImageSourceOperationV1::Manifest)
            )
        {
            return Err(invalid("invalid bounded FIRST receive request DATA"));
        }
        Ok(())
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.require_shape()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeFirstReceiveReplyV1 {
    pub request: NativeFirstReceiveRequestV1,
    pub target: ClusterNodeId,
    pub birth: crate::distribution::NativeStoreBirthV1,
    pub target_attempt: Option<Uuid>,
    /// Result DATA only; never a remote quorum or writer permission.
    pub result: serde_json::Value,
}

/// Streaming DATA projection after the actual operation returned. Retention is
/// bounded even if a source formats many chunks; incomplete UTF-8 prefixes
/// never resume with a later chunk. The complete Display stream is hashed.
pub(in crate::distribution_native_host) fn error_data(error: &BicDbError) -> serde_json::Value {
    struct Projection {
        prefix: String,
        bytes: u64,
        incomplete: bool,
        hash: Sha256,
    }
    impl std::fmt::Write for Projection {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            self.hash.update(value.as_bytes());
            self.bytes = self.bytes.saturating_add(value.len() as u64);
            if !self.incomplete {
                let remaining = 512usize.saturating_sub(self.prefix.len());
                if value.len() <= remaining {
                    self.prefix.push_str(value);
                } else {
                    let mut end = remaining;
                    while !value.is_char_boundary(end) {
                        end -= 1;
                    }
                    self.prefix.push_str(&value[..end]);
                    self.incomplete = true;
                }
            }
            Ok(())
        }
    }
    let mut p = Projection {
        prefix: String::new(),
        bytes: 0,
        incomplete: false,
        hash: Sha256::new(),
    };
    let formatted = std::fmt::write(&mut p, format_args!("{error}")).is_ok();
    serde_json::json!({"display_prefix":p.prefix,"display_bytes":p.bytes,
        "display_sha256":hex::encode(p.hash.finalize()),"prefix_incomplete":p.incomplete,
        "display_format_complete":formatted,"retained_error_or_authority":false})
}
