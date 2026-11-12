//! Correlated read DATA and routing hints. Decoding any reply grants neither a
//! metadata/data quorum nor a local floor or snapshot permission.
use super::*;
use crate::{ClusterEventOperation, ClusterEventRequest};
use serde_json::Value;

pub(crate) const NATIVE_EVENT_GET_VERSION: u32 = 1;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NativeEventReadMode {
    Strong,
    DeploymentDefault,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeEventGetRequest {
    pub version: u32,
    pub mode: NativeEventReadMode,
    pub business_request_id: Uuid,
    pub challenge: Uuid,
    pub range_id: crate::RangeId,
    pub range_epoch: u64,
    pub namespace: String,
    pub key: String,
    pub budget_ms: u64,
}
impl NativeEventGetRequest {
    pub(crate) fn validate(&self) -> Result<String> {
        if self.version != NATIVE_EVENT_GET_VERSION
            || self.business_request_id.is_nil()
            || self.challenge.is_nil()
            || self.range_epoch == 0
            || self.budget_ms == 0
            || self.budget_ms > 5000
        {
            return Err(invalid("invalid bounded correlated event Get"));
        }
        crate::cluster_event_record_id(&self.namespace, &self.key)
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.validate()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
    pub(crate) fn legacy_request(&self) -> ClusterEventRequest {
        ClusterEventRequest {
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            operation: ClusterEventOperation::Get {
                namespace: self.namespace.clone(),
                key: self.key.clone(),
            },
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeEventRoute {
    pub generation: u64,
    pub range_id: crate::RangeId,
    pub range_epoch: u64,
    pub leader: ClusterNodeId,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NativeEventGetUnavailableCode {
    LegacyUnsupported,
    MissingDispatcher,
    MissingWriter,
    Busy,
    MetadataUnavailable,
    NativeRead { stage: NativeStrongReadStage },
    Canceled,
    InvalidRequest,
}
/// Explicit presence preserves present JSON null across serialization. This
/// is returned DATA, never a floor/quorum/role permission.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "presence", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NativeEventValue {
    Present { value: Value },
    Absent,
}
impl From<Option<Value>> for NativeEventValue {
    fn from(value: Option<Value>) -> Self {
        match value {
            Some(value) => Self::Present { value },
            None => Self::Absent,
        }
    }
}
impl NativeEventValue {
    pub(crate) fn into_option(self) -> Option<Value> {
        match self {
            Self::Present { value } => Some(value),
            Self::Absent => None,
        }
    }
    fn as_ref(&self) -> Option<&Value> {
        match self {
            Self::Present { value } => Some(value),
            Self::Absent => None,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NativeEventGetResult {
    Value {
        value: NativeEventValue,
        route: NativeEventRoute,
        audit: NativeStrongPointReadAudit,
    },
    Redirect {
        route: NativeEventRoute,
        metadata_context_id: Uuid,
        metadata_term: u64,
        metadata_read_index: u64,
    },
    Unavailable {
        code: NativeEventGetUnavailableCode,
        route: Option<NativeEventRoute>,
        retry_after_ms: u64,
        detail: String,
    },
    /// Explicit actual born-v1 behavior. Never a Strong result or an inferred
    /// downgrade after a protocol/typed-request error.
    LegacyValue { value: NativeEventValue },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeEventGetReply {
    pub version: u32,
    pub business_request_id: Uuid,
    pub challenge: Uuid,
    pub result: NativeEventGetResult,
}
impl NativeEventGetReply {
    pub(crate) fn new(request: &NativeEventGetRequest, result: NativeEventGetResult) -> Self {
        Self {
            version: NATIVE_EVENT_GET_VERSION,
            business_request_id: request.business_request_id,
            challenge: request.challenge,
            result,
        }
    }
    pub(crate) fn unavailable(
        request: &NativeEventGetRequest,
        code: NativeEventGetUnavailableCode,
        route: Option<NativeEventRoute>,
        detail: impl std::fmt::Display,
    ) -> Self {
        Self::new(
            request,
            NativeEventGetResult::Unavailable {
                code,
                route,
                retry_after_ms: 50,
                detail: detail.to_string().chars().take(2048).collect(),
            },
        )
    }
    pub(crate) fn validate_for(&self, request: &NativeEventGetRequest) -> Result<()> {
        request.validate()?;
        if self.version != NATIVE_EVENT_GET_VERSION
            || self.business_request_id != request.business_request_id
            || self.challenge != request.challenge
        {
            return Err(invalid("event Get reply correlation differs"));
        }
        let route = |r: &NativeEventRoute| -> Result<()> {
            if r.generation == 0
                || r.range_epoch == 0
                || r.leader.as_str().is_empty()
                || r.leader.as_str().len() > 1024
            {
                return Err(invalid("invalid event route data"));
            }
            Ok(())
        };
        let digest = |h: &str| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit());
        match &self.result {
            NativeEventGetResult::Value {
                value,
                route: r,
                audit,
            } => {
                route(r)?;
                if r.range_id != request.range_id
                    || r.range_epoch != request.range_epoch
                    || audit.business_request_id != request.business_request_id
                    || audit.challenge != request.challenge
                    || audit.range_id != r.range_id
                    || audit.range_epoch != r.range_epoch
                    || audit.final_metadata_generation != r.generation
                    || audit.first_metadata_context_id.is_nil()
                    || audit.final_metadata_context_id.is_nil()
                    || audit.first_metadata_context_id == audit.final_metadata_context_id
                    || audit.final_metadata_term == 0
                    || audit.final_metadata_read_index == 0
                    || audit.snapshot_sequence < audit.target_local_visibility_db_sequence
                    || !digest(&audit.logical_genesis_sha256)
                    || !digest(&audit.logical_history_sha256)
                {
                    return Err(invalid("invalid actual Strong event result audit"));
                }
                crate::distribution_range_consensus::validate_native_json_size(
                    &value.as_ref(),
                    crate::MAX_CLUSTER_EVENT_BYTES,
                )?;
            }
            NativeEventGetResult::Redirect {
                route: r,
                metadata_context_id,
                metadata_term,
                metadata_read_index,
            } => {
                route(r)?;
                if metadata_context_id.is_nil() || *metadata_term == 0 || *metadata_read_index == 0
                {
                    return Err(invalid("invalid fresh HostMQ routing observation"));
                }
            }
            NativeEventGetResult::Unavailable {
                route: r,
                retry_after_ms,
                detail,
                ..
            } => {
                if let Some(r) = r {
                    route(r)?;
                }
                if *retry_after_ms > 1000 || detail.len() > 8192 {
                    return Err(invalid("invalid bounded event retry data"));
                }
            }
            NativeEventGetResult::LegacyValue { value } => {
                if request.mode == NativeEventReadMode::Strong {
                    return Err(invalid(
                        "explicit Strong Get cannot accept a legacy value/absence",
                    ));
                }
                crate::distribution_range_consensus::validate_native_json_size(
                    &value.as_ref(),
                    crate::MAX_CLUSTER_EVENT_BYTES,
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_event_get_presence_roundtrip_never_turns_present_null_or_object_into_absence() {
        for value in [Some(Value::Null), Some(serde_json::json!({})), None] {
            let bytes = serde_json::to_vec(&NativeEventValue::from(value.clone())).unwrap();
            let decoded: NativeEventValue = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(decoded.into_option(), value);
        }
        let present =
            serde_json::to_value(NativeEventValue::Present { value: Value::Null }).unwrap();
        let absent = serde_json::to_value(NativeEventValue::Absent).unwrap();
        assert_ne!(present, absent);
        assert!(serde_json::from_value::<NativeEventValue>(
            serde_json::json!({"presence":"present"})
        )
        .is_err());
        assert!(serde_json::from_value::<NativeEventValue>(
            serde_json::json!({"presence":"absent","value":null})
        )
        .is_err());
    }
    #[test]
    fn native_event_get_explicit_strong_rejects_legacy_and_foreign_correlation_projection() {
        // Codec/negative only: these DTOs are NOT metadata/data authority.
        let request = NativeEventGetRequest {
            version: 1,
            mode: NativeEventReadMode::Strong,
            business_request_id: Uuid::now_v7(),
            challenge: Uuid::now_v7(),
            range_id: crate::RangeId::new(1).unwrap(),
            range_epoch: 1,
            namespace: "tenant".into(),
            key: "key".into(),
            budget_ms: 1000,
        };
        let legacy = NativeEventGetReply::new(
            &request,
            NativeEventGetResult::LegacyValue {
                value: NativeEventValue::Absent,
            },
        );
        assert!(legacy.validate_for(&request).is_err());
        let mut default = request.clone();
        default.mode = NativeEventReadMode::DeploymentDefault;
        legacy.validate_for(&default).unwrap();
        default.challenge = Uuid::now_v7();
        assert!(legacy.validate_for(&default).is_err());
    }
}
