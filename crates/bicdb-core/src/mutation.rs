//! Native mutation authority enforced by the core transaction path.
//!
//! Grants are issued only by trusted native application hosts. WASM sees a
//! separate invocation-local handle maintained by the capability broker, never
//! this identifier or the grant contents.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MutationOperation {
    Insert,
    Update,
    Delete,
    Restore,
    Bulk,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MutationGrantId(pub(crate) u64);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MutationActor {
    pub actor_id: String,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    pub originating_plugin: String,
    #[serde(default)]
    pub originating_resource: Option<String>,
    #[serde(default)]
    pub originating_action: Option<String>,
    pub trace_id: String,
    pub deadline_unix_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MutationGrantSpec {
    pub relation: String,
    pub operation: MutationOperation,
    #[serde(default)]
    pub record_id: Option<String>,
    #[serde(default)]
    pub record_id_prefix: Option<String>,
    #[serde(default)]
    pub expected_version: Option<u64>,
    #[serde(default)]
    pub version_field: Option<String>,
    #[serde(default)]
    pub allowed_columns: BTreeSet<String>,
    #[serde(default)]
    pub bulk: bool,
    pub maximum_affected_rows: u64,
    #[serde(default)]
    pub cascade_relations: BTreeSet<String>,
    #[serde(default = "one_statement")]
    pub statement_budget: u32,
    #[serde(default)]
    pub tenant_field: Option<String>,
    #[serde(default)]
    pub workspace_field: Option<String>,
    #[serde(default)]
    pub audit_metadata: BTreeMap<String, String>,
}

fn one_statement() -> u32 {
    1
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MutationPolicy {
    #[serde(default)]
    pub grants_required: bool,
    #[serde(default)]
    pub append_only: bool,
    #[serde(default)]
    pub immutable_fields: BTreeSet<String>,
    #[serde(default)]
    pub version_field: Option<String>,
    #[serde(default)]
    pub tenant_field: Option<String>,
    #[serde(default)]
    pub workspace_field: Option<String>,
    #[serde(default)]
    pub audit_required: bool,
}

impl MutationPolicy {
    pub fn grants_required() -> Self {
        Self {
            grants_required: true,
            ..Self::default()
        }
    }

    pub fn append_only(mut self) -> Self {
        self.append_only = true;
        self
    }

    pub fn with_immutable_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.immutable_fields = fields.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_version_field(mut self, field: impl Into<String>) -> Self {
        self.version_field = Some(field.into());
        self
    }

    pub fn with_tenant_field(mut self, field: impl Into<String>) -> Self {
        self.tenant_field = Some(field.into());
        self
    }

    pub fn with_workspace_field(mut self, field: impl Into<String>) -> Self {
        self.workspace_field = Some(field.into());
        self
    }

    pub fn with_audit(mut self) -> Self {
        self.audit_required = true;
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MutationGrant {
    pub(crate) id: MutationGrantId,
    pub(crate) actor: MutationActor,
    pub(crate) spec: MutationGrantSpec,
    pub(crate) remaining_rows: u64,
    pub(crate) remaining_statements: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeCommitValidator {
    AppendOnly {
        relation: String,
    },
    ImmutableFields {
        relation: String,
        fields: BTreeSet<String>,
    },
    TenantWorkspaceImmutable {
        relation: String,
        #[serde(default)]
        tenant_field: Option<String>,
        #[serde(default)]
        workspace_field: Option<String>,
    },
    OptimisticVersion {
        relation: String,
        field: String,
    },
    LedgerBalanced {
        relation: String,
        subject_field: String,
        debit_field: String,
        credit_field: String,
    },
    AggregateInvariant {
        relation: String,
        subject_field: String,
        value_field: String,
        minimum: f64,
        maximum: f64,
    },
    FinanceGuard {
        relation: String,
        amount_field: String,
        maximum_absolute_amount: f64,
    },
    #[serde(alias = "carrier_invariant")]
    ApplicationInvariant {
        definition: NativeInvariantDefinition,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NativeInvariantDefinition {
    pub name: String,
    pub subject_relation: String,
    pub kind: NativeInvariantKind,
    pub expression: NativeInvariantExpression,
    pub dependency_relations: BTreeSet<String>,
    pub source: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeInvariantKind {
    MustAlways,
    MustNever,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeInvariantValueType {
    Bool,
    Int64,
    Float64,
    Decimal,
    String,
    Uuid,
    Timestamp,
    Date,
    Json,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeInvariantUnaryOperator {
    Not,
    Negate,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeInvariantBinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    And,
    Or,
    Implies,
    Contains,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeInvariantAggregateFunction {
    Count,
    Sum,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeInvariantExpression {
    SubjectField {
        field: String,
    },
    BindingField {
        binding: String,
        field: String,
    },
    Literal {
        value: serde_json::Value,
    },
    Unary {
        operator: NativeInvariantUnaryOperator,
        value: Box<NativeInvariantExpression>,
        value_type: NativeInvariantValueType,
    },
    Binary {
        operator: NativeInvariantBinaryOperator,
        left: Box<NativeInvariantExpression>,
        right: Box<NativeInvariantExpression>,
        value_type: NativeInvariantValueType,
    },
    Overlaps {
        values: Vec<NativeInvariantExpression>,
        value_type: NativeInvariantValueType,
    },
    Exists {
        binding: String,
        relation: String,
        condition: Box<NativeInvariantExpression>,
    },
    Aggregate {
        function: NativeInvariantAggregateFunction,
        binding: String,
        relation: String,
        condition: Box<NativeInvariantExpression>,
        field: Option<String>,
        value_type: NativeInvariantValueType,
    },
}
