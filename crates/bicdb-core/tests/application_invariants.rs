use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use bicdb_core::{
    BicDb, BicDbError, MutationActor, NativeCommitValidator, NativeInvariantAggregateFunction,
    NativeInvariantBinaryOperator, NativeInvariantDefinition, NativeInvariantExpression,
    NativeInvariantKind, NativeInvariantValueType, Record, Transaction,
};
use serde_json::{json, Value};

fn actor() -> MutationActor {
    MutationActor {
        actor_id: "application-test".to_string(),
        roles: BTreeSet::new(),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: "application-test".to_string(),
        originating_resource: None,
        originating_action: Some("test".to_string()),
        trace_id: "application-invariant-test".to_string(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000,
    }
}

fn subject(field: &str) -> NativeInvariantExpression {
    NativeInvariantExpression::SubjectField {
        field: field.to_string(),
    }
}

fn binding(name: &str, field: &str) -> NativeInvariantExpression {
    NativeInvariantExpression::BindingField {
        binding: name.to_string(),
        field: field.to_string(),
    }
}

fn literal(value: Value) -> NativeInvariantExpression {
    NativeInvariantExpression::Literal { value }
}

fn binary(
    operator: NativeInvariantBinaryOperator,
    left: NativeInvariantExpression,
    right: NativeInvariantExpression,
    value_type: NativeInvariantValueType,
) -> NativeInvariantExpression {
    NativeInvariantExpression::Binary {
        operator,
        left: Box::new(left),
        right: Box::new(right),
        value_type,
    }
}

fn invariant_transaction(db: &BicDb, definition: &NativeInvariantDefinition) -> Transaction {
    let mut transaction = db.begin_application_transaction(actor()).unwrap();
    transaction
        .register_commit_validator(NativeCommitValidator::ApplicationInvariant {
            definition: definition.clone(),
        })
        .unwrap();
    transaction
}

fn nonnegative_inventory() -> NativeInvariantDefinition {
    NativeInvariantDefinition {
        name: "InventoryNeverNegative".to_string(),
        subject_relation: "inventory_items".to_string(),
        kind: NativeInvariantKind::MustAlways,
        expression: binary(
            NativeInvariantBinaryOperator::GreaterEqual,
            subject("quantity"),
            literal(json!(0)),
            NativeInvariantValueType::Int64,
        ),
        dependency_relations: BTreeSet::from(["inventory_items".to_string()]),
        source: "quantity >= 0".to_string(),
    }
}

fn no_slot_overlap() -> NativeInvariantDefinition {
    NativeInvariantDefinition {
        name: "NoProviderOverlap".to_string(),
        subject_relation: "appointments".to_string(),
        kind: NativeInvariantKind::MustNever,
        expression: NativeInvariantExpression::Exists {
            binding: "other".to_string(),
            relation: "appointments".to_string(),
            condition: Box::new(binary(
                NativeInvariantBinaryOperator::And,
                binary(
                    NativeInvariantBinaryOperator::Equal,
                    binding("other", "provider_id"),
                    subject("provider_id"),
                    NativeInvariantValueType::String,
                ),
                NativeInvariantExpression::Overlaps {
                    values: vec![
                        binding("other", "start_slot"),
                        binding("other", "end_slot"),
                        subject("start_slot"),
                        subject("end_slot"),
                    ],
                    value_type: NativeInvariantValueType::Int64,
                },
                NativeInvariantValueType::Bool,
            )),
        },
        dependency_relations: BTreeSet::from(["appointments".to_string()]),
        source: "provider appointments must not overlap".to_string(),
    }
}

fn posted_entries_balance() -> NativeInvariantDefinition {
    let posting_matches_subject = binary(
        NativeInvariantBinaryOperator::Equal,
        binding("line", "journal_entry_id"),
        subject("id"),
        NativeInvariantValueType::Uuid,
    );
    let sum = NativeInvariantExpression::Aggregate {
        function: NativeInvariantAggregateFunction::Sum,
        binding: "line".to_string(),
        relation: "postings".to_string(),
        condition: Box::new(posting_matches_subject),
        field: Some("amount".to_string()),
        value_type: NativeInvariantValueType::Int64,
    };
    NativeInvariantDefinition {
        name: "EntryBalances".to_string(),
        subject_relation: "journal_entries".to_string(),
        kind: NativeInvariantKind::MustAlways,
        expression: binary(
            NativeInvariantBinaryOperator::Or,
            binary(
                NativeInvariantBinaryOperator::NotEqual,
                subject("status"),
                literal(json!("posted")),
                NativeInvariantValueType::String,
            ),
            binary(
                NativeInvariantBinaryOperator::Equal,
                sum,
                literal(json!(0)),
                NativeInvariantValueType::Int64,
            ),
            NativeInvariantValueType::Bool,
        ),
        dependency_relations: BTreeSet::from([
            "journal_entries".to_string(),
            "postings".to_string(),
        ]),
        source: "status == posted implies sum(postings.amount) == 0".to_string(),
    }
}

fn exact_decimal_arithmetic() -> NativeInvariantDefinition {
    let product = binary(
        NativeInvariantBinaryOperator::Multiply,
        subject("unit_price"),
        subject("quantity"),
        NativeInvariantValueType::Decimal,
    );
    let quotient = binary(
        NativeInvariantBinaryOperator::Divide,
        subject("total"),
        subject("quantity"),
        NativeInvariantValueType::Decimal,
    );
    NativeInvariantDefinition {
        name: "InvoiceArithmetic".to_string(),
        subject_relation: "invoice_lines".to_string(),
        kind: NativeInvariantKind::MustAlways,
        expression: binary(
            NativeInvariantBinaryOperator::And,
            binary(
                NativeInvariantBinaryOperator::Equal,
                product,
                subject("total"),
                NativeInvariantValueType::Decimal,
            ),
            binary(
                NativeInvariantBinaryOperator::Equal,
                quotient,
                subject("unit_price"),
                NativeInvariantValueType::Decimal,
            ),
            NativeInvariantValueType::Bool,
        ),
        dependency_relations: BTreeSet::from(["invoice_lines".to_string()]),
        source: "unit_price * quantity == total".to_string(),
    }
}

#[test]
fn subject_invariant_rejects_and_rolls_back_the_whole_commit() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("inventory_items").unwrap();
    let invariant = nonnegative_inventory();

    let mut rejected = invariant_transaction(&db, &invariant);
    rejected
        .insert(
            "inventory_items",
            Record::new("bad").with_metadata(json!({"quantity": -1})),
        )
        .unwrap();
    let error = rejected.commit().unwrap_err();
    assert!(matches!(error, BicDbError::CommitValidation(_)));
    assert!(db.get("inventory_items", "bad").unwrap().is_none());

    let mut accepted = invariant_transaction(&db, &invariant);
    accepted
        .insert(
            "inventory_items",
            Record::new("good").with_metadata(json!({"quantity": 0})),
        )
        .unwrap();
    accepted.commit().unwrap();
    assert!(db.get("inventory_items", "good").unwrap().is_some());
}

#[test]
fn exists_overlap_excludes_the_subject_and_accepts_adjacent_ranges() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("appointments").unwrap();
    let invariant = no_slot_overlap();

    let mut first = invariant_transaction(&db, &invariant);
    first
        .insert(
            "appointments",
            Record::new("a").with_metadata(json!({
                "provider_id": "provider-1",
                "start_slot": 10,
                "end_slot": 20
            })),
        )
        .unwrap();
    first.commit().unwrap();

    let mut adjacent = invariant_transaction(&db, &invariant);
    adjacent
        .insert(
            "appointments",
            Record::new("b").with_metadata(json!({
                "provider_id": "provider-1",
                "start_slot": 20,
                "end_slot": 30
            })),
        )
        .unwrap();
    adjacent.commit().unwrap();

    let mut overlap = invariant_transaction(&db, &invariant);
    overlap
        .insert(
            "appointments",
            Record::new("c").with_metadata(json!({
                "provider_id": "provider-1",
                "start_slot": 15,
                "end_slot": 25
            })),
        )
        .unwrap();
    assert!(matches!(
        overlap.commit(),
        Err(BicDbError::CommitValidation(_))
    ));
    assert!(db.get("appointments", "c").unwrap().is_none());
}

#[test]
fn aggregate_transition_invariant_is_deferred_and_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("journal_entries").unwrap();
    db.create_collection("postings").unwrap();
    let invariant = posted_entries_balance();

    let mut rejected = invariant_transaction(&db, &invariant);
    rejected
        .insert(
            "journal_entries",
            Record::new("entry-bad").with_metadata(json!({"status": "posted"})),
        )
        .unwrap();
    rejected
        .insert(
            "postings",
            Record::new("line-bad").with_metadata(json!({
                "journal_entry_id": "entry-bad",
                "amount": 5
            })),
        )
        .unwrap();
    assert!(matches!(
        rejected.commit(),
        Err(BicDbError::CommitValidation(_))
    ));
    assert!(db.get("journal_entries", "entry-bad").unwrap().is_none());
    assert!(db.get("postings", "line-bad").unwrap().is_none());

    let mut accepted = invariant_transaction(&db, &invariant);
    accepted
        .insert(
            "journal_entries",
            Record::new("entry-good").with_metadata(json!({"status": "posted"})),
        )
        .unwrap();
    for (id, amount) in [("debit", 5), ("credit", -5)] {
        accepted
            .insert(
                "postings",
                Record::new(id).with_metadata(json!({
                    "journal_entry_id": "entry-good",
                    "amount": amount
                })),
            )
            .unwrap();
    }
    accepted.commit().unwrap();
    assert!(db.get("journal_entries", "entry-good").unwrap().is_some());
}

#[test]
fn decimal_multiply_and_divide_match_the_application_runtime() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("invoice_lines").unwrap();
    let invariant = exact_decimal_arithmetic();

    let mut accepted = invariant_transaction(&db, &invariant);
    accepted
        .insert(
            "invoice_lines",
            Record::new("line-good").with_metadata(json!({
                "unit_price": "2.50",
                "quantity": "4",
                "total": "10.00"
            })),
        )
        .unwrap();
    accepted.commit().unwrap();

    let mut rejected = invariant_transaction(&db, &invariant);
    rejected
        .insert(
            "invoice_lines",
            Record::new("line-bad").with_metadata(json!({
                "unit_price": "2.50",
                "quantity": "4",
                "total": "9.99"
            })),
        )
        .unwrap();
    assert!(matches!(
        rejected.commit(),
        Err(BicDbError::CommitValidation(_))
    ));
}

#[test]
fn concurrent_cross_record_commits_are_serialized_through_validation() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("appointments").unwrap();
    let invariant = no_slot_overlap();

    let mut first = invariant_transaction(&db, &invariant);
    first
        .insert(
            "appointments",
            Record::new("a").with_metadata(json!({
                "provider_id": "provider-1",
                "start_slot": 10,
                "end_slot": 20
            })),
        )
        .unwrap();
    let mut second = invariant_transaction(&db, &invariant);
    second
        .insert(
            "appointments",
            Record::new("b").with_metadata(json!({
                "provider_id": "provider-1",
                "start_slot": 15,
                "end_slot": 25
            })),
        )
        .unwrap();

    let db_ref = &db;
    let (first_result, second_result) = std::thread::scope(|scope| {
        let first = scope.spawn(|| db_ref.commit_buffered_transaction(&mut first));
        let second = scope.spawn(|| db_ref.commit_buffered_transaction(&mut second));
        (first.join().unwrap(), second.join().unwrap())
    });
    let successes = [first_result.is_ok(), second_result.is_ok()]
        .into_iter()
        .filter(|success| *success)
        .count();
    assert_eq!(successes, 1, "{first_result:?} / {second_result:?}");
    assert_eq!(db.scan_collection("appointments").unwrap().len(), 1);
}
