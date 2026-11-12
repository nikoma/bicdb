//! Cross-version control using an actual Native Decimal predicate generation.
//! Copy a closed old fixture before opening it with the candidate reader.
use bicdb_core::{
    index_predicate_matches, BicDb, DbConfig, IndexDefinition, IndexField, IndexKind,
    IndexPredicate, IndexPredicateBinaryOperator, IndexPredicateValueType, IndexValue, Record,
    StorageMode,
};
use serde_json::json;

fn predicate() -> IndexPredicate {
    IndexPredicate::Binary {
        operator: IndexPredicateBinaryOperator::Greater,
        left: Box::new(IndexPredicate::Field {
            field: IndexField::MetadataPath(vec!["amount".into()]),
        }),
        right: Box::new(IndexPredicate::Literal {
            value: json!("0.5"),
        }),
        value_type: IndexPredicateValueType::Decimal,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let [_, action, path] = args.as_slice() else {
        return Err("expected create|verify PATH".into());
    };
    if action != "create" && action != "verify" {
        return Err("expected create|verify PATH".into());
    }
    if action == "create" {
        assert!(!std::path::Path::new(path).exists());
    } else {
        assert!(std::path::Path::new(path).join("indexes.json").is_file());
    }
    let config = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let mut db = match BicDb::open_with_config(path, config) {
        Ok(db) => db,
        Err(error) => {
            println!(
                "{}",
                json!({"status":"open_refused","error":error.to_string(),"version":env!("CARGO_PKG_VERSION")})
            );
            return Ok(());
        }
    };
    let original = Record::new("acknowledged")
        .with_metadata(json!({"business_key":"same","amount":"0.5000000000000000000001"}));
    if action == "create" {
        db.create_collection("precision_probe")?;
        db.insert("precision_probe", original.clone())?;
        db.create_index(IndexDefinition {
            name: "native_decimal_partial".into(),
            collection: "precision_probe".into(),
            fields: vec![IndexField::MetadataPath(vec!["business_key".into()])],
            unique: true,
            nulls_not_distinct: false,
            kind: IndexKind::BTree,
            predicate: Some(predicate()),
            exclusion: None,
        })?;
    }
    let row = db.get("precision_probe", "acknowledged")?.unwrap();
    assert_eq!(row.metadata, original.metadata);
    let matches = index_predicate_matches(&row, &predicate())?;
    let found = db.lookup_index(
        "native_decimal_partial",
        &[IndexValue::String("same".into())],
    )?;
    let verification = db.verify_indexes_consistently()?;
    let before = db.scan_collection("precision_probe")?.len();
    let insert = if action == "verify" {
        match db.insert(
            "precision_probe",
            Record::new("candidate_duplicate")
                .with_metadata(json!({"business_key":"same","amount":"0.75"})),
        ) {
            Ok(_) => json!({"status":"accepted"}),
            Err(error) => json!({"status":"rejected","error":error.to_string()}),
        }
    } else {
        json!(null)
    };
    let after = db.scan_collection("precision_probe")?.len();
    let definitions = db.index_definitions();
    db.close()?;
    println!(
        "{}",
        json!({"status":"observed_and_closed","action":action,
        "version":env!("CARGO_PKG_VERSION"),"predicate_matches_acknowledged_row":matches,
        "index_ids":found,"verification":verification,"rows_before":before,
        "duplicate_attempt":insert,"rows_after":after,"definitions":definitions})
    );
    Ok(())
}
