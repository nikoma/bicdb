//! Qualification-only creator. Compile against the actual old Core; never
//! synthesize legacy catalog versions or rewrite Native generation bytes.
use bicdb_core::{BicDb,DbConfig,StorageMode,Record,IndexDefinition,IndexField,IndexKind,IndexPredicate,IndexPredicateBinaryOperator,IndexPredicateUnaryOperator,IndexPredicateValueType};
use serde_json::json;
fn main() -> Result<(),Box<dyn std::error::Error>> {
    let a=std::env::args().collect::<Vec<_>>();
    let [_,root,case]=a.as_slice() else {return Err("ROOT embedded|duplicate|multiple".into())};
    assert!(!std::path::Path::new(root).exists());
    assert!(matches!(case.as_str(),"embedded"|"duplicate"|"multiple"));
    let mode=if case=="embedded" {StorageMode::EmbeddedMemory}else{StorageMode::ServerPaged};
    let config=DbConfig::default().with_fsync(true).with_storage_mode(mode).with_paged_buffer_pool_bytes(8*1024*1024);
    let mut db=BicDb::open_with_config(root,config)?;
    db.create_collection("precision_probe")?;
    db.insert("precision_probe",Record::new("acknowledged").with_metadata(json!({"business_key":"same","amount":"0.5000000000000000000001"})))?;
    if case=="duplicate" {
        db.insert("precision_probe",Record::new("second_acknowledged").with_metadata(json!({"business_key":"same","amount":"0.5000000000000000000002"})))?;
    } else if case=="multiple" {
        db.insert("precision_probe",Record::new("other_acknowledged").with_metadata(json!({"business_key":"other","amount":"0.75"})))?;
    }
    let predicate=IndexPredicate::Binary {operator:IndexPredicateBinaryOperator::Greater,left:Box::new(IndexPredicate::Field{field:IndexField::MetadataPath(vec!["amount".into()])}),right:Box::new(IndexPredicate::Literal{value:json!("0.5")}),value_type:IndexPredicateValueType::Decimal};
    let definition=IndexDefinition{name:"native_decimal_partial".into(),collection:"precision_probe".into(),fields:vec![IndexField::MetadataPath(vec!["business_key".into()])],unique:true,nulls_not_distinct:false,kind:IndexKind::BTree,predicate:Some(predicate.clone()),exclusion:None};
    db.create_index(definition.clone())?;
    if case=="multiple" {
        let mut second=definition;
        second.name="nested_decimal_partial".into();
        second.unique=false;
        second.predicate=Some(IndexPredicate::Unary{value_type:IndexPredicateValueType::Bool,operator:IndexPredicateUnaryOperator::Not,value:Box::new(IndexPredicate::Unary{value_type:IndexPredicateValueType::Bool,operator:IndexPredicateUnaryOperator::Not,value:Box::new(predicate)})});
        db.create_index(second)?;
    }
    let verification=db.verify_indexes_consistently()?;
    assert!(verification.iter().all(|report|report.valid));
    let rows=db.collection_record_count("precision_probe")?;
    let definitions=db.index_definitions();
    db.close()?;
    println!("{}",json!({"case":case,"source_version":env!("CARGO_PKG_VERSION"),"rows":rows,"definitions":definitions,"verification":verification,"status":"actual_old_store_closed"}));
    Ok(())
}
