//! PostgreSQL partition bounds and physical relation identities. Oracle's
//! partition metadata and routing are deliberately handled by its own module.
use crate::*;
mod hash;

#[derive(Clone, Debug)]
pub(crate) enum BoundValue {
    Min,
    Value(SqlValue),
    Max,
}

#[derive(Clone, Debug)]
pub(crate) enum Bound {
    Default,
    Range(Vec<BoundValue>, Vec<BoundValue>),
    List(Vec<SqlValue>),
    Hash { modulus: u64, remainder: u64 },
}

fn invalid(message: impl Into<String>) -> SqlError {
    SqlError::InvalidSql(message.into())
}

pub(crate) fn violation(table: &str, message: impl Into<String>) -> SqlError {
    SqlError::ConstraintViolation {
        sqlstate: "23514",
        message: message.into(),
        schema: None,
        table: Some(table.into()),
        column: None,
        constraint: None,
        oracle_code: None,
        unique_origin: None,
    }
}

struct Tokens {
    values: Vec<Token>,
    pos: usize,
}

impl Tokens {
    fn new(sql: &str) -> Result<Self> {
        let values = Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize()
            .map_err(|e| invalid(e.to_string()))?
            .into_iter()
            .filter(|token| !matches!(token, Token::Whitespace(_)))
            .collect();
        Ok(Self { values, pos: 0 })
    }

    fn keyword(&mut self, value: &str) -> bool {
        if matches!(self.values.get(self.pos), Some(Token::Word(word))
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value))
        {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, value: &str) -> Result<()> {
        if self.keyword(value) {
            Ok(())
        } else {
            Err(invalid(format!("partition bound expects {value}")))
        }
    }

    fn group(&mut self) -> Result<Vec<Vec<Token>>> {
        if self.values.get(self.pos) != Some(&Token::LParen) {
            return Err(invalid("partition bound expects a parenthesized list"));
        }
        self.pos += 1;
        let mut depth = 0;
        let mut groups = vec![Vec::new()];
        while let Some(token) = self.values.get(self.pos) {
            self.pos += 1;
            match token {
                Token::RParen if depth == 0 => {
                    if groups.iter().any(Vec::is_empty) {
                        return Err(invalid("empty partition bound value"));
                    }
                    return Ok(groups);
                }
                Token::Comma if depth == 0 => groups.push(Vec::new()),
                token => {
                    if *token == Token::LParen {
                        depth += 1;
                    } else if *token == Token::RParen {
                        depth -= 1;
                    }
                    groups.last_mut().unwrap().push(token.clone());
                }
            }
        }
        Err(invalid("unterminated partition bound"))
    }

    fn finish(self) -> Result<()> {
        if self.pos == self.values.len() {
            Ok(())
        } else {
            Err(invalid("unexpected trailing partition bound clause"))
        }
    }
}

fn bound_literal(tokens: &[Token], column: &ColumnSchema) -> Result<SqlValue> {
    let expression = tokens
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&expression)
        .map_err(|e| invalid(e.to_string()))?;
    let expr = parser.parse_expr().map_err(|e| invalid(e.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(invalid("unexpected trailing partition bound expression"));
    }
    // Bounds are persisted, not re-evaluated at each row's wall clock. Do not
    // admit session/clock/routine-dependent expressions as catalog constants.
    fn immutable(expr: &Expr) -> bool {
        match expr {
            Expr::Value(_) | Expr::TypedString(_) => true,
            Expr::Cast { expr, .. } | Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
                immutable(expr)
            }
            Expr::BinaryOp { left, right, .. } => immutable(left) && immutable(right),
            _ => false,
        }
    }
    if !immutable(&expr) {
        return Err(SqlError::Unsupported(
            "partition bounds require immutable literal expressions".into(),
        ));
    }
    cast_value_to_column_type(eval_constant_expr(&expr)?, column)
}

pub(crate) fn key_columns(parent: &TableSchema) -> Result<Vec<&ColumnSchema>> {
    let partitioning = parent
        .partitioning
        .as_ref()
        .ok_or_else(|| invalid("parent is not partitioned"))?;
    partitioning
        .key_columns
        .iter()
        .map(|name| {
            parent
                .columns
                .iter()
                .find(|column| column.name == *name && !column.hidden)
                .ok_or_else(|| invalid(format!("partition key column {name} does not exist")))
        })
        .collect()
}

fn is_keyword(tokens: &[Token], keyword: &str) -> bool {
    matches!(tokens, [Token::Word(word)] if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword))
}

pub(crate) fn parse_bound(parent: &TableSchema, sql: &str) -> Result<Bound> {
    let columns = key_columns(parent)?;
    let strategy = &parent.partitioning.as_ref().unwrap().strategy;
    let mut tokens = Tokens::new(sql)?;
    if tokens.keyword("DEFAULT") {
        tokens.finish()?;
        if strategy == "hash" {
            return Err(invalid("a hash partition cannot be a default partition"));
        }
        return Ok(Bound::Default);
    }
    tokens.expect("FOR")?;
    tokens.expect("VALUES")?;
    let bound = match strategy.as_str() {
        "range" => {
            tokens.expect("FROM")?;
            let lower = tokens.group()?;
            tokens.expect("TO")?;
            let upper = tokens.group()?;
            let convert = |values: Vec<Vec<Token>>| -> Result<Vec<BoundValue>> {
                if values.len() != columns.len() {
                    return Err(invalid(
                        "partition bound must have one value for each partition key",
                    ));
                }
                let mut tail = None;
                values
                    .iter()
                    .zip(&columns)
                    .map(|(value, column)| {
                        let value = if is_keyword(value, "MINVALUE") {
                            BoundValue::Min
                        } else if is_keyword(value, "MAXVALUE") {
                            BoundValue::Max
                        } else {
                            let value = bound_literal(value, column)?;
                            if matches!(value, SqlValue::Null) {
                                return Err(invalid(
                                    "NULL is not allowed in a range partition bound",
                                ));
                            }
                            BoundValue::Value(value)
                        };
                        let sentinel = match value {
                            BoundValue::Min => Some(false),
                            BoundValue::Max => Some(true),
                            _ => None,
                        };
                        if tail.is_some() && tail != sentinel {
                            return Err(invalid(
                                "values after MINVALUE/MAXVALUE must use the same sentinel",
                            ));
                        }
                        tail = sentinel;
                        Ok(value)
                    })
                    .collect()
            };
            let lower = convert(lower)?;
            let upper = convert(upper)?;
            if compare_bound_keys(&columns, &lower, &upper)? != Ordering::Less {
                return Err(invalid("empty range partition bound"));
            }
            Bound::Range(lower, upper)
        }
        "list" => {
            if columns.len() != 1 {
                return Err(invalid(
                    "LIST partitioning requires exactly one partition key",
                ));
            }
            tokens.expect("IN")?;
            let mut values = Vec::new();
            for value in tokens.group()? {
                let value = bound_literal(&value, columns[0])?;
                for old in &values {
                    if equal_value(columns[0], old, &value)? {
                        return Err(invalid("duplicate value in list partition bound"));
                    }
                }
                values.push(value);
            }
            Bound::List(values)
        }
        "hash" => {
            tokens.expect("WITH")?;
            let groups = tokens.group()?;
            let mut modulus = None;
            let mut remainder = None;
            for group in groups {
                let [Token::Word(word), Token::Number(value, _)] = group.as_slice() else {
                    return Err(invalid(
                        "hash partition bound expects MODULUS and REMAINDER",
                    ));
                };
                let value = value
                    .parse::<u64>()
                    .map_err(|_| invalid("invalid hash partition bound"))?;
                let slot = if word.value.eq_ignore_ascii_case("MODULUS") {
                    &mut modulus
                } else if word.value.eq_ignore_ascii_case("REMAINDER") {
                    &mut remainder
                } else {
                    return Err(invalid("unknown hash partition bound option"));
                };
                if slot.replace(value).is_some() {
                    return Err(invalid("duplicate hash partition bound option"));
                }
            }
            let (Some(modulus), Some(remainder)) = (modulus, remainder) else {
                return Err(invalid(
                    "hash partition bound requires MODULUS and REMAINDER",
                ));
            };
            if modulus == 0 || remainder >= modulus || modulus > i32::MAX as u64 {
                return Err(invalid("invalid hash partition modulus or remainder"));
            }
            Bound::Hash { modulus, remainder }
        }
        _ => return Err(invalid("unknown partition strategy")),
    };
    tokens.finish()?;
    Ok(bound)
}

fn compare_value(column: &ColumnSchema, left: &SqlValue, right: &SqlValue) -> Result<Ordering> {
    if let Some(Some(true)) = compare_values_with_collation(
        column.collation.as_deref(),
        left,
        &BinaryOperator::Eq,
        right,
    ) {
        return Ok(Ordering::Equal);
    }
    if let Some(Some(less)) = compare_values_with_collation(
        column.collation.as_deref(),
        left,
        &BinaryOperator::Lt,
        right,
    ) {
        return Ok(if less {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    type_codec::column_typed_compare(column, left, right)
}

fn equal_value(column: &ColumnSchema, left: &SqlValue, right: &SqlValue) -> Result<bool> {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ok(true),
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(false),
        _ => compare_value(column, left, right).map(|order| order == Ordering::Equal),
    }
}

fn compare_bound_keys(
    columns: &[&ColumnSchema],
    left: &[BoundValue],
    right: &[BoundValue],
) -> Result<Ordering> {
    for ((column, left), right) in columns.iter().zip(left).zip(right) {
        let order = match (left, right) {
            (BoundValue::Min, BoundValue::Min) | (BoundValue::Max, BoundValue::Max) => {
                Ordering::Equal
            }
            (BoundValue::Min, _) | (_, BoundValue::Max) => Ordering::Less,
            (BoundValue::Max, _) | (_, BoundValue::Min) => Ordering::Greater,
            (BoundValue::Value(left), BoundValue::Value(right)) => {
                compare_value(column, left, right)?
            }
        };
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(Ordering::Equal)
}

pub(crate) fn bounds_overlap(parent: &TableSchema, left: &Bound, right: &Bound) -> Result<bool> {
    let columns = key_columns(parent)?;
    Ok(match (left, right) {
        (Bound::Default, Bound::Default) => true,
        (Bound::Default, _) | (_, Bound::Default) => false,
        (Bound::Range(a, b), Bound::Range(c, d)) => {
            compare_bound_keys(&columns, a, d)? == Ordering::Less
                && compare_bound_keys(&columns, c, b)? == Ordering::Less
        }
        (Bound::List(a), Bound::List(b)) => {
            let mut overlap = false;
            for a in a {
                for b in b {
                    overlap |= equal_value(columns[0], a, b)?;
                }
            }
            overlap
        }
        (
            Bound::Hash {
                modulus: a,
                remainder: b,
            },
            Bound::Hash {
                modulus: c,
                remainder: d,
            },
        ) => {
            let (small, small_r, large, large_r) = if a <= c {
                (*a, *b, *c, *d)
            } else {
                (*c, *d, *a, *b)
            };
            if large % small != 0 {
                return Err(invalid(
                    "every hash partition modulus must divide the next larger modulus",
                ));
            }
            large_r % small == small_r
        }
        _ => return Err(invalid("partition bound strategy mismatch")),
    })
}

pub(crate) fn children(db: &BicDb, parent: &TableSchema) -> Result<Vec<TableSchema>> {
    // Compare actual stored identities, including encoded namespaces and case.
    Ok(list_schemas(db)?
        .into_iter()
        .filter(|child| {
            child.partition_of.as_ref().is_some_and(|link| {
                link.parent_table == parent.name && link.parent_schema == parent.schema_name
            })
        })
        .collect())
}

/// Preserve source collection identity alongside every heap row. Parent scans
/// include legacy rows stored in the parent's own heap as well as descendants;
/// an upgrade must not silently hide previously acknowledged data.
pub(crate) fn physical_tables(db: &BicDb, parent: &TableSchema) -> Result<Vec<TableSchema>> {
    fn visit(
        db: &BicDb,
        table: &TableSchema,
        seen: &mut BTreeSet<String>,
        out: &mut Vec<TableSchema>,
    ) -> Result<()> {
        if !seen.insert(table.name.clone()) {
            return Err(invalid("cyclic partition tree"));
        }
        out.push(table.clone());
        if table.partitioning.is_some() {
            for child in children(db, table)? {
                visit(db, &child, seen, out)?;
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    visit(db, parent, &mut BTreeSet::new(), &mut out)?;
    Ok(out)
}

/// Preserve visible values when a physical leaf uses a different Native key
/// than the relation named by the statement. Ordinary single-column PK records
/// may store that value only in their Native ID. The transient row image keeps
/// the real physical ID for writes, system columns and row locks.
pub(crate) fn materialize_primary_key_fields(schema: &TableSchema, record: &mut Record) {
    for column in schema
        .columns
        .iter()
        .filter(|column| column.primary_key && !column.hidden)
    {
        if record.metadata.get(&column.name).is_none() {
            let value = record_column_value(record, schema, &column.name);
            set_json_object_value(
                &mut record.metadata,
                &column.name,
                sql_value_to_column_storage_json(value, Some(column)),
            );
        }
    }
}

pub(crate) fn factor_reads_parent(db: &BicDb, factor: &TableFactor) -> Result<bool> {
    match factor {
        TableFactor::Table {
            name, args: None, ..
        } => {
            let table = resolve_session_relation_name_if_exists(db, &relation_name(name)?);
            Ok(load_schema(db, &table)?.is_some_and(|schema| schema.partitioning.is_some()))
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => from_reads_parent(db, table_with_joins),
        // Derived tables and CTEs execute through their own query scope.
        _ => Ok(false),
    }
}

pub(crate) fn from_reads_parent(db: &BicDb, from: &TableWithJoins) -> Result<bool> {
    if factor_reads_parent(db, &from.relation)? {
        return Ok(true);
    }
    for join in &from.joins {
        if factor_reads_parent(db, &join.relation)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn matches_bound(parent: &TableSchema, bound: &Bound, record: &Record) -> Result<bool> {
    let columns = key_columns(parent)?;
    let values = record_column_values(
        record,
        parent,
        &parent.partitioning.as_ref().unwrap().key_columns,
    );
    match bound {
        Bound::Range(lower, upper) => {
            if values.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(false);
            }
            let key = values
                .into_iter()
                .map(BoundValue::Value)
                .collect::<Vec<_>>();
            Ok(compare_bound_keys(&columns, &key, lower)? != Ordering::Less
                && compare_bound_keys(&columns, &key, upper)? == Ordering::Less)
        }
        Bound::List(list) => {
            for value in list {
                if equal_value(columns[0], &values[0], value)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Bound::Default => Err(invalid("default bounds require sibling exclusion")),
        Bound::Hash { modulus, remainder } => {
            Ok(hash::row_hash(&columns, &values)? % modulus == *remainder)
        }
    }
}

pub(crate) fn matches_partition(
    db: &BicDb,
    parent: &TableSchema,
    child: &TableSchema,
    record: &Record,
) -> Result<bool> {
    let link = child
        .partition_of
        .as_ref()
        .ok_or_else(|| invalid("relation is not a partition"))?;
    let bound = parse_bound(parent, &link.bound)?;
    if !matches!(bound, Bound::Default) {
        return matches_bound(parent, &bound, record);
    }
    for sibling in children(db, parent)? {
        let bound = parse_bound(parent, &sibling.partition_of.as_ref().unwrap().bound)?;
        if !matches!(bound, Bound::Default) && matches_bound(parent, &bound, record)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn validate_record(db: &BicDb, schema: &TableSchema, record: &Record) -> Result<()> {
    let mut current = schema.clone();
    let mut visited = BTreeSet::new();
    while let Some(link) = current.partition_of.as_ref() {
        if !visited.insert(current.name.clone()) {
            return Err(invalid("cyclic partition ancestry"));
        }
        let parent = load_schema(db, &link.parent_table)?
            .ok_or_else(|| invalid("partition parent is missing"))?;
        if parent.schema_name != link.parent_schema
            || !matches_partition(db, &parent, &current, record)?
        {
            return Err(violation(
                &schema.name,
                format!(
                    "new row for relation \"{}\" violates partition constraint",
                    catalog_display_name(&schema.name)
                ),
            ));
        }
        current = parent;
    }
    Ok(())
}

pub(crate) fn route_record(
    db: &BicDb,
    accessed: &TableSchema,
    record: &Record,
) -> Result<TableSchema> {
    validate_record(db, accessed, record)?;
    let mut current = accessed.clone();
    let mut seen = BTreeSet::new();
    while current.partitioning.is_some() {
        if !seen.insert(current.name.clone()) {
            return Err(invalid("cyclic partition tree"));
        }
        let mut target = None;
        for child in children(db, &current)? {
            if matches_partition(db, &current, &child, record)? {
                if target.is_some() {
                    return Err(invalid("row matches multiple partition bounds"));
                }
                target = Some(child);
            }
        }
        current = target.ok_or_else(|| {
            violation(
                &accessed.name,
                format!(
                    "no partition of relation \"{}\" found for row",
                    catalog_display_name(&current.name)
                ),
            )
        })?;
    }
    Ok(current)
}

fn scan(db: &BicDb, tx: Option<&Transaction>, table: &str) -> Result<Vec<Record>> {
    match tx {
        Some(tx) => tx.scan_collection(table).map_err(SqlError::from),
        None => db.scan_collection(table).map_err(SqlError::from),
    }
}

/// Preflight every catalog and row condition before publishing an attachment.
/// Existing acknowledged rows are checked in place; none are moved or erased.
pub(crate) fn validate_attachment(
    db: &BicDb,
    tx: Option<&Transaction>,
    parent: &TableSchema,
    child: &TableSchema,
    allow_missing_new_child: bool,
) -> Result<()> {
    let link = child
        .partition_of
        .as_ref()
        .ok_or_else(|| invalid("missing partition attachment"))?;
    let bound = parse_bound(parent, &link.bound)?;
    let parent_columns = parent
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .collect::<Vec<_>>();
    let child_columns = child
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .collect::<Vec<_>>();
    if parent_columns.len() != child_columns.len()
        || parent_columns.iter().zip(&child_columns).any(|(a, b)| {
            a.name != b.name
                || a.pg_type != b.pg_type
                || a.type_modifier != b.type_modifier
                || a.collation != b.collation
                || (!a.nullable && b.nullable)
        })
    {
        return Err(invalid("partition columns must match the parent table"));
    }
    let mut ancestor = Some(parent.clone());
    let mut visited = BTreeSet::new();
    while let Some(current) = ancestor {
        if current.name == child.name || !visited.insert(current.name.clone()) {
            return Err(invalid("circular partition attachment"));
        }
        ancestor = current
            .partition_of
            .as_ref()
            .map(|link| load_schema(db, &link.parent_table))
            .transpose()?
            .flatten();
    }
    let siblings = children(db, parent)?;
    for sibling in &siblings {
        if sibling.name == child.name {
            return Err(invalid("relation is already a partition of this parent"));
        }
        let other = parse_bound(parent, &sibling.partition_of.as_ref().unwrap().bound)?;
        if bounds_overlap(parent, &bound, &other)? {
            return Err(invalid(format!(
                "partition \"{}\" would overlap partition \"{}\"",
                catalog_display_name(&child.name),
                catalog_display_name(&sibling.name)
            )));
        }
    }
    fn physical_relations(
        db: &BicDb,
        table: &TableSchema,
        visited: &mut BTreeSet<String>,
        out: &mut Vec<String>,
    ) -> Result<()> {
        if !visited.insert(table.name.clone()) {
            return Err(invalid("cyclic partition tree"));
        }
        out.push(table.name.clone());
        if table.partitioning.is_some() {
            for child in children(db, table)? {
                physical_relations(db, &child, visited, out)?;
            }
        }
        Ok(())
    }
    let mut relations = Vec::new();
    physical_relations(db, child, &mut BTreeSet::new(), &mut relations)?;
    // CREATE PARTITION preflights a not-yet-created empty relation.
    for table in relations {
        let records = match scan(db, tx, &table) {
            Ok(records) => records,
            Err(SqlError::BicDb(BicDbError::CollectionNotFound(_)))
                if allow_missing_new_child && table == child.name =>
            {
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        for record in records {
            if !matches_partition(db, parent, child, &record)? {
                return Err(violation(
                    &child.name,
                    "partition constraint is violated by an existing row",
                ));
            }
            validate_record(db, parent, &record)?;
        }
    }
    if !matches!(bound, Bound::Default) {
        for sibling in siblings {
            if !matches!(
                parse_bound(parent, &sibling.partition_of.as_ref().unwrap().bound)?,
                Bound::Default
            ) {
                continue;
            }
            let mut relations = Vec::new();
            physical_relations(db, &sibling, &mut BTreeSet::new(), &mut relations)?;
            for table in relations {
                for record in scan(db, tx, &table)? {
                    if matches_bound(parent, &bound, &record)? {
                        return Err(violation(&sibling.name, "updated default partition constraint would be violated by an existing row"));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Validate an already attached forest without treating each child as a new
/// attachment. Exact identities and every sibling bound are preflighted.
pub(crate) fn validate_definition(schema: &TableSchema) -> Result<()> {
    let Some(partitioning) = &schema.partitioning else {
        return Ok(());
    };
    let covers = |columns: &[String]| {
        partitioning
            .key_columns
            .iter()
            .all(|key| columns.contains(key))
    };
    if !schema.has_hidden_primary_key() && !covers(&primary_key_columns_for_schema(schema)) {
        return Err(SqlError::Unsupported(
            "unique constraint on partitioned table must include all partitioning columns".into(),
        ));
    }
    for constraint in &schema.constraints {
        if let ConstraintSchema::Unique { columns, .. }
        | ConstraintSchema::UniqueWithNullPolicy { columns, .. } = constraint
        {
            if !covers(columns) {
                return Err(SqlError::Unsupported(
                    "unique constraint on partitioned table must include all partitioning columns"
                        .into(),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_existing_tree(db: &BicDb, schemas: &[TableSchema]) -> Result<()> {
    for parent in schemas
        .iter()
        .filter(|schema| schema.partitioning.is_some())
    {
        validate_definition(parent)?;
        let children = schemas
            .iter()
            .filter(|child| {
                child
                    .partition_of
                    .as_ref()
                    .is_some_and(|link| link.parent_table == parent.name)
            })
            .collect::<Vec<_>>();
        for (index, child) in children.iter().enumerate() {
            let link = child.partition_of.as_ref().unwrap();
            if link.parent_schema != parent.schema_name {
                return Err(invalid("partition namespace mismatch"));
            }
            let a = parent
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .collect::<Vec<_>>();
            let b = child
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .collect::<Vec<_>>();
            if a.len() != b.len()
                || a.iter().zip(&b).any(|(a, b)| {
                    a.name != b.name
                        || a.pg_type != b.pg_type
                        || a.type_modifier != b.type_modifier
                        || a.collation != b.collation
                        || (!a.nullable && b.nullable)
                })
            {
                return Err(invalid("legacy partition columns do not match parent"));
            }
            let bound = parse_bound(parent, &link.bound)?;
            for other in &children[..index] {
                let other = parse_bound(parent, &other.partition_of.as_ref().unwrap().bound)?;
                if bounds_overlap(parent, &bound, &other)? {
                    return Err(invalid("overlapping legacy partition bounds"));
                }
            }
        }
    }
    let _ = db;
    Ok(())
}
