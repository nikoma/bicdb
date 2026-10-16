//! Application-scoped incremental SELECT subscriptions.
use super::*;
use sqlparser::ast::{Expr, ObjectNamePart, Query as AstQuery, SetExpr, Visit, Visitor};

pub struct LiveQuery {
    database: std::sync::Weak<RwLock<BicDb>>,
    query: Query,
    subscription: bicdb_pgwire::EmbeddedSubscription,
}

struct ReadOnly;
fn read_body(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => select.into.is_none(),
        SetExpr::Query(query) => query.locks.is_empty() && read_body(&query.body),
        SetExpr::SetOperation { left, right, .. } => read_body(left) && read_body(right),
        SetExpr::Values(_) | SetExpr::Table(_) => true,
        _ => false,
    }
}
impl Visitor for ReadOnly {
    type Break = ();
    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
        if matches!(statement, Statement::Query(_)) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }
    fn pre_visit_query(&mut self, query: &AstQuery) -> ControlFlow<()> {
        if query.locks.is_empty() && read_body(&query.body) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }
    fn pre_visit_relation(&mut self, relation: &sqlparser::ast::ObjectName) -> ControlFlow<()> {
        let parts = relation
            .0
            .iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(id) => Some(id.value.to_ascii_lowercase()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        let allowed = parts.is_some_and(|parts| match parts.as_slice() {
            [table] => !table.starts_with("__bic"),
            [schema, table] => schema == "public" && !table.starts_with("__bic"),
            _ => false,
        });
        if allowed {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }
    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<()> {
        if let Expr::Function(function) = expression {
            let name = function.name.to_string().to_ascii_lowercase();
            let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
            // Volatile, provider and extension functions cannot acquire effects
            // merely because an HTTP consumer is polling a SELECT.
            if !matches!(
                name,
                "count"
                    | "sum"
                    | "avg"
                    | "min"
                    | "max"
                    | "coalesce"
                    | "nullif"
                    | "lower"
                    | "upper"
                    | "length"
                    | "char_length"
                    | "substring"
                    | "substr"
                    | "trim"
                    | "btrim"
                    | "ltrim"
                    | "rtrim"
                    | "abs"
                    | "round"
                    | "floor"
                    | "ceil"
                    | "ceiling"
                    | "power"
                    | "sqrt"
                    | "concat"
                    | "concat_ws"
                    | "json_build_object"
                    | "jsonb_build_object"
                    | "to_json"
                    | "to_jsonb"
                    | "json_agg"
                    | "jsonb_agg"
                    | "array_agg"
                    | "date_trunc"
                    | "extract"
            ) {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

impl Database {
    pub fn subscribe(&self, query: Query) -> Result<LiveQuery> {
        ensure!(
            query.sql.len() <= 128 * 1024
                && query.params.len() <= 1024
                && serde_json::to_vec(&query)?.len() <= 1024 * 1024,
            "live query too large"
        );
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, &query.sql)?;
        ensure!(statements.len() == 1 && matches!(statements[0], Statement::Query(_))
            && statements.visit(&mut ReadOnly).is_continue(),
            "live queries require a read-only SELECT over public application tables and pure built-in functions");
        self.edge_barrier()?;
        let subscription = bicdb_pgwire::EmbeddedSubscription::new(&self.db.read(), &query.sql)?;
        Ok(LiveQuery {
            database: Arc::downgrade(&self.db),
            query,
            subscription,
        })
    }

    pub fn poll_subscription(&self, live: &mut LiveQuery) -> Result<Option<SqlResult>> {
        ensure!(
            live.database
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &self.db)),
            "live query belongs to a different database instance"
        );
        self.edge_barrier()?;
        let db = self.db.read();
        let cancellation = bicdb_core::CancellationToken::new(
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
        );
        let parameters = live
            .query
            .params
            .iter()
            .map(|value| match value {
                Value::Null => SqlValue::Null,
                Value::Bool(value) => SqlValue::Bool(*value),
                Value::Number(value) => value
                    .as_i64()
                    .map(SqlValue::Int)
                    .or_else(|| value.as_f64().map(SqlValue::Float))
                    .unwrap_or_else(|| SqlValue::Json(value.clone().into())),
                Value::String(value) => SqlValue::String(value.clone()),
                other => SqlValue::Json(other.clone()),
            })
            .collect::<Vec<_>>();
        let result = live.subscription.poll_with(&db, |sql| {
            let mut session = SqlSession::new_shared(&db)
                .with_cancellation(cancellation.clone())
                .with_positional_parameters(parameters.clone());
            session.execute("BEGIN")?;
            let result = session.execute(sql);
            let _ = session.execute("ROLLBACK");
            let result = result?;
            if result.rows.len() > 10_000
                || serde_json::to_vec(&result)
                    .map_err(|error| bicdb_pgwire::PgWireError::Server(error.to_string()))?
                    .len()
                    > 1024 * 1024
            {
                return Err(bicdb_pgwire::PgWireError::Server(
                    "live query result exceeds 10000 rows or 1 MiB".into(),
                ));
            }
            Ok(result)
        })?;
        if let Some(result) = &result {
            ensure!(
                serde_json::to_vec(result)?.len() <= 2 * 1024 * 1024,
                "live query change batch exceeds 2 MiB"
            );
        }
        Ok(result)
    }
}
