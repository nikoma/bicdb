//! In-process access to the same incremental plans as protocol subscriptions.
use super::*;

/// One caller-owned live SELECT. The embedding host owns authorization,
/// evaluation deadlines, result limits and the connection's lifetime. This
/// handle owns neither a database lock nor a worker thread.
pub struct EmbeddedSubscription {
    subscription: Subscription,
    initialized: bool,
    last_sequence: u64,
}

impl EmbeddedSubscription {
    pub fn new(db: &BicDb, query: &str) -> Result<Self> {
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, query)
            .map_err(|error| PgWireError::Server(error.to_string()))?;
        if statements.len() != 1 || !matches!(statements[0], Statement::Query(_)) {
            return Err(PgWireError::Server(
                "a live query requires one SELECT".into(),
            ));
        }
        Ok(Self {
            subscription: Subscription::new(db, query),
            initialized: false,
            last_sequence: 0,
        })
    }

    /// Evaluate a coalesced change batch. Initial rows carry `+`; updates emit
    /// their old row with `-` and new row with `+`. Leading columns are
    /// `commit_seq` and `change`. A first empty result still returns its schema.
    /// The caller must keep the database stable across evaluation and authorize
    /// every generated SQL statement as the subscribing application.
    pub fn poll_with(
        &mut self,
        db: &BicDb,
        mut evaluate: impl FnMut(&str) -> Result<SqlResult>,
    ) -> Result<Option<SqlResult>> {
        let sequence = db.current_visibility_watermark();
        if self.initialized && sequence <= self.last_sequence {
            return Ok(None);
        }
        self.last_sequence = sequence;
        let mut evaluator = |sql: &str, _enforce: bool| evaluate(sql);
        let round = if !self.initialized {
            self.subscription.whole_round(&mut evaluator, sequence)?
        } else {
            let Some(previous) = self.subscription.take_changed_generations(db) else {
                return Ok(None);
            };
            let changes = |collection: &str, generation: u64| {
                db.collection_changes_since(collection, generation)
            };
            self.subscription
                .change_round(&changes, &mut evaluator, &previous, sequence)?
        };
        let first = !self.initialized;
        self.initialized = true;
        self.subscription.generations = self.subscription.generations(db);
        if !first && round.rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(batch(&self.subscription.template, round.rows)))
    }
}
