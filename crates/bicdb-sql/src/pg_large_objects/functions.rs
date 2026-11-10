//! Database-backed PostgreSQL LO calls. The enclosing SQL statement owns the
//! transaction, write admission, durability, rollback, and connection handles.
use super::*;
use descriptors::{Access, DescriptorTransaction};

const INV_WRITE: i32 = 0x20000;
const INV_READ: i32 = 0x40000;

// PostgreSQL 16.13 pg_proc control: native OIDs and input oidvectors for the
// implemented database-backed calls. Server-file import/export remain separate.
pub(crate) fn argument_oids(oid: i64) -> Option<&'static str> {
    Some(match oid {
        715 | 964 | 3458 => "26",
        952 => "26 23",
        953 | 957 | 958 | 3171 => "23",
        954 | 1004 => "23 23",
        955 => "23 17",
        956 => "23 23 23",
        3170 => "23 20 23",
        3172 => "23 20",
        3457 => "26 17",
        3459 => "26 20 23",
        3460 => "26 20 17",
        _ => return None,
    })
}

pub(crate) fn function_name(name: &str) -> Option<&str> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    matches!(
        name,
        "lo_create"
            | "lo_creat"
            | "lo_from_bytea"
            | "lo_get"
            | "lo_put"
            | "lo_open"
            | "lo_close"
            | "loread"
            | "lowrite"
            | "lo_lseek"
            | "lo_lseek64"
            | "lo_tell"
            | "lo_tell64"
            | "lo_truncate"
            | "lo_truncate64"
            | "lo_unlink"
    )
    .then_some(name)
}

pub(crate) fn query_has_calls(query: &Query) -> bool {
    use std::ops::ControlFlow;
    matches!(
        sqlparser::ast::visit_expressions(query, |expr| {
            if let Expr::Function(function) = expr {
                if object_name_lowercase(&function.name)
                    .ok()
                    .is_some_and(|name| function_name(&name).is_some())
                {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }),
        ControlFlow::Break(())
    )
}

pub(crate) fn return_type(name: &str) -> Option<&'static str> {
    Some(match function_name(name)? {
        "lo_create" | "lo_creat" | "lo_from_bytea" => "oid",
        "lo_get" | "loread" => "bytea",
        "lo_lseek64" | "lo_tell64" => "int8",
        "lo_put" => "void",
        _ => "int4",
    })
}

fn invalid(message: &str) -> SqlError {
    SqlError::invalid_parameter_value(message)
}

fn denied(oid: u32, owner: bool) -> SqlError {
    SqlError::data_exception(
        "42501",
        if owner {
            format!("must be owner of large object {oid}")
        } else {
            format!("permission denied for large object {oid}")
        },
        None,
    )
}

fn integer(value: &SqlValue, pg_type: &str) -> Result<i64> {
    match cast_value_to_pg_type(value.clone(), pg_type)? {
        SqlValue::Int(value) => Ok(value),
        _ => Err(invalid("invalid large object integer argument")),
    }
}
fn oid(value: &SqlValue) -> Result<u32> {
    Ok(integer(value, "oid")? as u32)
}
fn int4(value: &SqlValue) -> Result<i32> {
    Ok(integer(value, "int4")? as i32)
}
fn offset(value: &SqlValue) -> Result<u64> {
    let value = integer(value, "int8")?;
    u64::try_from(value)
        .ok()
        .filter(|n| *n <= storage::MAX_LENGTH)
        .ok_or_else(|| invalid("invalid large object offset"))
}
fn narrow_position(value: u64) -> Result<SqlValue> {
    i32::try_from(value)
        .map(|v| SqlValue::Int(v as i64))
        .map_err(|_| SqlError::numeric_value_out_of_range("large object position out of range"))
}
fn bytea(value: &SqlValue) -> Result<Vec<u8>> {
    let bytes = bytea_argument(value)?.ok_or_else(|| invalid("null bytea argument"))?;
    let retained = bytes.len().saturating_mul(8);
    bicdb_core::query_budget::observe(retained);
    bicdb_core::query_budget::check()?;
    Ok(bytes)
}

impl SqlSession<'_> {
    pub(crate) fn prepare_pg_large_object_storage(&mut self) -> Result<()> {
        self.db_ref().check_recovery_state()?;
        if !self.db_ref().has_collection(METADATA) || !self.db_ref().has_collection(PAGES) {
            storage::ensure_storage(self.db_mut()?)?;
        }
        Ok(())
    }

    fn require_large_object_rights(
        &self,
        tx: &Transaction,
        oid: u32,
        rights: u8,
        owner_only: bool,
    ) -> Result<()> {
        let metadata = storage::metadata(tx, oid)?;
        if self.current_user_is_superuser()? || self.current_user_holds_role(&metadata.owner)? {
            return Ok(());
        }
        if owner_only {
            return Err(denied(oid, true));
        }
        let user = current_user_from_gucs(&self.session_gucs);
        let roles = role_privilege_closure(self.db_ref(), &user)?;
        let granted = metadata
            .acl
            .iter()
            .filter(|(role, _)| {
                role.eq_ignore_ascii_case("public")
                    || role.eq_ignore_ascii_case(&user)
                    || roles.contains(&normalize_role_name(role))
            })
            .fold(0, |granted, (_, rights)| granted | rights);
        if granted & rights != rights {
            return Err(denied(oid, false));
        }
        Ok(())
    }

    fn create_large_object(&self, tx: &mut Transaction, requested: u32) -> Result<u32> {
        let owner = current_user_from_gucs(&self.session_gucs);
        for _ in 0..64 {
            self.cancellation.check()?;
            let candidate = if requested != 0 {
                requested
            } else {
                // OIDs are opaque identities. Check/reserve every candidate in
                // the same transaction; collisions never overwrite an object.
                (uuid::Uuid::new_v4().as_u128() as u32).max(16384)
            };
            match storage::create(
                self.db_ref(),
                tx,
                candidate,
                &owner,
                Some(&self.cancellation),
            ) {
                Ok(()) => return Ok(candidate),
                Err(SqlError::DataException {
                    sqlstate: "42710", ..
                }) if requested == 0 => {}
                Err(error) => return Err(error),
            }
        }
        Err(SqlError::data_exception(
            "54000",
            "could not allocate a large object OID",
            None,
        ))
    }

    pub(crate) fn eval_pg_large_object_function(
        &mut self,
        name: &str,
        args: &[SqlValue],
    ) -> Result<Option<SqlValue>> {
        let Some(name) = function_name(name) else {
            return Ok(None);
        };
        if oracle::is_oracle_database(self.db_ref())? {
            return Ok(None);
        }
        let count = match name {
            "lo_create" | "lo_creat" | "lo_get" | "lo_close" | "lo_tell" | "lo_tell64"
            | "lo_unlink" => 1,
            "lo_from_bytea" | "lo_open" | "loread" | "lowrite" | "lo_truncate"
            | "lo_truncate64" => 2,
            _ => 3,
        };
        if !(name == "lo_get" && args.len() == 3) {
            require_arg_count(name, args, count)?;
        }
        // Every database-backed native LO proc is strict.
        if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
            return Ok(Some(SqlValue::Null));
        }
        self.db_ref().check_recovery_state()?;
        self.cancellation.check()?;
        let mut tx = self.tx.take().ok_or_else(|| {
            SqlError::Unsupported(
                "large object operation requires its owning statement transaction".into(),
            )
        })?;
        let result = self.eval_pg_large_object_in_transaction(&mut tx, name, args);
        self.tx = Some(tx);
        self.db_ref().check_recovery_state()?;
        result.map(Some)
    }

    fn eval_pg_large_object_in_transaction(
        &mut self,
        tx: &mut Transaction,
        name: &str,
        args: &[SqlValue],
    ) -> Result<SqlValue> {
        let key = DescriptorTransaction::new(self.db_ref(), tx);
        let write = matches!(
            name,
            "lo_create"
                | "lo_creat"
                | "lo_from_bytea"
                | "lo_put"
                | "lowrite"
                | "lo_truncate"
                | "lo_truncate64"
                | "lo_unlink"
        ) || name == "lo_open" && int4(&args[1])? & INV_WRITE != 0;
        if write {
            self.require_transaction_write()?;
        }
        match name {
            "lo_create" | "lo_creat" | "lo_from_bytea" => {
                let requested = if name == "lo_creat" {
                    int4(&args[0])?;
                    0
                } else {
                    oid(&args[0])?
                };
                let bytes = if name == "lo_from_bytea" {
                    Some(bytea(&args[1])?)
                } else {
                    None
                };
                let oid = self.create_large_object(tx, requested)?;
                if let Some(bytes) = bytes {
                    storage::mutation(self.db_ref(), tx, oid, Some(&self.cancellation))?.write(
                        0,
                        &bytes,
                        &self.cancellation,
                    )?;
                }
                Ok(SqlValue::Int(oid as i64))
            }
            "lo_open" => {
                let oid = oid(&args[0])?;
                let mode = int4(&args[1])?;
                if mode & (INV_READ | INV_WRITE) == 0 {
                    return Err(invalid("invalid flags for opening a large object"));
                }
                let writing = mode & INV_WRITE != 0;
                self.require_large_object_rights(tx, oid, if writing { 3 } else { 1 }, false)?;
                let limit = bicdb_core::query_budget::limit(64 * 1024 * 1024);
                let access = if writing {
                    Access::Write(oid)
                } else {
                    Access::Read(storage::capture_read_view(
                        tx,
                        oid,
                        limit.saturating_sub(self.pg_large_objects.retained_bytes()?),
                        &self.cancellation,
                    )?)
                };
                Ok(SqlValue::Int(
                    self.pg_large_objects.open(key, access, limit)? as i64,
                ))
            }
            "lo_close" => {
                self.pg_large_objects.close(key, int4(&args[0])?)?;
                Ok(SqlValue::Int(0))
            }
            "lo_tell" | "lo_tell64" => {
                let position = self.pg_large_objects.tell(key, int4(&args[0])?)?;
                if name == "lo_tell" {
                    narrow_position(position)
                } else {
                    Ok(SqlValue::Int(position as i64))
                }
            }
            "lo_lseek" | "lo_lseek64" => {
                let fd = int4(&args[0])?;
                let displacement =
                    integer(&args[1], if name == "lo_lseek" { "int4" } else { "int8" })?;
                let whence = int4(&args[2])?;
                let ticket = self.pg_large_objects.begin_io(key, fd, false)?;
                let length = match ticket.access {
                    Access::Write(oid) if whence == 2 => {
                        storage::physical_length(tx, oid, &self.cancellation)?
                    }
                    _ => 0,
                };
                let position = self
                    .pg_large_objects
                    .seek(key, fd, displacement, whence, length)?;
                // Native 32-bit result overflow occurs after the seek. A
                // savepoint rollback does not rewind this existing cursor.
                if name == "lo_lseek" {
                    narrow_position(position)
                } else {
                    Ok(SqlValue::Int(position as i64))
                }
            }
            "loread" => {
                let ticket = self
                    .pg_large_objects
                    .begin_io(key, int4(&args[0])?, false)?;
                // Native loread with a nonpositive count returns empty bytea;
                // lo_get's explicit length has a separate negative-length error.
                let count = int4(&args[1])?.max(0) as usize;
                let bytes = match &ticket.access {
                    Access::Read(view) => {
                        view.read(self.db_ref(), ticket.position, count, &self.cancellation)?
                    }
                    Access::Write(oid) => storage::descriptor_read(
                        tx,
                        *oid,
                        ticket.position,
                        count,
                        &self.cancellation,
                    )?,
                };
                self.pg_large_objects.finish_io(ticket, bytes.len())?;
                Ok(SqlValue::String(format_bytea_hex(&bytes)))
            }
            "lowrite" => {
                let ticket = self.pg_large_objects.begin_io(key, int4(&args[0])?, true)?;
                let bytes = bytea(&args[1])?;
                storage::descriptor_mutation(
                    self.db_ref(),
                    tx,
                    ticket.access.oid(),
                    Some(&self.cancellation),
                )?
                .write(ticket.position, &bytes, &self.cancellation)?;
                self.pg_large_objects.finish_io(ticket, bytes.len())?;
                Ok(SqlValue::Int(bytes.len() as i64))
            }
            "lo_truncate" | "lo_truncate64" => {
                let ticket = self.pg_large_objects.begin_io(key, int4(&args[0])?, true)?;
                let length = if name == "lo_truncate" {
                    u64::try_from(int4(&args[1])?)
                        .map_err(|_| invalid("invalid large object truncation target"))?
                } else {
                    offset(&args[1])?
                };
                storage::descriptor_mutation(
                    self.db_ref(),
                    tx,
                    ticket.access.oid(),
                    Some(&self.cancellation),
                )?
                .truncate(length, &self.cancellation)?;
                Ok(SqlValue::Int(0))
            }
            "lo_get" => {
                let oid = oid(&args[0])?;
                self.require_large_object_rights(tx, oid, 1, false)?;
                let (offset, count) = if args.len() == 3 {
                    (
                        offset(&args[1])?,
                        usize::try_from(int4(&args[2])?)
                            .map_err(|_| invalid("requested length cannot be negative"))?,
                    )
                } else {
                    (0, usize::MAX)
                };
                let bytes = storage::read(tx, oid, offset, count, &self.cancellation)?;
                Ok(SqlValue::String(format_bytea_hex(&bytes)))
            }
            "lo_put" => {
                let oid = oid(&args[0])?;
                let position = offset(&args[1])?;
                let bytes = bytea(&args[2])?;
                self.require_large_object_rights(tx, oid, 3, false)?;
                storage::mutation(self.db_ref(), tx, oid, Some(&self.cancellation))?.write(
                    position,
                    &bytes,
                    &self.cancellation,
                )?;
                Ok(SqlValue::String(String::new()))
            }
            "lo_unlink" => {
                let oid = oid(&args[0])?;
                self.require_large_object_rights(tx, oid, 0, true)?;
                storage::mutation(self.db_ref(), tx, oid, Some(&self.cancellation))?
                    .unlink(&self.cancellation)?;
                self.pg_large_objects.close_object(key, oid)?;
                Ok(SqlValue::Int(1))
            }
            _ => unreachable!("recognized large object function"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::{DbConfig, StorageMode};

    fn one(sql: &mut SqlSession<'_>, query: &str) -> SqlValue {
        let result = sql
            .execute(query)
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(result.rows.len(), 1, "{query}");
        assert_eq!(result.rows[0].len(), 1, "{query}");
        result.rows[0][0].clone()
    }
    fn state(sql: &mut SqlSession<'_>, query: &str, expected: &str) {
        assert_eq!(
            sql.execute(query).unwrap_err().sqlstate(),
            expected,
            "{query}"
        );
    }
    fn database(path: &std::path::Path, mode: StorageMode) -> BicDb {
        BicDb::open_with_config(
            path,
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap()
    }

    #[test]
    fn sql_bytes_cursor_snapshot_sparse_writes_and_transaction_boundaries() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = database(root.path(), mode);
            let mut sql = SqlSession::new(&mut db);
            sql.execute("BEGIN").unwrap();
            assert_eq!(
                one(&mut sql, "SELECT lo_create(42001)"),
                SqlValue::Int(42001)
            );
            assert_eq!(
                one(&mut sql, "SELECT lo_open(42001,393216)"),
                SqlValue::Int(0)
            );
            assert_eq!(
                one(&mut sql, "SELECT lowrite(0,decode('00017f80ff','hex'))"),
                SqlValue::Int(5)
            );
            assert_eq!(one(&mut sql, "SELECT lo_lseek64(0,8,0)"), SqlValue::Int(8));
            assert_eq!(
                one(&mut sql, "SELECT lowrite(0,decode('ab','hex'))"),
                SqlValue::Int(1)
            );
            assert_eq!(
                one(&mut sql, "SELECT encode(lo_get(42001),'hex')"),
                SqlValue::String("00017f80ff000000ab".into())
            );
            assert_eq!(
                one(&mut sql, "SELECT lo_open(42001,262144)"),
                SqlValue::Int(1)
            );
            assert_eq!(one(&mut sql, "SELECT lo_truncate64(0,3)"), SqlValue::Int(0));
            assert_eq!(one(&mut sql, "SELECT lo_tell64(0)"), SqlValue::Int(9));
            assert_eq!(
                one(&mut sql, "SELECT encode(loread(1,100),'hex')"),
                SqlValue::String("00017f80ff000000ab".into())
            );
            assert_eq!(
                one(&mut sql, "SELECT encode(lo_get(42001),'hex')"),
                SqlValue::String("00017f".into())
            );
            sql.execute("SAVEPOINT before_write").unwrap();
            one(&mut sql, "SELECT lo_lseek64(0,0,0)");
            one(&mut sql, "SELECT lowrite(0,decode('ffff','hex'))");
            sql.execute("ROLLBACK TO before_write").unwrap();
            assert_eq!(one(&mut sql, "SELECT lo_tell64(0)"), SqlValue::Int(2));
            assert_eq!(
                one(&mut sql, "SELECT encode(lo_get(42001),'hex')"),
                SqlValue::String("00017f".into())
            );
            sql.execute("COMMIT; BEGIN").unwrap();
            state(&mut sql, "SELECT lo_tell(0)", "42704");
            sql.execute("ROLLBACK").unwrap();
            assert_eq!(
                one(
                    &mut sql,
                    "SELECT encode(loread(lo_open(42001,262144),3),'hex')"
                ),
                SqlValue::String("00017f".into())
            );
            state(&mut sql, "SELECT lo_tell(0)", "42704");
        }
    }

    #[test]
    fn sql_whole_statement_rollback_strict_null_and_native_return_types() {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path(), StorageMode::ServerPaged);
        let mut sql = SqlSession::new(&mut db);
        assert!(sql
            .execute("SELECT lo_from_bytea(43001,'abc'::bytea),1/0")
            .is_err());
        state(&mut sql, "SELECT lo_get(43001)", "42704");
        assert_eq!(
            one(&mut sql, "SELECT lo_from_bytea(43001,'abc'::bytea)"),
            SqlValue::Int(43001)
        );
        assert_eq!(one(&mut sql, "SELECT lo_create(NULL)"), SqlValue::Null);
        sql.execute("BEGIN").unwrap();
        let result = sql.execute("SELECT lo_open(43001,393216),lo_tell64(0),lo_get(43001),lo_put(43001,0,'X'::bytea)").unwrap();
        assert_eq!(
            result.column_types,
            vec![
                Some("int4".into()),
                Some("int8".into()),
                Some("bytea".into()),
                Some("void".into())
            ]
        );
        let result = sql.execute("SELECT lo_create(43002)").unwrap();
        assert_eq!(result.column_types, vec![Some("oid".into())]);
        assert_eq!(
            one(&mut sql, "SELECT encode(lo_get(43001),'hex')"),
            SqlValue::String("586263".into())
        );
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(
            one(&mut sql, "SELECT encode(lo_get(43001),'hex')"),
            SqlValue::String("616263".into())
        );
        state(&mut sql, "SELECT lo_get(43002)", "42704");
    }

    #[test]
    fn native_catalog_identity_signatures_and_regprocedure_lookup_are_consistent() {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path(), StorageMode::EmbeddedMemory);
        let mut sql = SqlSession::new(&mut db);
        // Independent PG16.13 catalog controls established these ABI values;
        // in particular 1004 belongs to lo_truncate, never a privilege helper.
        for (oid, name, args, ret, nargs) in [
            (952, "lo_open", "26 23", 23, 2),
            (954, "loread", "23 23", 17, 2),
            (1004, "lo_truncate", "23 23", 23, 2),
            (3170, "lo_lseek64", "23 20 23", 20, 3),
            (3459, "lo_get", "26 20 23", 17, 3),
            (3460, "lo_put", "26 20 17", 2278, 3),
        ] {
            let result = sql.execute(&format!("SELECT proname,proargtypes::text,prorettype,pronargs,proisstrict,provolatile,proparallel,prolang FROM pg_catalog.pg_proc WHERE oid={oid}")).unwrap();
            assert_eq!(
                result.rows,
                vec![vec![
                    SqlValue::String(name.into()),
                    SqlValue::String(args.into()),
                    SqlValue::Int(ret),
                    SqlValue::Int(nargs),
                    SqlValue::Bool(true),
                    SqlValue::String("v".into()),
                    SqlValue::String("u".into()),
                    SqlValue::Int(12)
                ]]
            );
        }
        assert_eq!(
            one(&mut sql, "SELECT proname FROM pg_proc WHERE oid=2272"),
            SqlValue::String("has_schema_privilege".into())
        );
        assert_eq!(
            one(&mut sql, "SELECT 'lo_open'::regproc::oid"),
            SqlValue::Int(952)
        );
        assert_eq!(
            one(&mut sql, "SELECT 'lo_get(oid)'::regprocedure::oid"),
            SqlValue::Int(3458)
        );
        assert_eq!(
            one(
                &mut sql,
                "SELECT 'pg_catalog.lo_get(oid,bigint,integer)'::regprocedure::oid"
            ),
            SqlValue::Int(3459)
        );
        assert_eq!(
            one(
                &mut sql,
                "SELECT to_regprocedure('lo_get(oid,text,integer)')"
            ),
            SqlValue::Null
        );
    }

    #[test]
    fn sql_readonly_permissions_and_native_argument_edges() {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path(), StorageMode::EmbeddedMemory);
        let mut sql = SqlSession::new(&mut db);
        one(&mut sql, "SELECT lo_from_bytea(44001,'abc'::bytea)");
        sql.execute("BEGIN READ ONLY").unwrap();
        for query in [
            "SELECT lo_create(0)",
            "SELECT lo_open(44001,131072)",
            "SELECT lo_put(44001,0,'Z'::bytea)",
            "SELECT lowrite(0,'Z'::bytea)",
            "SELECT lo_truncate(0,1)",
            "SELECT lo_unlink(44001)",
        ] {
            state(&mut sql, query, "25006");
        }
        assert_eq!(
            one(&mut sql, "SELECT encode(lo_get(44001),'hex')"),
            SqlValue::String("616263".into())
        );
        sql.execute("ROLLBACK; BEGIN").unwrap();
        assert_eq!(one(&mut sql, "SELECT lo_open(44001,-1)"), SqlValue::Int(0));
        assert_eq!(
            one(&mut sql, "SELECT encode(loread(0,-1),'hex')"),
            SqlValue::String(String::new())
        );
        state(&mut sql, "SELECT lo_open(44001,0)", "22023");
        state(&mut sql, "SELECT lo_get(44001,0,-1)", "22023");
        state(&mut sql, "SELECT lo_truncate64(0,-1)", "22023");
        one(&mut sql, "SELECT lo_lseek64(0,2147483648,0)");
        state(&mut sql, "SELECT lo_lseek(0,0,1)", "22003");
        state(&mut sql, "SELECT lo_tell(0)", "22003");
        assert_eq!(
            one(&mut sql, "SELECT lo_tell64(0)"),
            SqlValue::Int(2147483648)
        );
        sql.execute("ROLLBACK;CREATE ROLE lo_stranger;SET ROLE lo_stranger")
            .unwrap();
        state(&mut sql, "SELECT lo_get(44001)", "42501");
        state(&mut sql, "SELECT lo_unlink(44001)", "42501");
        sql.execute("RESET ROLE").unwrap();
        assert_eq!(one(&mut sql, "SELECT lo_unlink(44001)"), SqlValue::Int(1));
    }
}
