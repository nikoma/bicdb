//! Native scripts run inside one ordinary database transaction. The existing
//! collection access checks and conflict detection apply to every operation.
use crate::{BicDb, Record};
pub use bicdb_lua::{Limits as LuaLimits, Reply as LuaReply, ScriptError as LuaError};

impl BicDb {
    /// Trusted local API (not a SQL/RLS network entry point). Collections must
    /// already exist. GET returns a JSON Record; PUT accepts a JSON Record;
    /// DELETE/EXISTS take collection and id. Any script failure rolls back.
    pub fn eval_lua(
        &mut self,
        source: &[u8],
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
        limits: &LuaLimits,
    ) -> Result<LuaReply, LuaError> {
        let mut tx = self
            .begin_transaction()
            .map_err(|e| LuaError(e.to_string()))?;
        let reply = bicdb_lua::execute(source, keys, args, limits, |args| {
            if args.len() != 3 {
                return Err("db.call requires command, collection, and record/id".into());
            }
            let command = std::str::from_utf8(&args[0])
                .map_err(|e| e.to_string())?
                .to_ascii_uppercase();
            let collection = std::str::from_utf8(&args[1]).map_err(|e| e.to_string())?;
            match command.as_str() {
                "GET" | "EXISTS" | "DELETE" => {
                    let id = std::str::from_utf8(&args[2]).map_err(|e| e.to_string())?;
                    let record = tx.get(collection, id).map_err(|e| e.to_string())?;
                    if command == "GET" {
                        return record
                            .map(|r| {
                                serde_json::to_vec(r.as_ref())
                                    .map(LuaReply::Bulk)
                                    .map_err(|e| e.to_string())
                            })
                            .unwrap_or(Ok(LuaReply::Nil));
                    }
                    if command == "DELETE" && record.is_some() {
                        tx.delete(collection, id).map_err(|e| e.to_string())?;
                    }
                    Ok(LuaReply::Integer(i64::from(record.is_some())))
                }
                "PUT" => {
                    let record: Record =
                        serde_json::from_slice(&args[2]).map_err(|e| e.to_string())?;
                    tx.insert(collection, record).map_err(|e| e.to_string())?;
                    Ok(LuaReply::Status("OK".into()))
                }
                _ => Err(format!(
                    "unsupported native database script command: {command}"
                )),
            }
        });
        match reply {
            Ok(LuaReply::Error(error)) => {
                tx.rollback().map_err(|e| LuaError(e.to_string()))?;
                Err(LuaError(error))
            }
            Ok(reply) => {
                tx.commit().map_err(|e| LuaError(e.to_string()))?;
                Ok(reply)
            }
            Err(error) => {
                tx.rollback()
                    .map_err(|e| LuaError(format!("{error}; rollback failed: {e}")))?;
                Err(error)
            }
        }
    }
}
