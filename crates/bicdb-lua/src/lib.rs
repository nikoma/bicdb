//! Bounded Lua 5.1 execution shared by the native database and RESP endpoint.
//! Host callbacks supply storage and authority; scripts cannot access the OS.
use mlua::{HookTriggers, Lua, LuaOptions, LuaSerdeExt, StdLib, Value, VmState};
use std::cell::Cell;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    Nil,
    Integer(i64),
    Bulk(Vec<u8>),
    Array(Vec<Reply>),
    Status(String),
    Error(String),
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub memory_bytes: usize,
    pub instructions: u64,
    pub timeout: Duration,
    pub source_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: 64 * 1024 * 1024,
            instructions: 5_000_000,
            timeout: Duration::from_secs(2),
            source_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Lua execution failed: {0}")]
pub struct ScriptError(pub String);

pub fn validate(source: &[u8], limits: &Limits) -> Result<(), ScriptError> {
    if source.len() > limits.source_bytes {
        return Err(ScriptError("script source limit exceeded".into()));
    }
    let lua = Lua::new_with(StdLib::NONE, LuaOptions::default())
        .map_err(|e| ScriptError(e.to_string()))?;
    lua.set_memory_limit(limits.memory_bytes)
        .map_err(|e| ScriptError(e.to_string()))?;
    lua.load(source)
        .set_mode(mlua::ChunkMode::Text)
        .into_function()
        .map(|_| ())
        .map_err(|e| ScriptError(e.to_string()))
}

fn to_lua(lua: &Lua, reply: Reply) -> mlua::Result<Value> {
    Ok(match reply {
        Reply::Nil => Value::Boolean(false),
        Reply::Integer(n) => Value::Number(n as f64),
        Reply::Bulk(s) => Value::String(lua.create_string(s)?),
        Reply::Array(values) => {
            let table = lua.create_table()?;
            for (i, value) in values.into_iter().enumerate() {
                table.raw_set(i + 1, to_lua(lua, value)?)?;
            }
            Value::Table(table)
        }
        Reply::Status(s) | Reply::Error(s) => {
            let table = lua.create_table()?;
            // Error replies are provided by pcall/error_reply separately.
            table.set("ok", s)?;
            Value::Table(table)
        }
    })
}

fn json_to_lua(lua: &Lua, value: serde_json::Value, depth: usize) -> mlua::Result<Value> {
    if depth > 64 {
        return Err(mlua::Error::runtime("JSON nesting limit exceeded"));
    }
    Ok(match value {
        serde_json::Value::Null => lua.null(),
        serde_json::Value::Bool(b) => Value::Boolean(b),
        serde_json::Value::Number(n) => Value::Number(
            n.as_f64()
                .ok_or_else(|| mlua::Error::runtime("JSON number out of range"))?,
        ),
        serde_json::Value::String(s) => Value::String(lua.create_string(s)?),
        serde_json::Value::Array(values) => {
            let t = lua.create_table()?;
            t.set_metatable(Some(lua.array_metatable()))?;
            for (i, value) in values.into_iter().enumerate() {
                t.raw_set(i + 1, json_to_lua(lua, value, depth + 1)?)?;
            }
            Value::Table(t)
        }
        serde_json::Value::Object(values) => {
            let t = lua.create_table()?;
            for (k, value) in values {
                t.raw_set(k, json_to_lua(lua, value, depth + 1)?)?;
            }
            Value::Table(t)
        }
    })
}

fn lua_to_json(
    lua: &Lua,
    value: Value,
    depth: usize,
    bytes: &mut usize,
    items: &mut usize,
) -> mlua::Result<serde_json::Value> {
    if depth > 64 || *items == 0 {
        return Err(mlua::Error::runtime("JSON nesting/item limit exceeded"));
    }
    *items -= 1;
    Ok(match value {
        Value::Nil => serde_json::Value::Null,
        Value::LightUserData(p) if p.0.is_null() => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(b),
        Value::Integer(n) => serde_json::Value::Number(n.into()),
        Value::Number(n) if n.is_finite() => {
            let number = if n.fract() == 0.0 && n >= i64::MIN as f64 && n < -(i64::MIN as f64) {
                serde_json::Number::from(n as i64)
            } else {
                serde_json::Number::from_f64(n)
                    .ok_or_else(|| mlua::Error::runtime("invalid JSON number"))?
            };
            serde_json::Value::Number(number)
        }
        Value::String(s) => {
            *bytes = bytes
                .checked_sub(s.as_bytes().len())
                .ok_or_else(|| mlua::Error::runtime("JSON byte limit exceeded"))?;
            serde_json::Value::String(s.to_str()?.to_string())
        }
        Value::Table(t) => {
            let mut array = std::collections::BTreeMap::new();
            let mut object = serde_json::Map::new();
            let marked_array = t
                .metatable()
                .is_some_and(|m| m.to_pointer() == lua.array_metatable().to_pointer());
            for pair in t.pairs::<Value, Value>() {
                let (key, value) = pair?;
                let value = lua_to_json(lua, value, depth + 1, bytes, items)?;
                match key {
                    Value::String(s) => {
                        *bytes = bytes
                            .checked_sub(s.as_bytes().len())
                            .ok_or_else(|| mlua::Error::runtime("JSON byte limit exceeded"))?;
                        object.insert(s.to_str()?.to_string(), value);
                    }
                    Value::Integer(i) if i > 0 => {
                        array.insert(i as usize, value);
                    }
                    Value::Number(n) if n > 0.0 && n.fract() == 0.0 && n <= 100_000.0 => {
                        array.insert(n as usize, value);
                    }
                    _ => {
                        return Err(mlua::Error::runtime(
                            "JSON table keys must be strings or array indexes",
                        ))
                    }
                }
            }
            if !array.is_empty() || marked_array {
                if !object.is_empty()
                    || array
                        .last_key_value()
                        .is_some_and(|(last, _)| *last != array.len())
                {
                    return Err(mlua::Error::runtime("mixed or sparse JSON array"));
                }
                serde_json::Value::Array(array.into_values().collect())
            } else {
                serde_json::Value::Object(object)
            }
        }
        _ => return Err(mlua::Error::runtime("unsupported JSON value")),
    })
}

fn from_lua(
    value: Value,
    depth: usize,
    remaining: &mut usize,
    bytes: &mut usize,
) -> mlua::Result<Reply> {
    if depth > 64 || *remaining == 0 {
        return Err(mlua::Error::runtime(
            "script reply exceeds nesting/item limit",
        ));
    }
    *remaining -= 1;
    Ok(match value {
        Value::Nil | Value::Boolean(false) => Reply::Nil,
        Value::Boolean(true) => Reply::Integer(1),
        Value::Integer(n) => Reply::Integer(n),
        Value::Number(n) if n.is_finite() && n >= i64::MIN as f64 && n < -(i64::MIN as f64) => {
            Reply::Integer(n as i64)
        }
        Value::String(s) => {
            *bytes = bytes
                .checked_sub(s.as_bytes().len())
                .ok_or_else(|| mlua::Error::runtime("script reply byte limit exceeded"))?;
            Reply::Bulk(s.as_bytes().to_vec())
        }
        Value::Table(t) => {
            if let Some(s) = t.get::<Option<String>>("err")? {
                *bytes = bytes
                    .checked_sub(s.len())
                    .ok_or_else(|| mlua::Error::runtime("script reply byte limit exceeded"))?;
                return Ok(Reply::Error(s));
            }
            if let Some(s) = t.get::<Option<String>>("ok")? {
                *bytes = bytes
                    .checked_sub(s.len())
                    .ok_or_else(|| mlua::Error::runtime("script reply byte limit exceeded"))?;
                return Ok(Reply::Status(s));
            }
            let mut values = Vec::new();
            for i in 1.. {
                let value = t.raw_get::<Value>(i)?;
                if matches!(value, Value::Nil) {
                    break;
                }
                values.push(from_lua(value, depth + 1, remaining, bytes)?);
            }
            Reply::Array(values)
        }
        _ => return Err(mlua::Error::runtime("unsupported Lua reply type")),
    })
}

/// Execute with a borrowed host callback. Each invocation has a fresh VM,
/// isolated globals, no filesystem/network/module loading, and bounded work.
pub fn execute(
    source: &[u8],
    keys: &[Vec<u8>],
    args: &[Vec<u8>],
    limits: &Limits,
    mut command: impl FnMut(Vec<Vec<u8>>) -> Result<Reply, String>,
) -> Result<Reply, ScriptError> {
    let mut run = || -> mlua::Result<Reply> {
        if limits.memory_bytes == 0 || limits.instructions == 0 || limits.timeout.is_zero() {
            return Err(mlua::Error::runtime("script limits must be positive"));
        }
        if source.len() > limits.source_bytes {
            return Err(mlua::Error::runtime("script source limit exceeded"));
        }
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            LuaOptions::default(),
        )?;
        lua.set_memory_limit(limits.memory_bytes)?;
        let deadline = Instant::now()
            .checked_add(limits.timeout)
            .ok_or_else(|| mlua::Error::runtime("invalid script timeout"))?;
        let instructions = Cell::new(0u64);
        let maximum = limits.instructions;
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(1000),
            move |_, _| {
                instructions.set(instructions.get() + 1000);
                if instructions.get() > maximum || Instant::now() >= deadline {
                    return Err(mlua::Error::runtime("script execution limit exceeded"));
                }
                Ok(VmState::Continue)
            },
        )?;
        let globals = lua.globals();
        for name in [
            "dofile",
            "loadfile",
            "load",
            "loadstring",
            "collectgarbage",
            "newproxy",
        ] {
            globals.set(name, Value::Nil)?;
        }
        globals.set(
            "KEYS",
            lua.create_sequence_from(
                keys.iter()
                    .map(|s| lua.create_string(s))
                    .collect::<mlua::Result<Vec<_>>>()?,
            )?,
        )?;
        globals.set(
            "ARGV",
            lua.create_sequence_from(
                args.iter()
                    .map(|s| lua.create_string(s))
                    .collect::<mlua::Result<Vec<_>>>()?,
            )?,
        )?;
        let json = lua.create_table()?;
        json.set(
            "decode",
            lua.create_function(|lua, s: mlua::String| {
                let value: serde_json::Value =
                    serde_json::from_slice(&s.as_bytes()).map_err(mlua::Error::external)?;
                json_to_lua(lua, value, 0)
            })?,
        )?;
        let json_bytes = limits.memory_bytes;
        json.set(
            "encode",
            lua.create_function(move |lua, v: Value| {
                let mut bytes = json_bytes;
                let value = lua_to_json(lua, v, 0, &mut bytes, &mut 100_000)?;
                serde_json::to_string(&value).map_err(mlua::Error::external)
            })?,
        )?;
        json.set("null", lua.null())?;
        globals.set("cjson", json)?;
        lua.scope(|scope| {
            let callback = scope.create_function_mut(|lua, values: mlua::MultiValue| {
                let args = values.into_iter().map(|value| match value {
                    Value::String(s) => Ok(s.as_bytes().to_vec()),
                    Value::Integer(n) => Ok(n.to_string().into_bytes()),
                    Value::Number(n) if n.is_finite() => Ok(n.to_string().into_bytes()),
                    _ => Err(mlua::Error::runtime("command arguments must be strings or numbers")),
                }).collect::<mlua::Result<Vec<_>>>()?;
                match command(args) {
                    Ok(Reply::Error(err)) | Err(err) => Err(mlua::Error::runtime(err)),
                    Ok(reply) => to_lua(lua, reply),
                }
            })?;
            let redis = lua.create_table()?;
            redis.set("call", callback)?;
            globals.set("redis", redis)?;
            lua.load("redis.pcall = function(...) local ok, v = pcall(redis.call, ...); if ok then return v else return {err=tostring(v)} end end; redis.error_reply=function(s) return {err=s} end; redis.status_reply=function(s) return {ok=s} end; db = {call=redis.call}").exec()?;
            let value: Value = lua.load(source).set_mode(mlua::ChunkMode::Text).eval()?;
            let mut reply_bytes = limits.memory_bytes;
            from_lua(value, 0, &mut 100_000, &mut reply_bytes)
        })
    };
    run().map_err(|err| ScriptError(err.to_string()))
}
