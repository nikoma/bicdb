use crate::{commands, resp, CacheStore, RespServerError, Result};
use bicdb_lua::Reply;
use sha1::{Digest, Sha1};

pub(crate) fn write_reply(out: &mut Vec<u8>, reply: Reply) {
    match reply {
        Reply::Nil => resp::write_nil(out),
        Reply::Integer(n) => resp::write_int(out, n),
        Reply::Bulk(s) => resp::write_bulk(out, &s),
        Reply::Status(s) => resp::write_simple(out, &s),
        Reply::Error(s) => resp::write_error(out, &s),
        Reply::Array(values) => {
            resp::write_array_header(out, values.len());
            for v in values {
                write_reply(out, v);
            }
        }
    }
}

fn error(s: impl Into<String>) -> RespServerError {
    RespServerError::Command(s.into())
}
fn cache(store: &CacheStore, source: &[u8]) -> Result<String> {
    bicdb_lua::validate(source, &bicdb_lua::Limits::default()).map_err(|e| error(e.to_string()))?;
    let digest = format!("{:x}", Sha1::digest(source));
    let mut scripts = store.scripts.lock();
    if !scripts.contains_key(&digest) {
        if scripts.len() >= 1024
            || scripts
                .values()
                .map(Vec::len)
                .sum::<usize>()
                .saturating_add(source.len())
                > 16 * 1024 * 1024
        {
            return Err(error("OOM script cache limit exceeded"));
        }
        scripts.insert(digest.clone(), source.to_vec());
    }
    Ok(digest)
}

pub(crate) fn dispatch(store: &CacheStore, index: u8, args: &[Vec<u8>]) -> Result<Reply> {
    let command = args[0].to_ascii_uppercase();
    let a = &args[1..];
    if command == b"SCRIPT" {
        let Some(sub) = a.first() else {
            return Err(error("wrong number of arguments for 'script' command"));
        };
        return match sub.to_ascii_uppercase().as_slice() {
            b"LOAD" if a.len() == 2 => Ok(Reply::Bulk(cache(store, &a[1])?.into_bytes())),
            b"EXISTS" if a.len() >= 2 => {
                let scripts = store.scripts.lock();
                Ok(Reply::Array(
                    a[1..]
                        .iter()
                        .map(|s| {
                            Reply::Integer(i64::from(
                                std::str::from_utf8(s)
                                    .ok()
                                    .is_some_and(|s| scripts.contains_key(s)),
                            ))
                        })
                        .collect(),
                ))
            }
            b"FLUSH"
                if a.len() == 1
                    || a.len() == 2
                        && (a[1].eq_ignore_ascii_case(b"SYNC")
                            || a[1].eq_ignore_ascii_case(b"ASYNC")) =>
            {
                store.scripts.lock().clear();
                Ok(Reply::Status("OK".into()))
            }
            _ => Err(error("unsupported SCRIPT subcommand or invalid arguments")),
        };
    }
    if a.len() < 2 {
        return Err(error("wrong number of arguments for script command"));
    }
    let count: usize = std::str::from_utf8(&a[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| error("Number of keys can't be negative or invalid"))?;
    if count > a.len() - 2 {
        return Err(error("Number of keys can't be greater than number of args"));
    }
    let source = if command == b"EVALSHA" {
        let digest = std::str::from_utf8(&a[0])
            .map_err(|_| error("NOSCRIPT No matching script. Please use EVAL."))?;
        store
            .scripts
            .lock()
            .get(digest)
            .cloned()
            .ok_or_else(|| error("NOSCRIPT No matching script. Please use EVAL."))?
    } else {
        cache(store, &a[0])?;
        a[0].clone()
    };
    store.atomic(index, |db| {
        let reply = bicdb_lua::execute(
            &source,
            &a[2..2 + count],
            &a[2 + count..],
            &bicdb_lua::Limits::default(),
            |args| commands::execute(db, &args).map_err(|e| e.to_string()),
        )
        .map_err(|e| error(e.to_string()))?;
        if let Reply::Error(s) = reply {
            return Err(error(s));
        }
        Ok(reply)
    })
}
