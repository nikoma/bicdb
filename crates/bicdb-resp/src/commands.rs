//! Transactional Redis command family used by durable delivery queues.
use crate::store::{AtomicCache, TypedEntry};
use crate::{RespServerError, Result};
use bicdb_lua::Reply;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

type StreamFields = Vec<(Vec<u8>, Vec<u8>)>;
#[derive(Serialize, Deserialize, Default)]
struct Stream {
    last_ms: u64,
    last_sequence: u64,
    entries: Vec<(String, StreamFields)>,
}

fn error(message: impl Into<String>) -> RespServerError {
    RespServerError::Command(message.into())
}
fn wrongtype() -> RespServerError {
    error("WRONGTYPE Operation against a key holding the wrong kind of value")
}
fn integer(bytes: &[u8]) -> Result<i64> {
    let s =
        std::str::from_utf8(bytes).map_err(|_| error("value is not an integer or out of range"))?;
    let n = s
        .parse::<i64>()
        .map_err(|_| error("value is not an integer or out of range"))?;
    if n.to_string() != s {
        return Err(error("value is not an integer or out of range"));
    }
    Ok(n)
}
fn number(bytes: &[u8]) -> Result<f64> {
    let s = std::str::from_utf8(bytes).map_err(|_| error("value is not a valid float"))?;
    let n = s
        .parse::<f64>()
        .map_err(|_| error("value is not a valid float"))?;
    if !n.is_finite() {
        return Err(error("non-finite scores are not supported"));
    }
    Ok(n)
}
fn arity(command: &str, args: &[Vec<u8>], minimum: usize) -> Result<()> {
    if args.len() < minimum {
        return Err(error(format!(
            "wrong number of arguments for '{command}' command"
        )));
    }
    Ok(())
}
fn exact(command: &str, args: &[Vec<u8>], size: usize) -> Result<()> {
    arity(command, args, size)?;
    if args.len() != size {
        return Err(error(format!(
            "wrong number of arguments for '{command}' command"
        )));
    }
    Ok(())
}
fn load<T: serde::de::DeserializeOwned + Default>(
    cache: &AtomicCache<'_>,
    key: &[u8],
    kind: &str,
) -> Result<(T, Option<i64>)> {
    match cache.get(key)? {
        None => Ok((T::default(), None)),
        Some(e) if e.kind.as_deref() == Some(kind) => Ok((
            ciborium::from_reader(e.value.as_slice())
                .map_err(|e| error(format!("corrupt {kind} value: {e}")))?,
            e.expires_at_ms,
        )),
        Some(_) => Err(wrongtype()),
    }
}
fn save<T: Serialize>(
    cache: &mut AtomicCache<'_>,
    key: Vec<u8>,
    kind: &str,
    value: &T,
    expiry: Option<i64>,
) -> Result<()> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).map_err(|e| error(e.to_string()))?;
    cache.put(
        key,
        Some(TypedEntry {
            value: bytes,
            kind: Some(kind.into()),
            expires_at_ms: expiry,
        }),
    )?;
    Ok(())
}
fn string(cache: &AtomicCache<'_>, key: &[u8]) -> Result<Option<TypedEntry>> {
    let entry = cache.get(key)?;
    if entry.as_ref().is_some_and(|e| e.kind.is_some()) {
        return Err(wrongtype());
    }
    Ok(entry)
}
fn bulk(value: Vec<u8>) -> Reply {
    Reply::Bulk(value)
}
fn flat_pairs(items: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>) -> Reply {
    Reply::Array(
        items
            .into_iter()
            .flat_map(|(k, v)| [bulk(k), bulk(v)])
            .collect(),
    )
}

pub(crate) fn supported(command: &str) -> bool {
    matches!(
        command,
        "HSET"
            | "HSETNX"
            | "HMSET"
            | "HGET"
            | "HMGET"
            | "HGETALL"
            | "HKEYS"
            | "HVALS"
            | "HLEN"
            | "HEXISTS"
            | "HDEL"
            | "HINCRBY"
            | "ZADD"
            | "ZREM"
            | "ZSCORE"
            | "ZCARD"
            | "ZRANGE"
            | "ZREVRANGE"
            | "ZRANGEBYSCORE"
            | "ZREVRANGEBYSCORE"
            | "ZCOUNT"
            | "XADD"
            | "XRANGE"
            | "XREVRANGE"
            | "XLEN"
            | "XDEL"
            | "XTRIM"
            | "TIME"
            | "TYPE"
    )
}

pub(crate) fn execute(cache: &mut AtomicCache<'_>, args: &[Vec<u8>]) -> Result<Reply> {
    arity("script", args, 1)?;
    let command = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let a = &args[1..];
    match command.as_str() {
        "TIME" => {
            exact(&command, a, 0)?;
            Ok(Reply::Array(vec![
                bulk((cache.now / 1000).to_string().into_bytes()),
                bulk(((cache.now % 1000) * 1000).to_string().into_bytes()),
            ]))
        }
        "TYPE" => {
            exact(&command, a, 1)?;
            Ok(Reply::Status(
                cache
                    .get(&a[0])?
                    .map_or("none".into(), |e| e.kind.unwrap_or("string".into())),
            ))
        }
        "GET" => {
            exact(&command, a, 1)?;
            Ok(string(cache, &a[0])?.map_or(Reply::Nil, |e| bulk(e.value)))
        }
        "SET" => {
            arity(&command, a, 2)?;
            let old = cache.get(&a[0])?;
            let mut expiry = None;
            let (mut nx, mut xx, mut get, mut keep) = (false, false, false, false);
            let mut at = 2;
            while at < a.len() {
                let option = a[at].to_ascii_uppercase();
                match option.as_slice() {
                    b"NX" if !nx && !xx => nx = true,
                    b"XX" if !nx && !xx => xx = true,
                    b"GET" if !get => get = true,
                    b"KEEPTTL" if !keep && expiry.is_none() => keep = true,
                    b"EX" | b"PX" | b"EXAT" | b"PXAT"
                        if !keep && expiry.is_none() && at + 1 < a.len() =>
                    {
                        at += 1;
                        let n = integer(&a[at])?;
                        if n <= 0 {
                            return Err(error("invalid expire time in 'set' command"));
                        }
                        let scale = if option == b"EX" || option == b"EXAT" {
                            1000
                        } else {
                            1
                        };
                        let n = n
                            .checked_mul(scale)
                            .ok_or_else(|| error("invalid expire time"))?;
                        expiry = Some(if option == b"EX" || option == b"PX" {
                            cache
                                .now
                                .checked_add(n)
                                .ok_or_else(|| error("invalid expire time"))?
                        } else {
                            n
                        });
                    }
                    _ => return Err(error("syntax error")),
                }
                at += 1;
            }
            if get && old.as_ref().is_some_and(|e| e.kind.is_some()) {
                return Err(wrongtype());
            }
            let reply = if get {
                old.as_ref().map_or(Reply::Nil, |e| bulk(e.value.clone()))
            } else {
                Reply::Status("OK".into())
            };
            if (nx && old.is_some()) || (xx && old.is_none()) {
                return Ok(if get { reply } else { Reply::Nil });
            }
            if keep {
                expiry = old.and_then(|e| e.expires_at_ms);
            }
            cache.put(
                a[0].clone(),
                Some(TypedEntry {
                    value: a[1].clone(),
                    kind: None,
                    expires_at_ms: expiry,
                }),
            )?;
            Ok(reply)
        }
        "INCR" | "DECR" | "INCRBY" | "DECRBY" => {
            exact(&command, a, if command.ends_with("BY") { 2 } else { 1 })?;
            let old = string(cache, &a[0])?;
            let current = old
                .as_ref()
                .map(|e| integer(&e.value))
                .transpose()?
                .unwrap_or(0);
            let delta = if a.len() == 2 { integer(&a[1])? } else { 1 };
            let value = if command.starts_with("DECR") {
                current.checked_sub(delta)
            } else {
                current.checked_add(delta)
            }
            .ok_or_else(|| error("increment or decrement would overflow"))?;
            cache.put(
                a[0].clone(),
                Some(TypedEntry {
                    value: value.to_string().into_bytes(),
                    kind: None,
                    expires_at_ms: old.and_then(|e| e.expires_at_ms),
                }),
            )?;
            Ok(Reply::Integer(value))
        }
        "DEL" | "UNLINK" | "EXISTS" => {
            arity(&command, a, 1)?;
            let mut count = 0;
            for key in a {
                if cache.get(key)?.is_some() {
                    count += 1;
                    if command != "EXISTS" {
                        cache.put(key.clone(), None)?;
                    }
                }
            }
            Ok(Reply::Integer(count))
        }
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
            exact(&command, a, 2)?;
            let n = integer(&a[1])?;
            let scaled = if command.starts_with('P') {
                n
            } else {
                n.checked_mul(1000)
                    .ok_or_else(|| error("expiry overflow"))?
            };
            let deadline = if command.ends_with("AT") {
                scaled
            } else {
                cache
                    .now
                    .checked_add(scaled)
                    .ok_or_else(|| error("expiry overflow"))?
            };
            let Some(mut e) = cache.get(&a[0])? else {
                return Ok(Reply::Integer(0));
            };
            e.expires_at_ms = Some(deadline);
            cache.put(
                a[0].clone(),
                if deadline <= cache.now { None } else { Some(e) },
            )?;
            Ok(Reply::Integer(1))
        }
        "TTL" | "PTTL" | "PERSIST" => {
            exact(&command, a, 1)?;
            let Some(mut e) = cache.get(&a[0])? else {
                return Ok(Reply::Integer(if command == "PERSIST" { 0 } else { -2 }));
            };
            if command == "PERSIST" {
                let changed = e.expires_at_ms.take().is_some();
                if changed {
                    cache.put(a[0].clone(), Some(e))?;
                }
                return Ok(Reply::Integer(i64::from(changed)));
            }
            let n = e.expires_at_ms.map_or(-1, |d| {
                if command == "TTL" {
                    (d - cache.now + 500) / 1000
                } else {
                    d - cache.now
                }
            });
            Ok(Reply::Integer(n))
        }
        "HSET" | "HMSET" | "HSETNX" => {
            arity(&command, a, 3)?;
            if a.len() % 2 != 1 || (command == "HSETNX" && a.len() != 3) {
                return Err(error("wrong number of arguments for hash command"));
            }
            let (mut hash, expiry) = load::<BTreeMap<Vec<u8>, Vec<u8>>>(cache, &a[0], "hash")?;
            if command == "HSETNX" && hash.contains_key(&a[1]) {
                return Ok(Reply::Integer(0));
            }
            let mut created = 0;
            for pair in a[1..].chunks_exact(2) {
                created += i64::from(hash.insert(pair[0].clone(), pair[1].clone()).is_none());
            }
            save(cache, a[0].clone(), "hash", &hash, expiry)?;
            Ok(if command == "HMSET" {
                Reply::Status("OK".into())
            } else {
                Reply::Integer(created)
            })
        }
        "HGET" | "HEXISTS" | "HMGET" | "HDEL" | "HINCRBY" | "HGETALL" | "HKEYS" | "HVALS"
        | "HLEN" => {
            let minimum = match command.as_str() {
                "HGETALL" | "HKEYS" | "HVALS" | "HLEN" => 1,
                "HINCRBY" => 3,
                _ => 2,
            };
            if command == "HMGET" || command == "HDEL" {
                arity(&command, a, minimum)?;
            } else {
                exact(&command, a, minimum)?;
            }
            let (mut hash, expiry) = load::<BTreeMap<Vec<u8>, Vec<u8>>>(cache, &a[0], "hash")?;
            match command.as_str() {
                "HGET" => Ok(hash.get(&a[1]).cloned().map_or(Reply::Nil, bulk)),
                "HEXISTS" => Ok(Reply::Integer(i64::from(hash.contains_key(&a[1])))),
                "HMGET" => Ok(Reply::Array(
                    a[1..]
                        .iter()
                        .map(|k| hash.get(k).cloned().map_or(Reply::Nil, bulk))
                        .collect(),
                )),
                "HGETALL" => Ok(flat_pairs(hash)),
                "HKEYS" => Ok(Reply::Array(hash.into_keys().map(bulk).collect())),
                "HVALS" => Ok(Reply::Array(hash.into_values().map(bulk).collect())),
                "HLEN" => Ok(Reply::Integer(hash.len() as i64)),
                "HDEL" => {
                    let removed = a[1..].iter().filter(|k| hash.remove(*k).is_some()).count();
                    if removed > 0 {
                        if hash.is_empty() {
                            cache.put(a[0].clone(), None)?;
                        } else {
                            save(cache, a[0].clone(), "hash", &hash, expiry)?;
                        }
                    }
                    Ok(Reply::Integer(removed as i64))
                }
                _ => {
                    let old = hash
                        .get(&a[1])
                        .map(|v| integer(v))
                        .transpose()?
                        .unwrap_or(0);
                    let value = old
                        .checked_add(integer(&a[2])?)
                        .ok_or_else(|| error("increment or decrement would overflow"))?;
                    hash.insert(a[1].clone(), value.to_string().into_bytes());
                    save(cache, a[0].clone(), "hash", &hash, expiry)?;
                    Ok(Reply::Integer(value))
                }
            }
        }
        "ZADD" => {
            arity(&command, a, 3)?;
            let mut at = 1;
            let (mut nx, mut xx, mut changed, mut increment, mut gt, mut lt) =
                (false, false, false, false, false, false);
            while at < a.len() {
                match a[at].to_ascii_uppercase().as_slice() {
                    b"NX" => nx = true,
                    b"XX" => xx = true,
                    b"CH" => changed = true,
                    b"INCR" => increment = true,
                    b"GT" => gt = true,
                    b"LT" => lt = true,
                    _ => break,
                }
                at += 1;
            }
            if nx && (xx || gt || lt)
                || gt && lt
                || at >= a.len()
                || !(a.len() - at).is_multiple_of(2)
                || increment && a.len() - at != 2
            {
                return Err(error("syntax error"));
            }
            let pairs = a[at..]
                .chunks_exact(2)
                .map(|p| Ok((number(&p[0])?, p[1].clone())))
                .collect::<Result<Vec<_>>>()?;
            let (mut set, expiry) = load::<BTreeMap<Vec<u8>, f64>>(cache, &a[0], "zset")?;
            let mut count = 0;
            let mut increment_reply = Reply::Nil;
            for (mut score, member) in pairs {
                let old = set.get(&member).copied();
                if increment {
                    score += old.unwrap_or(0.0);
                    if !score.is_finite() {
                        return Err(error("resulting score is not finite"));
                    }
                }
                if nx && old.is_some()
                    || xx && old.is_none()
                    || gt && old.is_some_and(|n| score <= n)
                    || lt && old.is_some_and(|n| score >= n)
                {
                    continue;
                }
                count += i64::from(old.is_none() || changed && old != Some(score));
                set.insert(member, score);
                if increment {
                    increment_reply = bulk(score.to_string().into_bytes());
                }
            }
            if !set.is_empty() {
                save(cache, a[0].clone(), "zset", &set, expiry)?;
            }
            Ok(if increment {
                increment_reply
            } else {
                Reply::Integer(count)
            })
        }
        "ZREM" | "ZSCORE" | "ZCARD" => {
            if command == "ZREM" {
                arity(&command, a, 2)?;
            } else {
                exact(&command, a, if command == "ZSCORE" { 2 } else { 1 })?;
            }
            let (mut set, expiry) = load::<BTreeMap<Vec<u8>, f64>>(cache, &a[0], "zset")?;
            match command.as_str() {
                "ZCARD" => Ok(Reply::Integer(set.len() as i64)),
                "ZSCORE" => Ok(set
                    .get(&a[1])
                    .map_or(Reply::Nil, |v| bulk(v.to_string().into_bytes()))),
                _ => {
                    let removed = a[1..].iter().filter(|k| set.remove(*k).is_some()).count();
                    if removed > 0 {
                        if set.is_empty() {
                            cache.put(a[0].clone(), None)?;
                        } else {
                            save(cache, a[0].clone(), "zset", &set, expiry)?;
                        }
                    }
                    Ok(Reply::Integer(removed as i64))
                }
            }
        }
        "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZCOUNT" => {
            zrange(cache, &command, a)
        }
        "XADD" | "XRANGE" | "XREVRANGE" | "XLEN" | "XDEL" | "XTRIM" => stream(cache, &command, a),
        _ => Err(error(format!(
            "command '{command}' is not allowed in scripts"
        ))),
    }
}

fn bound(bytes: &[u8]) -> Result<(f64, bool)> {
    let (raw, exclusive) = if bytes.first() == Some(&b'(') {
        (&bytes[1..], true)
    } else {
        (bytes, false)
    };
    let n = match raw {
        b"-inf" => f64::NEG_INFINITY,
        b"+inf" | b"inf" => f64::INFINITY,
        _ => number(raw)?,
    };
    Ok((n, exclusive))
}
fn zrange(cache: &AtomicCache<'_>, command: &str, a: &[Vec<u8>]) -> Result<Reply> {
    arity(command, a, 3)?;
    let (set, _) = load::<BTreeMap<Vec<u8>, f64>>(cache, &a[0], "zset")?;
    let mut score_range = command.contains("BYSCORE") || command == "ZCOUNT";
    let mut reverse = command.contains("REV");
    let mut scores = false;
    let mut limit = None;
    let mut at = 3;
    while at < a.len() {
        match a[at].to_ascii_uppercase().as_slice() {
            b"WITHSCORES" if !scores && command != "ZCOUNT" => scores = true,
            b"BYSCORE" if command == "ZRANGE" && !score_range => score_range = true,
            b"REV" if command == "ZRANGE" && !reverse => reverse = true,
            b"LIMIT" if limit.is_none() && command != "ZCOUNT" && at + 2 < a.len() => {
                let offset = integer(&a[at + 1])?;
                let count = integer(&a[at + 2])?;
                if offset < 0 {
                    return Err(error("offset is out of range"));
                }
                limit = Some((offset as usize, count));
                at += 2;
            }
            _ => return Err(error("syntax error")),
        }
        at += 1;
    }
    if limit.is_some() && !score_range {
        return Err(error("LIMIT is only supported with BYSCORE"));
    }
    let mut items: Vec<_> = set.into_iter().collect();
    items.sort_by(|(ka, sa), (kb, sb)| sa.total_cmp(sb).then_with(|| ka.cmp(kb)));
    if reverse {
        items.reverse();
    }
    let items = if score_range {
        let (min, max) = if reverse {
            (bound(&a[2])?, bound(&a[1])?)
        } else {
            (bound(&a[1])?, bound(&a[2])?)
        };
        let (offset, count) = limit.unwrap_or((0, -1));
        items
            .into_iter()
            .filter(|(_, n)| {
                (if min.1 { *n > min.0 } else { *n >= min.0 })
                    && (if max.1 { *n < max.0 } else { *n <= max.0 })
            })
            .skip(offset)
            .take(if count < 0 {
                usize::MAX
            } else {
                count as usize
            })
            .collect::<Vec<_>>()
    } else {
        let len = items.len() as i64;
        let start = integer(&a[1])?;
        let end = integer(&a[2])?;
        let start = if start < 0 {
            len.saturating_add(start)
        } else {
            start
        }
        .max(0);
        let end = if end < 0 {
            len.saturating_add(end)
        } else {
            end
        };
        if start >= len || end < start {
            Vec::new()
        } else {
            items
                .into_iter()
                .skip(start as usize)
                .take((end.min(len - 1) - start + 1) as usize)
                .collect()
        }
    };
    if command == "ZCOUNT" {
        return Ok(Reply::Integer(items.len() as i64));
    }
    Ok(Reply::Array(
        items
            .into_iter()
            .flat_map(|(k, n)| {
                if scores {
                    vec![bulk(k), bulk(n.to_string().into_bytes())]
                } else {
                    vec![bulk(k)]
                }
            })
            .collect(),
    ))
}

fn stream_id(raw: &[u8], high: bool) -> Result<(u64, u64)> {
    if raw == b"-" {
        return Ok((0, 0));
    }
    if raw == b"+" {
        return Ok((u64::MAX, u64::MAX));
    }
    let text = std::str::from_utf8(raw)
        .map_err(|_| error("Invalid stream ID specified as stream command argument"))?;
    let (ms, seq) = text
        .split_once('-')
        .unwrap_or((text, if high { "18446744073709551615" } else { "0" }));
    Ok((
        ms.parse().map_err(|_| error("Invalid stream ID"))?,
        seq.parse().map_err(|_| error("Invalid stream ID"))?,
    ))
}
fn stream(cache: &mut AtomicCache<'_>, command: &str, a: &[Vec<u8>]) -> Result<Reply> {
    arity(command, a, 1)?;
    let (mut stream, expiry) = load::<Stream>(cache, &a[0], "stream")?;
    match command {
        "XLEN" => {
            exact(command, a, 1)?;
            Ok(Reply::Integer(stream.entries.len() as i64))
        }
        "XADD" => {
            arity(command, a, 4)?;
            let mut at = 1;
            let mut maxlen = None;
            if a[at].eq_ignore_ascii_case(b"MAXLEN") {
                at += 1;
                if a.get(at).is_some_and(|s| s == b"~" || s == b"=") {
                    at += 1;
                }
                let n = integer(a.get(at).ok_or_else(|| error("syntax error"))?)?;
                if n < 0 {
                    return Err(error("MAXLEN must be non-negative"));
                }
                maxlen = Some(n as usize);
                at += 1;
            }
            if a.len() < at + 3 || !(a.len() - at - 1).is_multiple_of(2) {
                return Err(error("wrong number of arguments for 'xadd' command"));
            }
            let id = if a[at] == b"*" {
                let ms = (cache.now.max(0) as u64).max(stream.last_ms);
                let seq = if ms == stream.last_ms {
                    stream
                        .last_sequence
                        .checked_add(1)
                        .ok_or_else(|| error("stream ID overflow"))?
                } else {
                    0
                };
                (ms, seq)
            } else {
                stream_id(&a[at], false)?
            };
            if id == (0, 0) || id <= (stream.last_ms, stream.last_sequence) {
                return Err(error(
                    "The ID specified in XADD is equal or smaller than the target stream top item",
                ));
            }
            let text = format!("{}-{}", id.0, id.1);
            stream.last_ms = id.0;
            stream.last_sequence = id.1;
            stream.entries.push((
                text.clone(),
                a[at + 1..]
                    .chunks_exact(2)
                    .map(|p| (p[0].clone(), p[1].clone()))
                    .collect(),
            ));
            if let Some(max) = maxlen {
                let remove = stream.entries.len().saturating_sub(max);
                stream.entries.drain(..remove);
            }
            save(cache, a[0].clone(), "stream", &stream, expiry)?;
            Ok(bulk(text.into_bytes()))
        }
        "XRANGE" | "XREVRANGE" => {
            arity(command, a, 3)?;
            let count = if a.len() == 3 {
                usize::MAX
            } else if a.len() == 5 && a[3].eq_ignore_ascii_case(b"COUNT") {
                let n = integer(&a[4])?;
                if n < 0 {
                    return Err(error("COUNT must be non-negative"));
                }
                n as usize
            } else {
                return Err(error("syntax error"));
            };
            let reverse = command == "XREVRANGE";
            let start = if reverse { &a[2] } else { &a[1] };
            let end = if reverse { &a[1] } else { &a[2] };
            let start_exclusive = start.first() == Some(&b'(');
            let end_exclusive = end.first() == Some(&b'(');
            let min = stream_id(if start_exclusive { &start[1..] } else { start }, false)?;
            let max = stream_id(if end_exclusive { &end[1..] } else { end }, true)?;
            if reverse {
                stream.entries.reverse();
            }
            let mut replies = Vec::new();
            for (id, fields) in stream.entries {
                let pair = stream_id(id.as_bytes(), false)?;
                if (if start_exclusive {
                    pair > min
                } else {
                    pair >= min
                }) && (if end_exclusive {
                    pair < max
                } else {
                    pair <= max
                }) && replies.len() < count
                {
                    replies.push(Reply::Array(vec![
                        bulk(id.into_bytes()),
                        flat_pairs(fields),
                    ]));
                }
            }
            Ok(Reply::Array(replies))
        }
        "XDEL" => {
            arity(command, a, 2)?;
            let ids = a[1..]
                .iter()
                .map(|id| stream_id(id, false))
                .collect::<Result<Vec<_>>>()?;
            let old = stream.entries.len();
            stream.entries.retain(|(id, _)| {
                !ids.contains(&stream_id(id.as_bytes(), false).expect("stored stream ID"))
            });
            let removed = old - stream.entries.len();
            if removed > 0 {
                save(cache, a[0].clone(), "stream", &stream, expiry)?;
            }
            Ok(Reply::Integer(removed as i64))
        }
        _ => {
            arity(command, a, 3)?;
            if !a[1].eq_ignore_ascii_case(b"MAXLEN") {
                return Err(error("only MAXLEN stream trimming is supported"));
            }
            let at = if a[2] == b"~" || a[2] == b"=" { 3 } else { 2 };
            exact(command, a, at + 1)?;
            let max = integer(&a[at])?;
            if max < 0 {
                return Err(error("MAXLEN must be non-negative"));
            }
            let removed = stream.entries.len().saturating_sub(max as usize);
            if removed > 0 {
                stream.entries.drain(..removed);
                save(cache, a[0].clone(), "stream", &stream, expiry)?;
            }
            Ok(Reply::Integer(removed as i64))
        }
    }
}
