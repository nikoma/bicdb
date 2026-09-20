//! RESP2 wire protocol: request parsing and reply encoding.
//!
//! Requests arrive as arrays of bulk strings (`*N\r\n$len\r\n...\r\n`); a
//! plain-text inline form (`GET foo\r\n`) is also accepted so the server can be
//! poked with netcat/telnet. Replies are written into a caller-owned buffer so
//! a pipelined batch flushes as one syscall.

use std::io::{self, BufRead};

/// Upper bound on a single bulk-string argument (values included).
pub const MAX_BULK_LEN: usize = 64 * 1024 * 1024;
/// Upper bound on the number of arguments in one command.
pub const MAX_ARGS: usize = 1024 * 1024;

/// Read one client command. Returns `Ok(None)` on clean EOF at a command
/// boundary. Protocol violations surface as `InvalidData` errors; the caller
/// should drop the connection.
pub fn read_command<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<Vec<u8>>>> {
    let first = match peek_byte(reader)? {
        Some(byte) => byte,
        None => return Ok(None),
    };
    if first != b'*' {
        return read_inline_command(reader);
    }
    let header = read_line(reader)?;
    let count = parse_i64(&header[1..], "invalid multibulk length")?;
    if count <= 0 {
        // "*0\r\n" and negative counts: treat as no command, keep the connection.
        return Ok(Some(Vec::new()));
    }
    if count as usize > MAX_ARGS {
        return Err(protocol_error("invalid multibulk length"));
    }
    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let line = read_line(reader)?;
        if line.first() != Some(&b'$') {
            return Err(protocol_error("expected '$', got something else"));
        }
        let len = parse_i64(&line[1..], "invalid bulk length")?;
        if !(0..=MAX_BULK_LEN as i64).contains(&len) {
            return Err(protocol_error("invalid bulk length"));
        }
        let mut buf = vec![0u8; len as usize + 2];
        reader.read_exact(&mut buf)?;
        if &buf[len as usize..] != b"\r\n" {
            return Err(protocol_error("bulk string missing CRLF terminator"));
        }
        buf.truncate(len as usize);
        args.push(buf);
    }
    Ok(Some(args))
}

/// Inline commands: a single line of whitespace-separated words. No quoting —
/// this exists for human debugging, not for real clients.
fn read_inline_command<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<Vec<u8>>>> {
    let line = read_line(reader)?;
    let args: Vec<Vec<u8>> = line
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|word| !word.is_empty())
        .map(|word| word.to_vec())
        .collect();
    Ok(Some(args))
}

fn peek_byte<R: BufRead>(reader: &mut R) -> io::Result<Option<u8>> {
    loop {
        match reader.fill_buf() {
            Ok(buf) if buf.is_empty() => return Ok(None),
            Ok(buf) => return Ok(Some(buf[0])),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
}

/// Read a CRLF-terminated line (without the terminator), bounded to keep a
/// malicious peer from ballooning memory.
fn read_line<R: BufRead>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        match reader.read_exact(&mut byte) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof && !line.is_empty() => {
                return Err(protocol_error("unexpected EOF mid-line"));
            }
            Err(err) => return Err(err),
        }
        if byte[0] == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(line);
        }
        line.push(byte[0]);
        if line.len() > 64 * 1024 {
            return Err(protocol_error("protocol line too long"));
        }
    }
}

fn parse_i64(bytes: &[u8], message: &str) -> io::Result<i64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.trim().parse::<i64>().ok())
        .ok_or_else(|| protocol_error(message))
}

fn protocol_error(message: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("Protocol error: {message}"),
    )
}

// ---- reply encoding ----

pub fn write_simple(out: &mut Vec<u8>, text: &str) {
    out.push(b'+');
    out.extend_from_slice(text.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn write_error(out: &mut Vec<u8>, text: &str) {
    out.push(b'-');
    out.extend_from_slice(text.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn write_int(out: &mut Vec<u8>, value: i64) {
    out.push(b':');
    out.extend_from_slice(value.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn write_bulk(out: &mut Vec<u8>, value: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(value.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

pub fn write_nil(out: &mut Vec<u8>) {
    out.extend_from_slice(b"$-1\r\n");
}

pub fn write_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    fn parse(input: &[u8]) -> io::Result<Option<Vec<Vec<u8>>>> {
        read_command(&mut BufReader::new(input))
    }

    #[test]
    fn parses_multibulk_command() {
        let args = parse(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nhi\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(args, vec![b"SET".to_vec(), b"k".to_vec(), b"hi".to_vec()]);
    }

    #[test]
    fn parses_binary_safe_bulk() {
        let args = parse(b"*2\r\n$3\r\nGET\r\n$4\r\na\r\nb\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(args[1], b"a\r\nb".to_vec());
    }

    #[test]
    fn parses_inline_command() {
        let args = parse(b"PING extra\r\n").unwrap().unwrap();
        assert_eq!(args, vec![b"PING".to_vec(), b"extra".to_vec()]);
    }

    #[test]
    fn eof_returns_none() {
        assert!(parse(b"").unwrap().is_none());
    }

    #[test]
    fn rejects_garbage_bulk_header() {
        assert!(parse(b"*1\r\n%3\r\nfoo\r\n").is_err());
    }
}
