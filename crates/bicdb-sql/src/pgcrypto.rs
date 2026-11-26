//! pgcrypto's password hashing: `crypt(password, setting)` and
//! `gen_salt(type [, rounds])`, producing and accepting the same hash strings
//! as PostgreSQL's pgcrypto (bcrypt `$2a$`, md5-crypt `$1$`, extended DES
//! `_`, traditional DES, and PostgreSQL 18's sha256crypt `$5$` and
//! sha512crypt `$6$`).
use crate::*;

/// The crypt(3) base-64 alphabet used by md5-crypt and the DES variants.
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
/// bcrypt's own base-64 alphabet.
const BF_ITOA64: &[u8; 64] = b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// Evaluates `crypt` and `gen_salt`; `None` for any other function.
pub(crate) fn eval_pgcrypto_function(name: &str, args: &[SqlValue]) -> Result<Option<SqlValue>> {
    let bare = name.strip_prefix("public.").unwrap_or(name);
    match bare {
        "crypt" => {
            require_arg_count(name, args, 2)?;
            if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            crypt(&args[0].to_cell(), &args[1].to_cell()).map(|hash| Some(SqlValue::String(hash)))
        }
        "gen_salt" => {
            if args.is_empty() || args.len() > 2 {
                return Err(SqlError::InvalidSql(format!(
                    "function gen_salt expects 1 or 2 arguments, got {}",
                    args.len()
                )));
            }
            if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            let rounds = match args.get(1) {
                Some(value) => value.to_cell().trim().parse::<i64>().map_err(|_| {
                    SqlError::invalid_text_representation(
                        "integer",
                        format!("\"{}\"", value.to_cell()),
                    )
                })?,
                None => 0,
            };
            gen_salt(&args[0].to_cell(), rounds).map(|salt| Some(SqlValue::String(salt)))
        }
        _ => Ok(None),
    }
}

fn crypt_failed() -> SqlError {
    SqlError::RaisedException {
        sqlstate: "22023".into(),
        message: "invalid salt".into(),
        detail: None,
    }
}

fn gen_salt_error(message: &str) -> SqlError {
    SqlError::invalid_parameter_value(format!("gen_salt: {message}"))
}

/// pgcrypto's `crypt`: the algorithm is chosen by the setting's prefix, in
/// PostgreSQL's order (`$2a$`, `$2x$`, `$2$`, `$1$`, `$5$`, `$6$`, `_`, and
/// traditional DES for anything else).
// The DES variants are marked deprecated upstream as weak, but pgcrypto
// still offers them, so they are implemented for compatibility.
#[allow(deprecated)]
pub(crate) fn crypt(password: &str, setting: &str) -> Result<String> {
    let password = password.as_bytes();
    if setting.starts_with("$2a$") || setting.starts_with("$2x$") {
        return bcrypt(password, setting);
    }
    if setting.starts_with("$2$") {
        return Err(crypt_failed());
    }
    if setting.starts_with("$1$") {
        let salt = setting[3..]
            .split('$')
            .next()
            .unwrap_or_default()
            .chars()
            .take(8)
            .collect::<String>();
        return md5_crypt(password, &salt);
    }
    if setting.starts_with("$5$") || setting.starts_with("$6$") {
        if !password.is_ascii() || !setting.is_ascii() {
            // Exactness for non-ASCII input has not been verified.
            return Err(SqlError::Unsupported(
                "crypt() with sha256crypt/sha512crypt requires ASCII input".to_string(),
            ));
        }
        let result = if setting.starts_with("$5$") {
            pwhash::sha256_crypt::hash_with(setting, password)
        } else {
            pwhash::sha512_crypt::hash_with(setting, password)
        };
        return result.map_err(|_| crypt_failed());
    }
    if setting.starts_with('_') {
        let bytes = setting.as_bytes();
        if bytes.len() < 9 || !bytes[1..9].iter().all(|byte| ITOA64.contains(byte)) {
            return Err(crypt_failed());
        }
        return pwhash::bsdi_crypt::hash_with(&setting[..9], password).map_err(|_| crypt_failed());
    }
    // Traditional DES reads two salt characters, mapping any character the
    // way pgcrypto's ascii_to_bin does, and echoes them in the result.
    let bytes = setting.as_bytes();
    let Some(&first) = bytes.first() else {
        return Err(crypt_failed());
    };
    let second = bytes.get(1).copied().unwrap_or(first);
    let canonical = [
        ITOA64[ascii_to_bin(first)] as char,
        ITOA64[ascii_to_bin(second)] as char,
    ]
    .iter()
    .collect::<String>();
    let hash = pwhash::unix_crypt::hash_with(&canonical, password).map_err(|_| crypt_failed())?;
    let mut out = String::with_capacity(hash.len());
    out.push(first as char);
    out.push(second as char);
    out.push_str(&hash[2..]);
    Ok(out)
}

/// pgcrypto's `ascii_to_bin`: the 6-bit value of a salt character.
fn ascii_to_bin(ch: u8) -> usize {
    (match ch {
        b'a'..=b'z' => ch - b'a' + 38,
        b'A'..=b'Z' => ch - b'A' + 12,
        b'.'..=b'9' => ch - b'.',
        _ => 0,
    }) as usize
}

/// Blowfish: `$2a$NN$` (or `$2x$`) and 22 salt characters.
fn bcrypt(password: &[u8], setting: &str) -> Result<String> {
    let bytes = setting.as_bytes();
    if bytes.len() < 29
        || bytes[6] != b'$'
        || !bytes[4].is_ascii_digit()
        || !bytes[5].is_ascii_digit()
    {
        return Err(crypt_failed());
    }
    let cost = u32::from(bytes[4] - b'0') * 10 + u32::from(bytes[5] - b'0');
    if !(4..=31).contains(&cost) {
        return Err(crypt_failed());
    }
    let salt = &setting[7..29];
    if !salt.bytes().all(|byte| BF_ITOA64.contains(&byte)) {
        return Err(crypt_failed());
    }
    let variant = bytes[2] as char;
    if !password.is_ascii() {
        // `$2x$` reproduces a historical sign-extension bug for bytes with
        // the high bit set and `$2a$` applies a countermeasure for them; these
        // are not implemented, so such passwords are refused, not misjudged.
        return Err(SqlError::Unsupported(format!(
            "crypt() with ${variant}$ blowfish hashes requires an ASCII password"
        )));
    }
    let normalized = format!("$2a${cost:02}${salt}");
    let hash =
        pwhash::bcrypt::hash_with(normalized.as_str(), password).map_err(|_| crypt_failed())?;
    Ok(format!("$2{variant}{}", &hash[3..]))
}

/// md5-crypt (`$1$salt$hash`), the FreeBSD algorithm pgcrypto implements.
fn md5_crypt(password: &[u8], salt: &str) -> Result<String> {
    use md5::{Digest, Md5};
    let salt = salt.as_bytes();
    let mut alternate = Md5::new();
    alternate.update(password);
    alternate.update(salt);
    alternate.update(password);
    let alternate = alternate.finalize();

    let mut context = Md5::new();
    context.update(password);
    context.update(b"$1$");
    context.update(salt);
    let mut remaining = password.len();
    while remaining > 0 {
        let take = remaining.min(16);
        context.update(&alternate[..take]);
        remaining -= take;
    }
    let mut length = password.len();
    while length > 0 {
        if length & 1 == 1 {
            context.update([0u8]);
        } else {
            context.update(&password[..1.min(password.len())]);
        }
        length >>= 1;
    }
    let mut digest = context.finalize();
    for round in 0..1000 {
        let mut step = Md5::new();
        if round & 1 == 1 {
            step.update(password);
        } else {
            step.update(digest);
        }
        if round % 3 != 0 {
            step.update(salt);
        }
        if round % 7 != 0 {
            step.update(password);
        }
        if round & 1 == 1 {
            step.update(digest);
        } else {
            step.update(password);
        }
        digest = step.finalize();
    }
    let mut out = String::with_capacity(34);
    out.push_str("$1$");
    out.push_str(std::str::from_utf8(salt).map_err(|_| crypt_failed())?);
    out.push('$');
    let d = digest;
    for (a, b, c) in [(0, 6, 12), (1, 7, 13), (2, 8, 14), (3, 9, 15), (4, 10, 5)] {
        push_itoa64(
            &mut out,
            (u32::from(d[a]) << 16) | (u32::from(d[b]) << 8) | u32::from(d[c]),
            4,
        );
    }
    push_itoa64(&mut out, u32::from(d[11]), 2);
    Ok(out)
}

fn push_itoa64(out: &mut String, mut value: u32, count: usize) {
    for _ in 0..count {
        out.push(ITOA64[(value & 0x3f) as usize] as char);
        value >>= 6;
    }
}

fn random_bytes<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes)
        .map_err(|error| SqlError::Unsupported(format!("gen_salt: {error}")))?;
    Ok(bytes)
}

/// pgcrypto's `gen_salt`, with its default and permitted round counts.
pub(crate) fn gen_salt(kind: &str, rounds: i64) -> Result<String> {
    let (default_rounds, min_rounds, max_rounds) = match kind.to_ascii_lowercase().as_str() {
        "des" => (0, 0, 0),
        "md5" => (1000, 1000, 1000),
        "xdes" => (29 * 25, 1, 0xff_ffff),
        "bf" => (6, 4, 31),
        "sha256crypt" | "sha512crypt" => (5000, 1000, 999_999_999),
        _ => return Err(gen_salt_error("Unknown salt algorithm")),
    };
    let rounds = if rounds == 0 {
        default_rounds
    } else if rounds < min_rounds || rounds > max_rounds {
        return Err(gen_salt_error("Incorrect number of rounds"));
    } else {
        rounds
    };
    let mut out = String::new();
    match kind.to_ascii_lowercase().as_str() {
        "des" => {
            let input = random_bytes::<2>()?;
            out.push(ITOA64[(input[0] & 0x3f) as usize] as char);
            out.push(ITOA64[(input[1] & 0x3f) as usize] as char);
        }
        "md5" => {
            let input = random_bytes::<6>()?;
            out.push_str("$1$");
            push_itoa64(
                &mut out,
                u32::from(input[0]) | (u32::from(input[1]) << 8) | (u32::from(input[2]) << 16),
                4,
            );
            push_itoa64(
                &mut out,
                u32::from(input[3]) | (u32::from(input[4]) << 8) | (u32::from(input[5]) << 16),
                4,
            );
        }
        "xdes" => {
            if rounds & 1 == 0 {
                return Err(gen_salt_error("Incorrect number of rounds"));
            }
            let input = random_bytes::<3>()?;
            out.push('_');
            push_itoa64(&mut out, rounds as u32, 4);
            push_itoa64(
                &mut out,
                u32::from(input[0]) | (u32::from(input[1]) << 8) | (u32::from(input[2]) << 16),
                4,
            );
        }
        "sha256crypt" | "sha512crypt" => {
            let magic = if kind.eq_ignore_ascii_case("sha256crypt") {
                5
            } else {
                6
            };
            out.push_str(&format!("${magic}$rounds={rounds}$"));
            for byte in random_bytes::<16>()? {
                out.push(ITOA64[(byte & 0x3f) as usize] as char);
            }
        }
        _ => {
            let input = random_bytes::<16>()?;
            out.push_str(&format!("$2a${rounds:02}$"));
            out.push_str(&bf_encode(&input));
        }
    }
    Ok(out)
}

/// bcrypt's base-64 encoding of the 16-byte salt (22 characters).
fn bf_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(22);
    let mut index = 0;
    while index < input.len() {
        let c1 = input[index];
        index += 1;
        out.push(BF_ITOA64[(c1 >> 2) as usize] as char);
        let mut carry = (c1 & 0x03) << 4;
        if index >= input.len() {
            out.push(BF_ITOA64[carry as usize] as char);
            break;
        }
        let c2 = input[index];
        index += 1;
        carry |= c2 >> 4;
        out.push(BF_ITOA64[carry as usize] as char);
        carry = (c2 & 0x0f) << 2;
        if index >= input.len() {
            out.push(BF_ITOA64[carry as usize] as char);
            break;
        }
        let c3 = input[index];
        index += 1;
        carry |= c3 >> 6;
        out.push(BF_ITOA64[carry as usize] as char);
        out.push(BF_ITOA64[(c3 & 0x3f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_hashes_match_the_reference_implementations() {
        // Published vectors for each algorithm.
        assert_eq!(
            crypt("password", "$1$saltsalt$").unwrap(),
            "$1$saltsalt$qjXMvbEw8oaL.CzflDtaK/"
        );
        assert_eq!(crypt("test", "aZ").unwrap(), "aZGJuE6EXrjEE");
        let bf = crypt("U*U", "$2a$05$CCCCCCCCCCCCCCCCCCCCC.").unwrap();
        assert_eq!(
            bf,
            "$2a$05$CCCCCCCCCCCCCCCCCCCCC.E5YPO9kmyuRGyh0XouQYb4YMJKvyOeW"
        );
        // Outputs of PostgreSQL 18's pgcrypto.
        for (password, setting, expected) in [
            ("password", "$5$rounds=5000$saltstring", "$5$rounds=5000$saltstring$OH4IDuTlsuTYPdED1gsuiRMyTAwNlRWyA6Xr3I4/dQ5"),
            ("password", "$5$saltstring", "$5$saltstring$OH4IDuTlsuTYPdED1gsuiRMyTAwNlRWyA6Xr3I4/dQ5"),
            ("password", "$6$rounds=10000$saltstringsaltstring", "$6$rounds=10000$saltstringsaltst$Qzq13AQeOS5aa8nj8fFRVn5cZkLp.t1PUmhPeygZiOwD7eFxWmcxrxGtIgnXDKoTYvTEmyGzyqVjaDGGtAurL/"),
            ("password", "$6$saltstringsaltstringXYZ$", "$6$saltstringsaltst$6JOgtRfhXqEisnc/Nr64lml/zPnCnvtLyMVxFEVg0sI2Ph9URAKlnVjjIHOFI2r8ATszyoPTXlBwcJIQYQ0QN0"),
            ("password", "$5$rounds=10$saltstring", "$5$rounds=1000$saltstring$S2dlplsLzofRuJ/frAjtaYev58CdqEN6fUby2laILG1"),
            ("x", "$9$abc", "$9Pup5rgKK1Dc"),
            ("password", "$1$saltsaltextra$", "$1$saltsalt$qjXMvbEw8oaL.CzflDtaK/"),
            ("U*U", "$2x$05$CCCCCCCCCCCCCCCCCCCCC.", "$2x$05$CCCCCCCCCCCCCCCCCCCCC.E5YPO9kmyuRGyh0XouQYb4YMJKvyOeW"),
            ("U*U", "$2y$05$CCCCCCCCCCCCCCCCCCCCC.", "$2C92it9i1RT."),
            ("", "$2a$05$CCCCCCCCCCCCCCCCCCCCC.", "$2a$05$CCCCCCCCCCCCCCCCCCCCC.7uG0VCzI2bS7j6ymqJi9CdcdxiRTWNy"),
        ] {
            assert_eq!(crypt(password, setting).unwrap(), expected, "{setting}");
        }
        assert_eq!(
            crypt("pässwörd", "$2a$05$CCCCCCCCCCCCCCCCCCCCC.")
                .unwrap_err()
                .sqlstate(),
            "0A000"
        );
    }

    #[test]
    fn generated_salts_round_trip_through_crypt() {
        for (kind, rounds) in [
            ("bf", 4),
            ("md5", 0),
            ("md5", 1000),
            ("xdes", 0),
            ("des", 0),
            ("BF", 0),
            ("sha256crypt", 0),
            ("sha512crypt", 1000),
        ] {
            let salt = gen_salt(kind, rounds).unwrap();
            let hash = crypt("s3cret", &salt).unwrap();
            assert_eq!(crypt("s3cret", &hash).unwrap(), hash, "{kind}");
            assert_ne!(crypt("wrong", &hash).unwrap(), hash, "{kind}");
        }
        assert_eq!(gen_salt("bf", 0).unwrap().len(), 29);
        assert_eq!(gen_salt("md5", 0).unwrap().len(), 11);
        assert_eq!(gen_salt("xdes", 0).unwrap().len(), 9);
        assert!(gen_salt("xdes", 0).unwrap().starts_with("_J9.."));
        assert!(gen_salt("bf", 3).is_err());
        assert!(gen_salt("xdes", 2).is_err());
        assert!(gen_salt("md5", 5).is_err());
        assert!(gen_salt("des", 1).is_err());
        assert!(gen_salt("sha256crypt", 999).is_err());
        assert_eq!(gen_salt("sha256crypt", 0).unwrap().len(), 31);
        assert!(gen_salt("nope", 0).is_err());
        assert_eq!(crypt("x", "$2$05$x").unwrap_err().sqlstate(), "22023");
        // PostgreSQL falls back to traditional DES for unknown prefixes.
        assert_eq!(crypt("x", "$9$abc").unwrap().len(), 13);
    }
}
