// PostgreSQL hash_bytes_extended, adapted to safe Rust (UTF-8 text, little-endian).
// Based on PostgreSQL REL_17_STABLE src/common/hashfn.c; Bob Jenkins' mixing
// algorithm, adapted for PostgreSQL by Neil Conway.
// https://github.com/postgres/postgres/blob/REL_17_STABLE/src/common/hashfn.c
/*
PostgreSQL Database Management System
(also known as Postgres, formerly known as Postgres95)

Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group

Portions Copyright (c) 1994, The Regents of the University of California

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement
is hereby granted, provided that the above copyright notice and this
paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
*/

fn mix(a: &mut u32, b: &mut u32, c: &mut u32) {
    *a = a.wrapping_sub(*c);
    *a ^= c.rotate_left(4);
    *c = c.wrapping_add(*b);
    *b = b.wrapping_sub(*a);
    *b ^= a.rotate_left(6);
    *a = a.wrapping_add(*c);
    *c = c.wrapping_sub(*b);
    *c ^= b.rotate_left(8);
    *b = b.wrapping_add(*a);
    *a = a.wrapping_sub(*c);
    *a ^= c.rotate_left(16);
    *c = c.wrapping_add(*b);
    *b = b.wrapping_sub(*a);
    *b ^= a.rotate_left(19);
    *a = a.wrapping_add(*c);
    *c = c.wrapping_sub(*b);
    *c ^= b.rotate_left(4);
    *b = b.wrapping_add(*a);
}

pub(crate) fn hash_text_extended(text: &str, seed: i64) -> i64 {
    let bytes = text.as_bytes();
    let mut a = 0x9e3779b9u32
        .wrapping_add(bytes.len() as u32)
        .wrapping_add(3923095);
    let mut b = a;
    let mut c = a;
    if seed != 0 {
        a = a.wrapping_add(((seed as u64) >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        mix(&mut a, &mut b, &mut c);
    }
    let mut chunks = bytes.chunks_exact(12);
    for chunk in chunks.by_ref() {
        a = a.wrapping_add(u32::from_le_bytes(chunk[0..4].try_into().unwrap()));
        b = b.wrapping_add(u32::from_le_bytes(chunk[4..8].try_into().unwrap()));
        c = c.wrapping_add(u32::from_le_bytes(chunk[8..12].try_into().unwrap()));
        mix(&mut a, &mut b, &mut c);
    }
    for (i, byte) in chunks.remainder().iter().enumerate() {
        match i {
            0..=3 => a = a.wrapping_add(u32::from(*byte) << (8 * i)),
            4..=7 => b = b.wrapping_add(u32::from(*byte) << (8 * (i - 4))),
            _ => c = c.wrapping_add(u32::from(*byte) << (8 * (i - 7))),
        }
    }
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(14));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(16));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(4));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(14));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(24));
    ((u64::from(b) << 32) | u64::from(c)) as i64
}
