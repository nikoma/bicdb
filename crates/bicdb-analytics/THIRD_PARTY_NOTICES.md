# BicDB third-party notices

BicDB itself is licensed under the terms in [LICENSE](LICENSE). Third-party
components remain under their respective licenses.

The Rust dependency graph is recorded exactly in `Cargo.lock`. It is
predominantly licensed under permissive terms including MIT, Apache-2.0,
BSD-2-Clause, BSD-3-Clause, ISC, Zlib, Unicode-3.0, BSL-1.0, CC0-1.0, 0BSD,
Unlicense, and CDLA-Permissive-2.0.

Notable components and license choices include:

| Component | License used |
| --- | --- |
| `smartstring` through the default-enabled `osm-import` feature | MPL-2.0 |
| `osmpbfreader`, `par-map`, `pub-iterator-type` | WTFPL |
| `ittapi`, `ittapi-sys` | BSD-3-Clause option |
| `self_cell` | Apache-2.0 option |
| `r-efi` | MIT or Apache-2.0 option |
| `hpke` RFC 9180 implementation | MIT or Apache-2.0 option |
| `rust-stemmers`, `waken_snowball` | MIT/BSD-3-Clause |
| `arrow-array` | Apache-2.0 and MIT |
| `@bjorn3/browser_wasi_shim` vendored under `web/bicdb-client/vendor/` | MIT (the included `LICENSE-MIT`) |

EmbeddingGemma-300m weights are not included in this repository. If obtained
or distributed separately, they remain subject to Google's Gemma terms.
OpenStreetMap datasets are not included; any separately obtained data remains
subject to its applicable terms, including ODbL where relevant.

The `bicdb-core` default feature set currently includes `osm-import`. Therefore
the MPL-2.0 and WTFPL dependencies above are part of the default Rust dependency
graph even when no OpenStreetMap dataset is supplied. Distributors that disable
default features may omit that code path, but must evaluate the exact features
and targets they ship.

Binary distributors must reproduce all notices required by the exact
third-party components they ship. This summary is not a replacement for the
license files supplied with those components.

## PostgreSQL text hashing

`crates/bicdb-sql/src/pg_hash.rs` adapts PostgreSQL's
`src/common/hashfn.c` extended hash algorithm to Rust. That file retains the
PostgreSQL License and attribution inline; its separately licensed code is not
relicensed under the BicDB product restrictions. Binary distributors must also
retain that notice. The source is PostgreSQL's `REL_17_STABLE` branch.

```text
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
```
