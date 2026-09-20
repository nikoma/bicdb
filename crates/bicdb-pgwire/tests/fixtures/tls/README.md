# TLS channel-binding test fixtures

These certificates and private keys are **public test data**, generated for
localhost by `python3 generate.py`. Never use these keys for a real service.
Regeneration changes the certificate bytes; the tests compute expected digests
from the checked-in DER rather than hardcoding a fingerprint.

The SHA-256 and SHA-384 ECDSA pairs support verified TLS client tests. The
Ed25519 pair exercises the explicit plain-SCRAM policies for a certificate
without a supported binding hash. The remaining certificates exercise
signature-algorithm parsing only; their presence here does not mean rustls or
a PostgreSQL client accepts them for TLS.

`pss384.pem` signs with SHA-384 while MGF1 uses SHA-256, proving the binding uses
the message hash. `pss_default.pem` uses the default SHA-1 PSS parameters and must
produce the RFC 5929 SHA-256 fallback. The MD5 and SHA-1 fixtures exercise the
same fallback. Ed25519 has no supported single-hash mapping for this binding and
is explicitly rejected for SCRAM-PLUS; `prefer` and `disable` can use plain
SCRAM over verified TLS instead.

The libpq interoperability test is ignored by the default Rust test command
because it needs `psql`. Run it explicitly with:

```sh
cargo test -p bicdb-pgwire --test tls_channel_binding libpq_requires -- --ignored
```
