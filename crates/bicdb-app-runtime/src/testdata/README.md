`test-only-rs256.pk8.der` is a synthetic, publicly committed 2048-bit RSA key
generated solely for JWT verification tests. It is not a production credential.
The tests sign synthetic tokens using ring and verify their JWKS representation.

Recreate it with OpenSSL:

```sh
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -outform DER -out /tmp/bicdb-test-rsa.der
openssl pkcs8 -topk8 -nocrypt -inform DER -in /tmp/bicdb-test-rsa.der -outform DER -out test-only-rs256.pk8.der
```
