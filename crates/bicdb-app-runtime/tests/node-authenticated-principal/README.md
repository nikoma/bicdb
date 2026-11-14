# Native authenticated-principal SDK qualification

Run `node qualify.mjs CONFIG.json NEW_PRIVATE_OUTPUT` with explicit `native` and
`oldNative` objects containing executable `path` and `sha256`. The existing
source-authored SDK fixture and strict ABI, subject, effective grant, issuer,
audience and legacy-accessor assertions remain unchanged.

After actual SIGKILL/restart tests and a genuinely awaited source exit, cold
recovery uses the shared [custody helper](../node-cold-copy-adoption/README.md).
Official inactive `NotRequired` is accepted with exact source/target byte proofs;
active stores require the actual refusal/adoption/idempotency controls. The
original source is retained closed at its original path and checked unchanged
after copied-runtime identity replay. No real issuer or Auth account is changed.
