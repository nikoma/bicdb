# Frozen protocol-19 codec and protocol-20 refusal

The exact optimized 1.0.463-beta CLI, source `d49a391b3c6157f90f29e9f2f5214143c60f9f4b`, SHA-256 `840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d`, was executed against fresh owned loopback roots. No engine was built and no retained failed-experiment database was opened or changed. Two sequential local runs preserve the original execution and the reproducible helper with optional artifact path/hash arguments; each used one server process at a time.

The old balanced controller established an actual metadata leader and a committed index. Protocol-19 `Ping` returned `Pong` before and after the paired requests. This is a protocol/metadata-runtime control: server logs also show the range write schema fence was not ready. No application write-health result is established.

A synthetic self-addressed metadata snapshot request was derived from that server's actual current term, base index, base term and committed topology. The request added the non-nil `ClusterTopology.range_image_bootstrap_id` field from unmerged source `2603545fe60d42deb2dc928e49b66b852c957b70`. This is an adversarial compatibility input, not an authorized native-genesis operation or ordinary multi-node Raft scenario.

With header **20**, the old server returned `unsupported cluster data protocol 20; expected 19`, with no metadata Append acknowledgment. In the initial run, captured metadata-state bytes were identical immediately before and after this request. In the reusable-helper run, metadata advanced during the observation window; the paired protocol-19 Append then returned `success=false` with a conflict. That optional codec-omission check is explicitly `NOT_TESTED_PRECONDITION_OR_CONCURRENT_CHANGE`. Automatic controller activity is visible in raw logs and is not attributed to the refused request.

In the initial run, with header **19**, the identical Append body received actual `MetadataAppend.success = true`. The persisted committed topology omitted the new domain UUID. The paired raw frames, decoded projections and before/after state establish this specific old-codec omission. They do not dynamically demonstrate loss of `handoff_session`; that field remains a separate source-level compatibility concern.

Both processes stopped on the helper's task-owned process group with `SIGTERM`, exited zero and were reaped. Their PIDs were absent and both owned ports returned `ECONNREFUSED` afterward. Each helper bounds initialization, startup, socket and shutdown waits and installs a 90-second alarm while its server is active. Evidence fsync remains ordinary non-preemptible OS I/O. The local fresh roots and full original manifests remain retained privately; this export includes the exact control-state and wire evidence, not whole data files. `original-private-run-manifest.json` records private-run provenance and is not a claim that every original DB file is exported here. The top-level `manifest.json` covers every public export file.

This demonstrates the need to label newer traffic with protocol 20, where the old runtime refuses it. It does **not** test a new protocol-20 server, rolling upgrades, TLS identity, native image installation, multi-node fault safety, physical failure behavior or production qualification. Checksums provide reproducibility/integrity within trusted evidence custody; they are not remote execution attestations or voter signatures. PR #144's independent restore-lineage repair is separate from this protocol-gated image batch.

Reproduce only with the exact retained binary (it is not built or downloaded by this script):

```sh
python3 scripts/experiments/protocol19-frozen-compat.py \
  --binary /path/to/frozen/bicdb-1.0.463-beta \
  --expected-sha256 840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d
```

Verify the retained export without starting a process or touching a database:

```sh
python3 scripts/verify-protocol19-frozen-compat.py \
  docs/evidence/protocol19-frozen-compat-2026-11-11
python3 -m unittest discover -s scripts/tests -p test_protocol19_frozen_compat.py
```

Seven offline verifier boundary tests passed. The export also retains one artifact-assembly check that correctly rejected a stale manifest after a README edit; the manifest was rebuilt before the final recorded tests. This assembly error is separate from engine/runtime outcomes. Raw stdout/stderr are deterministic gzip streams; decompression reproduces the original bytes.
