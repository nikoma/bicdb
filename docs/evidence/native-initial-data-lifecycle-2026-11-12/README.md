# Initial DATA lifecycle: four exact component controls

Classification: **passed focused lifecycle controls**, not public operator,
physical or production qualification. Source
`8e9642433d064d87c79fdf396194bdbf9289fadc`,
[run 43244356602](https://github.com/nikoma/bicdb/actions/runs/43244356602),
Core job 131272939022: **four passed, zero failed, zero ignored**, 902 filtered,
7.37 seconds. The same-source all-target and isolated-consumer check is separate.

The controls execute actual initial factories, native/metadata quorums, TLS
listeners and current writer enrollment. They reject changed epoch/foreign
birth, reject reusing a genesis proof after an actual same-root/same-birth reopen,
exercise partial/canceled startup and real event ACK/Strong reads, and check
that no-stop, canceled-stop and poisoned-health cleanup releases the actual
outer and inner DB owners. Healthy cleanup cases strictly reopen the same roots
without reviving a serving hook. These are same-process fixture controls, not
VM reset or power-loss tests.

The subsequent independent review found that Drop still acquired a short
in-memory dispatcher lock. Successor `fe811c036` changes that to try-clear and
deferred cleanup. Test-only successor `5db91d9de` strengthens the original row
presence assertions to exact full records plus native R/H and explicit pre-cut
replica convergence. **This earlier four-test pass does not validate either
successor.** New final-source runtime checks are required and remain pending.
The separate public four-command-process CLI acceptance is not established by
these four Core tests. Automatic fifth-node placement/image/joint relocation,
controller restart, nonempty cold recovery and capacity remain open.

`original-artifact.zip` is unchanged artifact13955069340, 40,317 bytes,
SHA-256 `218f175663715aad830ed0609503e4232de446d93649b07c05c00acd69a30e32`.
It contains source revision, compiler identity, selected test list and test log;
no database/key directory. Exact names, outcome and raw log digest are retained
in `summary.json`. Checksums establish custody, not independent execution.
