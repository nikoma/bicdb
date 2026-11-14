# Native hot publication: failed first compile check

Exact unmerged source `d5fead60a42348edeaaab59bf28e603d685a9303` retains
source version **1.0.500-beta**. [Run 43521850216](https://github.com/nikoma/bicdb/actions/runs/43521850216)
failed both workspace all-target compilation and the Core chaos-helper build.
Each compiler reports the same six Rust errors: a missing Arc import, an
Arc-versus-inner materialization trait view, Weak-versus-Arc original ownership,
and three private-field accesses. These are code errors, not runner loss.

The six new local metadata-version tests were **not selected or executed**:
Core failed before its listing step. The independent consumer compile check was
also not reached. No full hot handoff, APP/Strong read, controller, cold restart,
physical experiment or production qualification follows.

`55c50e07c0f36ee7314342b4c8653988f444a835` is a separate small source correction
using the actual retained owner and existing sealed getters, plus an immutable
historical-fact accessor. This packet does not establish that correction's
compilation or runtime result and never relabels the original failed run.

The complete hashed logs, job/step metadata, exact commands and classifications
are retained. Reproduce with the pinned source and commands in
`source-and-classification.json`; verify this captured evidence offline with:

```sh
python3 -B /path/to/this/packet/verify.py
```

The verifier checks the original failed job/step states and six actual compiler
diagnostics per log. Its success means evidence consistency, not passing code.
The original failed Azure qualification and production gate remain unchanged.
