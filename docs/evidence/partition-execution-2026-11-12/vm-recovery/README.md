# Partition upgrade under actual guest memory loss

All **56 KVM reset cases passed** at example source
`0adf78abbc53f697e76f889ef52f036e7a1153d6`, whose Core, Page, SQL and protocol
production trees and manifests exactly match runtime
`5aa92dcecb29b639251acc81a5d60428dfae96f7`.
The independent verifier also passed all 56 cases. This private evidence is not
a merge, release, production deployment, physical host power cut, RF3 HA or
transparent distributed capacity qualification.

Both storage modes exercised parent heap migration, invalid-child relocation,
and both reviewed independent-policy decisions. Each of those eight controls
was cut at seven boundaries: before fence write, sync, rename and directory
sync; before Native transaction-log write and sync; and after an independently
observed upgrade ACK. These are the existing **pre-I/O** hooks, not claims of a
pause after each completed sync. The external controller waited for the exact
serial witness, persisted its own witness with file and directory fsync, then
sent SIGKILL only to its owned QEMU child. No volatile-file emulation was enabled.

The preserved probe and guest binary have identical SHA256:
`47159ec9ea97e9f9b3d6180a2ef78244b0e1fc8a908ac4b234d7fc9d8350d539`.
The guest used a private initramfs and ext4 image, existing cached QEMU/kernel,
KVM, one vCPU, 2 GiB RAM and no network. No new cloud resources were purchased.
The shared VM qualification lock excluded other campaigns; shared VM artifacts
and earlier images were preserved. Host memory and cgroup pressure were monitored.

Each preserved crashed raw image was copied for recovery. A fresh guest opened
the database three times, requiring exact complete-old or complete-new Native
records and raw SQL schemas, valid indexes and correct certificate/fence state.
All three observations agreed. Forty cases recovered old state; sixteen new.
All eight cases with an externally acknowledged upgrade recovered complete new
state. All fourteen original old482 ACKs were independently linked to the old
snapshot and their original database file manifests remained unchanged.
The saved crash-image hashes remained unchanged after recovery verification.

The first two-case smoke attempt failed in the host reader: a partial serial
JSON line was parsed before its newline. That failure and the original reader
are retained. The reader now waits for complete lines; the separate second smoke
passed both cases before the 56-case matrix ran. No product assertion or timeout
was relaxed.

`original-source-scoped-results.tar.gz` contains the independent verification,
case reports, serial transcripts, external witnesses, controllers and cleanup
receipt. `archived-files.json` records every member's size and SHA256. Raw VM
images, binaries and scratch database directories are excluded from Git; the
case reports retain image paths and hashes under
`/root/bicdb-durability-s3/partition-vm-0adf78ab/` on the build host.

After the preceding full Core/SQL/protocol run passed, 415 superseded, task-owned
cc84 test executables were removed under the source lock. Exact path, inode,
size, mtime, link count and ELF checks, plus all-process FD/maps checks, protected
current binaries and other work. This reclaimed 120,502,267,904 allocated bytes
(112.23 GiB). The measured remaining targets were 483,643,046,109 and
76,498,217,234 bytes; later builds can change those sizes.
