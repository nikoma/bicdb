"""Linux descriptor-bound collector storage preflight, not durability proof.

Retain the returned real directory FD for openat/file+directory fsync. Path-only
mountpoint sorting cannot disambiguate overmounts. Unknown or inconsistent
kernel observations refuse rather than guessing a filesystem.
"""
import os
from pathlib import Path, PurePosixPath
import re
import stat

import verify as V

LOCAL_FILESYSTEMS = {"ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "f2fs"}


def mount_id(fd):
    raw = V.bounded(f"/proc/self/fdinfo/{fd}", 65536).decode("ascii")
    found = [line.split(":", 1)[1].strip() for line in raw.splitlines() if line.startswith("mnt_id:")]
    V.require(len(found) == 1 and re.fullmatch(r"[1-9][0-9]*", found[0]),
              "actual opened directory lacks one kernel mount ID; unsupported")
    return int(found[0])


def mountinfo():
    result = []
    for line in V.bounded("/proc/self/mountinfo", 1024**2).decode().splitlines():
        before, after = line.split(" - ", 1)
        fields, tail = before.split(), after.split()
        V.require(len(fields) >= 6 and len(tail) >= 3 and fields[0].isdigit(), "malformed kernel mountinfo")
        major, minor = fields[2].split(":")
        V.require(major.isdigit() and minor.isdigit(), "malformed mount device")
        def path(raw):
            decoded = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), raw)
            V.require("\0" not in decoded, "invalid kernel mount path bytes")
            # Unrelated nsfs mounts legitimately have roots such as net:[...].
            # Preserve kernel DATA here; validate absolute paths only for the
            # actual opened directory's uniquely selected mount below.
            return decoded
        result.append({"mount_id": int(fields[0]), "major": int(major), "minor": int(minor),
                       "root": path(fields[3]), "mountpoint": path(fields[4]), "filesystem": tail[0]})
    return result


def identity(fd):
    s = os.fstat(fd)
    V.require(stat.S_ISDIR(s.st_mode), "actual storage handle must be a directory")
    return (s.st_dev, s.st_ino, mount_id(fd))


def inspect_directory(fd):
    original = identity(fd)
    device, inode, actual_mount = original
    actual = os.readlink(f"/proc/self/fd/{fd}")
    V.require(actual.startswith("/") and not actual.endswith(" (deleted)"), "unlinked/nonabsolute storage handle")
    path = PurePosixPath(actual)
    matches = [m for m in mountinfo() if m["mount_id"] == actual_mount]
    V.require(len(matches) == 1, "ambiguous or missing actual kernel mount ID")
    selected = matches[0]
    V.require((selected["major"], selected["minor"]) == (os.major(device), os.minor(device)),
              "actual directory st_dev differs from kernel mount device")
    root, point = PurePosixPath(selected["root"]), PurePosixPath(selected["mountpoint"])
    V.require(root.is_absolute() and ".." not in root.parts, "invalid selected filesystem root")
    V.require(point.is_absolute() and ".." not in point.parts, "invalid selected mountpoint")
    V.require(path == point or point in path.parents, "opened directory is outside selected kernel mountpoint")
    filesystem_path = root / path.relative_to(point)
    V.require(filesystem_path == root or root in filesystem_path.parents, "opened path escapes selected filesystem root")
    # Bind/root attribution comes from the same actual mnt_id, and the current
    # namespace mountpoint must still open on that exact device+mount. Do not
    # mistake another old/equal-path overmount entry for the opened filesystem.
    flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
    point_fd = os.open(str(point), flags)
    try:
        pdev, _, pmount = identity(point_fd)
        V.require(pdev == device and pmount == actual_mount, "mountpoint changed after opening actual storage")
    finally:
        os.close(point_fd)
    V.require(selected["filesystem"] in LOCAL_FILESYSTEMS,
              "collector requires supported local durable filesystem; no NFS/SSHFS/tmpfs assumption")
    V.require(identity(fd) == original, "actual opened storage identity changed during preflight")
    return {"canonical_directory": str(path), "mountpoint": str(point),
            "mount_filesystem_root": str(root), "filesystem_relative_path": str(filesystem_path),
            "filesystem": selected["filesystem"], "st_dev": device, "st_ino": inode,
            "kernel_mount_id": actual_mount, "actual_device_major": os.major(device),
            "actual_device_minor": os.minor(device),
            "mount_preflight": "opened_directory_device_and_kernel_mount_id",
            "actual_backing_storage_and_flush_behavior_qualified": False}


def open_local_storage(path):
    """Return retained real directory FD plus DATA; never a database grant."""
    flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
    path_fd = os.open(path, flags)
    directory_fd = None
    try:
        contract = inspect_directory(path_fd)
        # Open relative to the actual held directory; no second path lookup.
        directory_fd = os.open(".", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=path_fd)
        V.require(identity(directory_fd) == identity(path_fd), "retained fsync directory differs from actual checked handle")
        return directory_fd, contract
    except BaseException:
        if directory_fd is not None:
            os.close(directory_fd)
        raise
    finally:
        os.close(path_fd)


def local_storage_contract(path):
    fd, contract = open_local_storage(path)
    try:
        return contract
    finally:
        os.close(fd)
