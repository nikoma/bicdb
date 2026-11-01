/* Linux qualification helper. Never linked into BicDB or shipped at runtime.
 * Inject real libc I/O errors/stalls in the exact production executable.
 * The controller changes an external file atomically; every hit is journaled.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

static _Atomic int manifest_linked;

static int control(char *out) {
    const char *path = getenv("BICDB_QUALIFY_CONTROL");
    if (!path) return 0;
    int fd = syscall(SYS_openat, AT_FDCWD, path, O_RDONLY, 0);
    if (fd < 0) return 0;
    int n = syscall(SYS_read, fd, out, 79);
    syscall(SYS_close, fd);
    if (n <= 0) return 0;
    out[n] = 0;
    out[strcspn(out, "\r\n")] = 0;
    return 1;
}

static void hit(const char *kind, const char *path) {
    const char *trace = getenv("BICDB_QUALIFY_TRACE");
    if (!trace) return;
    int fd = syscall(SYS_openat, AT_FDCWD, trace, O_WRONLY|O_APPEND|O_CREAT, 0600);
    if (fd < 0) return;
    char line[4608];
    int n = snprintf(line, sizeof(line), "%s %s\n", kind, path);
    if (n > 0 && n < (int)sizeof(line)) syscall(SYS_write, fd, line, n);
    syscall(SYS_fsync, fd);
    syscall(SYS_close, fd);
}

static int within(const char *path, const char *environment) {
    const char *prefix = getenv(environment);
    if (!prefix) return 0;
    size_t n = strlen(prefix);
    return !strncmp(path, prefix, n) && (!path[n] || path[n] == '/');
}

static void stall(const char *kind, const char *path) {
    hit(kind, path);
    /* Controller must SIGKILL the process, not unwind a simulated exception. */
    for (;;) syscall(SYS_pause);
}

static int fd_fault(int fd, int syncing) {
    char kind[80], name[64], path[4096];
    if (!control(kind)) return 0;
    snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
    int n = syscall(SYS_readlinkat, AT_FDCWD, name, path, sizeof(path)-1);
    if (n < 0) return 0;
    path[n] = 0;
    int repository = within(path, "BICDB_QUALIFY_REPOSITORY");
    int source = within(path, "BICDB_QUALIFY_SOURCE");
    int is_object = repository && strstr(path, "/objects/.tmp-");
    if (!syncing && is_object && !strcmp(kind, "stall-object")) stall(kind, path);
    struct stat st;
    int directory = !fstat(fd, &st) && S_ISDIR(st.st_mode);
    int fail = (!syncing && is_object && !strcmp(kind, "enospc-object"))
        || (syncing && repository && !directory && !strcmp(kind, "eio-file-sync"))
        || (syncing && repository && directory && !strcmp(kind, "eio-dir-sync"))
        || (syncing && repository && directory && !strcmp(kind, "eio-published-dir-sync")
            && atomic_load_explicit(&manifest_linked, memory_order_relaxed))
        || (syncing && source && (strstr(path, "/transactions.log")
            || strstr(path, "/paged/store.wal")) && !strcmp(kind, "eio-source-wal"));
    if (!fail) return 0;
    hit(kind, path);
    errno = !strcmp(kind, "enospc-object") ? ENOSPC : EIO;
    return 1;
}

ssize_t write(int fd, const void *data, size_t n) {
    if (fd_fault(fd, 0)) return -1;
    return syscall(SYS_write, fd, data, n);
}
ssize_t pwrite(int fd, const void *data, size_t n, off_t offset) {
    if (fd_fault(fd, 0)) return -1;
    return syscall(SYS_pwrite64, fd, data, n, offset);
}
ssize_t pwrite64(int fd, const void *data, size_t n, off64_t offset) {
    return pwrite(fd, data, n, offset);
}
int fsync(int fd) {
    if (fd_fault(fd, 1)) return -1;
    return syscall(SYS_fsync, fd);
}
int fdatasync(int fd) {
    if (fd_fault(fd, 1)) return -1;
    return syscall(SYS_fdatasync, fd);
}
int link(const char *old, const char *new) {
    char kind[80];
    int manifest = within(new, "BICDB_QUALIFY_REPOSITORY") && strstr(new, ".bicbackup");
    if (manifest && control(kind)) {
        if (!strcmp(kind, "stall-manifest")) stall(kind, new);
        if (!strcmp(kind, "eio-manifest")) {
            hit(kind, new); errno = EIO; return -1;
        }
    } else kind[0] = 0;
    int result = syscall(SYS_linkat, AT_FDCWD, old, AT_FDCWD, new, 0);
    if (!result && manifest && !strcmp(kind, "eio-published-dir-sync"))
        atomic_store_explicit(&manifest_linked, 1, memory_order_relaxed);
    if (!result && manifest && !strcmp(kind, "stall-published")) stall(kind, new);
    return result;
}
int rename(const char *old, const char *new) {
    int result = syscall(SYS_renameat, AT_FDCWD, old, AT_FDCWD, new);
    char kind[80];
    if (!result && within(new, "BICDB_QUALIFY_RESTORE") && control(kind)
        && !strcmp(kind, "stall-install") && strstr(old, "/.backup-restore-"))
        stall(kind, new);
    return result;
}
int linkat(int oldfd, const char *old, int newfd, const char *new, int flags) {
    if (old[0] == '/' && new[0] == '/' && !flags) return link(old, new);
    return syscall(SYS_linkat, oldfd, old, newfd, new, flags);
}
int renameat(int oldfd, const char *old, int newfd, const char *new) {
    if (old[0] == '/' && new[0] == '/') return rename(old, new);
    return syscall(SYS_renameat, oldfd, old, newfd, new);
}
