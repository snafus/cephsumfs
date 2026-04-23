# cephsumfs

External checksum helper for XRootD on Ceph-backed POSIX storage.

Computes and caches file checksums via Linux extended attributes using the
XRootD `XrdCks` wire format.  XRootD delegates checksum requests to this
tool via the `xrd.chksum` directive rather than computing them inline.

## Features

- Multithreaded file reading (`os.pread`) tuned for CephFS RADOS stripe layout
- Checksum caching via Linux xattrs in XRootD `XrdCks` wire format
- Automatic cache invalidation: cached values are validated against file mtime
  before use — modified files are always recomputed
- Supports adler32, crc32, crc32c, md5, sha256; new algorithms can be registered
  at runtime without modifying the package
- Zero external Python dependencies (stdlib only, Python ≥ 3.6.8)
- Packaged for RHEL 8, RHEL 9 (RPM) and Ubuntu 22.04, 24.04 (DEB)

## Components

| Module | Role |
|--------|------|
| `cephsumfs/algorithms.py` | Algorithm ABC and registry (adler32, crc32, crc32c, md5, sha256) |
| `cephsumfs/reader.py` | Algorithm-agnostic multithreaded `os.pread()` pipeline |
| `cephsumfs/xattr.py` | XrdCks 96-byte blob serialisation and mtime validation |
| `cephsumfs/cli.py` | CLI entry point; strict stdout / errno exit-code contract |
| `scripts/run_checksum.sh` | Thin shell wrapper invoked by XRootD |

## Requirements

- Linux with xattr support (`user.*` namespace enabled on the filesystem)
- Python 3.6.8 or later
- No third-party Python packages
- `libcrc32c` recommended for hardware-accelerated CRC-32C (see [Performance](#performance))

## Installation

```bash
# RHEL 8 / RHEL 9 / AlmaLinux (RPM)
dnf install cephsumfs

# Ubuntu 22.04 / 24.04 (DEB)
apt install ./cephsumfs_*.deb

# From source
pip install .
```

Pre-built packages for each release are attached to the
[GitHub Releases](https://github.com/snafus/cephsumfs/releases) page.

## XRootD integration

Add one line to the XRootD configuration file:

```
# /etc/xrootd/xrootd.cfg
xrd.chksum adler32 /usr/libexec/cephsumfs/run_checksum.sh
```

The `xrootd` user must be able to execute the wrapper script and read the
files being checksummed.  No other XRootD configuration is required.

### How XRootD uses this tool

XRootD spawns `run_checksum.sh <lfn>` for each checksum request.  The script
prepends `CEPHSUM_PREFIX` to form the full filesystem path and invokes
`cephsumfs`.  On success, `cephsumfs` writes exactly `<hex-digest>\n` to
stdout and exits 0.  On error it writes nothing to stdout and exits with an
errno-derived code.

## CLI usage

```bash
# Return cached checksum, or compute and cache it (normal XRootD path)
cephsumfs /path/to/file

# Force recompute and overwrite cached value
cephsumfs --override /path/to/file

# Compute only — do not read or write xattr
cephsumfs --compute-only /path/to/file

# Compute if needed but never write xattr (useful for read-only mounts)
cephsumfs --dry-run /path/to/file

# Verify cached checksum against file data
cephsumfs --verify /path/to/file

# Remove cached checksum xattr
cephsumfs --remove /path/to/file

# Use a different algorithm
cephsumfs --algo sha256 /path/to/file

# Tune I/O parameters
cephsumfs --block-mib 4 --threads 4 --inflight 16 /path/to/file

# Enable diagnostic logging
cephsumfs --log-file /var/log/cephsumfs.log /path/to/file
cephsumfs --log-stderr /path/to/file
```

### Exit codes

| Code | Errno | Meaning |
|------|-------|---------|
| 0 | — | Success; checksum on stdout |
| 2 | `ENOENT` | File not found |
| 5 | `EIO` | I/O error or file modified during read |
| 13 | `EACCES` | Permission denied |
| 22 | `EINVAL` | Malformed xattr blob |
| 61 | `ENODATA` | xattr absent (e.g. `--verify` with no cached value) |

## Caching behaviour

The default mode (`no flags`) follows this logic on every invocation:

```
xattr present?
  ├─ yes → mtime matches stored fmTime?
  │         ├─ yes  → return cached digest immediately (no file I/O)
  │         └─ no   → recompute, overwrite xattr, return new digest
  ├─ no   → compute, write xattr, return digest
  └─ malformed → exit EINVAL (surface to operator for repair)
```

The mtime comparison uses integer seconds (`st_mtime_ns ÷ 10⁹`) to match the
`uint64` seconds field in the XrdCks wire format.  A file written and
checksummed within the same wall-clock second, then modified again in that
same second, would return a stale cached value until the clock ticks over.
This is a known limitation of the XrdCks format.

## run_checksum.sh configuration

All site-specific values are controlled by environment variables; no file
editing is required after deployment.

| Variable | Default | Description |
|----------|---------|-------------|
| `CEPHSUM_PREFIX` | `/mnt` | Filesystem prefix prepended to the XRootD LFN |
| `CEPHSUM_ALGO` | `adler32` | Checksum algorithm |
| `CEPHSUM_BLOCK_MIB` | `4` | Read block size in MiB (align with CephFS `stripe_unit`) |
| `CEPHSUM_THREADS` | `4` | Concurrent read threads (match CephFS `stripe_count`) |
| `CEPHSUM_INFLIGHT` | *(unset)* | Max outstanding read requests; defaults to `2 × threads` |
| `CEPHSUM_LOG_FILE` | *(unset)* | Append diagnostics to this path |
| `CEPHSUM_PYTHON` | `python3` | Python interpreter to use |

## xattr format

Checksums are stored under `user.XrdCks.<algo>` as a 96-byte blob serialised in
**network byte order (big-endian)**:

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 16 B | name | `char[]` | Algorithm name, NUL-padded, ASCII |
| 16 | 8 B | fmTime | `uint64` **BE** | File mtime at compute time (seconds since epoch) |
| 24 | 4 B | csTime | `uint32` **BE** | Elapsed seconds during computation |
| 28 | 2 B | — | `uint16` **BE** | Reserved |
| 30 | 1 B | — | `uint8` | Reserved |
| 31 | 1 B | length | `uint8` | Meaningful bytes in value[] |
| 32 | 64 B | value | `char[]` | Raw digest bytes, zero-padded |

### Endianness

The `name` and `value` fields are byte arrays and are endianness-neutral.  The
numeric header fields (`fmTime`, `csTime`, the reserved words, and `length`) are
all big-endian, matching XRootD's `XrdCks` native wire format.  Writing these
fields in little-endian would produce silently incorrect mtime values, causing
every cache lookup to be treated as stale.

The digest bytes stored in `value` are also big-endian for the built-in 32-bit
algorithms:

| Algorithm | Digest bytes |
|-----------|-------------|
| adler32 | `struct.pack(">I", value)` — 4 bytes, big-endian |
| crc32 | `struct.pack(">I", value)` — 4 bytes, big-endian |
| crc32c | `struct.pack(">I", value)` — 4 bytes, big-endian |
| md5 | `hashlib` byte string — 16 bytes, endianness-neutral |
| sha256 | `hashlib` byte string — 32 bytes, endianness-neutral |

**For custom algorithm implementors:** `ChecksumAlgorithm.digest()` must return
bytes in the order XRootD expects.  For integer-valued checksums this means
big-endian (`struct.pack(">I", ...)` for 32-bit, `struct.pack(">Q", ...)` for
64-bit).  Using little-endian here would produce a checksum that verifies
correctly within cephsumfs but disagrees with any other XRootD client that
reads the xattr directly.

## Adding custom algorithms

Subclass `ChecksumAlgorithm` and register it before calling any CLI or reader
function:

```python
import struct
from cephsumfs.algorithms import ChecksumAlgorithm, register_algorithm

class MyAlgo(ChecksumAlgorithm):
    name = "myalgo"

    def __init__(self):
        self._value = 0

    def update(self, data: bytes) -> None:
        # accumulate over data
        ...

    def digest(self) -> bytes:
        return struct.pack(">I", self._value)  # big-endian for XRootD

register_algorithm(MyAlgo)
```

The algorithm is then available via `--algo myalgo` and via `get_algorithm("myalgo")`.

## Performance

### Architecture

The reader opens each file once with `O_RDONLY | O_NOATIME` (falling back to
`O_RDONLY` silently if the process does not own the file) and submits concurrent
`os.pread()` calls on a thread pool.  `pread` is POSIX thread-safe: it takes an
explicit offset and does not move the file-descriptor position.  The main thread
consumes completed blocks strictly in file order and feeds each block to the
checksum algorithm.  An inflight cap bounds peak memory usage to
`inflight × block_size` regardless of file size.

Checksum computation is necessarily sequential (adler32 and CRC variants require
bytes in order), so threads do not accelerate the CPU path — they hide per-read
latency on high-latency networked storage.

If a `pread` returns fewer bytes than expected (file truncated or modified during
the read), cephsumfs logs a `WARNING` and aborts with `EIO` rather than silently
computing a checksum over partial data.

### CephFS tuning

CephFS files are striped across RADOS objects.  The three layout parameters that
directly affect optimal cephsumfs settings are:

```bash
# Query a file's layout
getfattr -n ceph.file.layout /mnt/ceph/path/to/file
# stripe_unit=4194304 stripe_count=1 object_size=4194304 pool=cephfs_data
```

| Layout parameter | Maps to cephsumfs setting | Guidance |
|---|---|---|
| `stripe_unit` | `CEPHSUM_BLOCK_MIB` | Set block size equal to stripe_unit (typically 4 MiB). Misaligned blocks force the kernel client to split or merge RADOS object reads. |
| `stripe_count` | `CEPHSUM_THREADS` | Match thread count to stripe_count so concurrent reads hit independent OSDs simultaneously. |
| Network latency | `CEPHSUM_INFLIGHT` | Each `pread` incurs an OSD round-trip (~1–5 ms). Raise inflight to pipeline RPCs: `4 × threads` is a good starting point for 10/25 GbE. |

**Example — default CephFS pool (stripe_count=1, stripe_unit=4 MiB)**

```sh
CEPHSUM_BLOCK_MIB=4
CEPHSUM_THREADS=4
CEPHSUM_INFLIGHT=16   # 4 × threads; pipelines reads to hide OSD latency
```

**Example — striped pool (stripe_count=4, stripe_unit=4 MiB)**

```sh
CEPHSUM_BLOCK_MIB=4
CEPHSUM_THREADS=4     # concurrent reads reach 4 independent OSDs
CEPHSUM_INFLIGHT=16
```

If `CEPHSUM_INFLIGHT` is not set, the code defaults to `2 × threads`, which
is conservative and suitable for low-latency local storage but under-pipelines
on networked Ceph.

### Block size and CPU cache

Blocks larger than the CPU's L3 cache (~8–16 MiB on typical server CPUs)
cause cache pressure during the checksum computation pass and reduce throughput
by ~20–25 % on CPU-bound workloads.  The 4 MiB default aligns with the CephFS
stripe_unit and fits comfortably in most L3 caches.

Use `scripts/profile_performance.py --dir /mnt/ceph` to measure the actual
throughput of your storage backend and confirm the optimal settings.

### adler32 throughput and zlib-ng

`zlib.adler32` is a C extension and releases the GIL.  Its throughput depends
on the system zlib library:

| Library | Typical adler32 throughput | Notes |
|---------|---------------------------|-------|
| zlib 1.2.x | 2–3 GB/s | Default on RHEL 8 |
| zlib-ng 2.x | 10–15 GB/s | SIMD (SSE4.2 / AVX2 / AVX-512) |

To check which library Python is using:

```bash
python3 -c "import zlib; print(zlib.ZLIB_VERSION)"
# zlib-ng identifies itself as e.g. "1.3.0.zlib-ng"
```

For most Ceph deployments, network I/O (1–5 GB/s per client) is the bottleneck
and the 2–3 GB/s adler32 throughput is not a constraint.  If you are operating
at higher bandwidth (fast NVMe-backed Ceph, or multiple concurrent checksums on
a high-core node), consider replacing the system zlib with zlib-ng:

**RHEL 9 / AlmaLinux 9** — zlib-ng may already be the system zlib.  Check the
version string above.  If not, install from EPEL:

```bash
dnf install zlib-ng-compat    # drop-in replacement for /usr/lib64/libz.so
```

After installing zlib-ng, Python's `zlib` module will automatically use it on
the next interpreter start — no code changes are needed.  Verify:

```bash
python3 -c "import zlib; print(zlib.ZLIB_VERSION)"
# Should print e.g. 1.3.0.zlib-ng
```

**RHEL 8** — zlib-ng is available from EPEL as `zlib-ng-compat`.  The package
provides a drop-in `libz.so.1` replacement.  Note that replacing the system
zlib on RHEL 8 requires care: test in a staging environment first and verify
that other zlib consumers (OpenSSL, RPM, etc.) continue to function correctly.

### CRC-32C hardware acceleration

CRC-32C is hardware-accelerated on x86 via the SSE4.2 `CRC32` instruction.
cephsumfs loads `libcrc32c` via ctypes if available; otherwise it falls back to a
pure-Python table implementation (~10× slower).  A `WARNING` is logged on the
first use of the fallback path.

```bash
dnf install libcrc32c    # RHEL 8/9
apt install libcrc32c1   # Ubuntu
```

### Profiling

Run `scripts/profile_performance.py` to measure throughput across all block size
and thread count combinations on your specific system and storage backend:

```bash
python3 scripts/profile_performance.py --size-mib 512 --dir /mnt/ceph
```

The script reports the raw algorithm ceiling, per-combination throughput, whether
the workload is I/O-bound or CPU-bound, and recommended settings for
`run_checksum.sh`.

## Known limitations

**mtime granularity** — The XrdCks wire format stores file mtime as integer
seconds.  A file written and checksummed within the same wall-clock second and
then modified again within that same second will return a stale cached checksum
until the next second boundary.  This is inherent to the XrdCks format and
cannot be resolved without a format change.  In practice, HEP and astronomy data
files are written once and then read-only, so this window is never reached.

**stat/setxattr race** — The file mtime is read with `stat()` and then written
with `setxattr()` as two separate syscalls.  If the file is modified between
them, the stored mtime will refer to the newer modification, causing the cached
entry to appear valid on the next read when it is not.  Closing this window
would require external file locking, which XRootD does not provide to external
checksum helpers.

**No cross-process locking** — If two processes checksum the same file
simultaneously and both find the cache absent or stale, both will compute and
write.  Both compute the same digest (the file has not changed), so the last
`setxattr` write wins and the result is correct.
