# cephsumfs

External checksum helper for XRootD on Ceph-backed POSIX storage.

Computes and caches file checksums via Linux extended attributes using the
XRootD `XrdCks` wire format.  XRootD delegates checksum requests to this
tool via the `xrd.chksum` directive rather than computing them inline.

## Features

- Multithreaded file reading (`os.pread`) for high-throughput checksum computation
- Checksum caching via Linux xattrs in XRootD `XrdCks` wire format
- Automatic cache invalidation: cached values are validated against file mtime
  before use — modified files are always recomputed
- Supports adler32, crc32, crc32c, md5, sha256; new algorithms can be registered
  at runtime without modifying the package
- Zero external Python dependencies (stdlib only, Python ≥ 3.6.8)
- RPM-packaged for RHEL 8, RHEL 9, and EPEL

## Components

| Module | Role |
|--------|------|
| `cephsumfs/algorithms.py` | Algorithm ABC and registry (adler32, crc32, crc32c, md5, sha256) |
| `cephsumfs/reader.py` | Algorithm-agnostic multithreaded `os.pread()` pipeline |
| `cephsumfs/xattr.py` | XrdCks 96-byte blob serialisation and mtime validation |
| `cephsumfs/cli.py` | CLI entry point; strict stdout / errno exit-code contract |
| `scripts/run_checksum.sh` | Thin shell wrapper invoked by XRootD |

## Requirements

- Linux with xattr support (`user.*` namespace, enabled on the filesystem)
- Python 3.6.8 or later
- No third-party Python packages
- `libcrc32c` recommended for hardware-accelerated CRC-32C (see below)

## Installation

```bash
# From RPM (recommended for production)
dnf install cephsumfs

# From source
pip install .
```

## XRootD integration

```
# /etc/xrootd/xrootd.cfg
xrd.chksum adler32 /usr/libexec/cephsumfs/run_checksum.sh
```

The `xrootd` user must be able to execute the wrapper script and read the
files being checksummed.

## CLI usage

```bash
# Return cached checksum, or compute and cache it (normal XRootD path)
cephsumfs /path/to/file

# Verify cached checksum against file data
cephsumfs --verify /path/to/file

# Force recompute and overwrite cached value
cephsumfs --override /path/to/file

# Remove cached checksum xattr
cephsumfs --remove /path/to/file

# Compute only — do not read or write xattr
cephsumfs --compute-only /path/to/file

# Use a different algorithm
cephsumfs --algo sha256 /path/to/file

# Enable diagnostic logging
cephsumfs --log-file /var/log/cephsumfs.log /path/to/file
```

On success exactly `<hex-digest>\n` is written to stdout; the process exits 0.
On error nothing is written to stdout; a message goes to the log and the
process exits with an errno-derived code.

## run_checksum.sh configuration

All site-specific values are controlled by environment variables; no file
editing is required after deployment.

| Variable | Default | Description |
|----------|---------|-------------|
| `CEPHSUM_PREFIX` | `/mnt` | Filesystem prefix prepended to the XRootD LFN |
| `CEPHSUM_ALGO` | `adler32` | Checksum algorithm |
| `CEPHSUM_BLOCK_MIB` | `4` | Read block size in MiB |
| `CEPHSUM_THREADS` | `4` | Concurrent read threads |
| `CEPHSUM_LOG_FILE` | *(unset)* | Append diagnostics to this path |
| `CEPHSUM_PYTHON` | `python3` | Python interpreter |

## xattr format

Checksums are stored under `user.XrdCks.<algo>` as a 96-byte big-endian blob:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 16 B | name | Algorithm name, NUL-padded |
| 16 | 8 B | fmTime | File mtime at compute time (uint64, seconds) |
| 24 | 4 B | csTime | Elapsed seconds during computation (uint32) |
| 28 | 2 B | — | Reserved |
| 30 | 1 B | — | Reserved |
| 31 | 1 B | length | Meaningful bytes in value[] |
| 32 | 64 B | value | Raw digest bytes, zero-padded |

## Performance

### Architecture

The reader opens the file once with `O_RDONLY | O_NOATIME` and submits
concurrent `os.pread()` calls on a thread pool (pread is POSIX thread-safe: it
takes an explicit offset and does not move the file-descriptor position).  The
main thread consumes completed blocks strictly in file order and feeds each
block to the checksum algorithm.  An inflight cap bounds peak memory usage to
`inflight × block_size` regardless of file size.

Checksum computation is necessarily sequential (adler32 and CRC variants require
bytes in order), so threads do not accelerate the CPU path — they hide per-read
latency on high-latency networked storage.

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
