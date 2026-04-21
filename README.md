# cephsum

External checksum script for XRootD, providing Adler-32 computation and metadata management for files on Ceph-backed storage.

Checksums are stored in and retrieved from Linux extended attributes using XRootD's `XrdCks` binary format, allowing XRootD to delegate checksum operations to this tool rather than computing them inline.

## Features

- Multithreaded file reading for high-throughput checksum computation
- Checksum caching via Linux xattrs in XRootD `XrdCks` wire format
- Automatic cache invalidation based on file mtime
- Verify, remove, or force-recompute stored checksums
- Zero external Python dependencies (stdlib only, Python 3.6.8+)

## Components

| File | Role |
|------|------|
| `mt_adler32.py` | Multithreaded Adler-32 computation using `os.pread()` for concurrent block reads |
| `xrdcks_xattr.py` | Serialisation/deserialisation of the 96-byte `XrdCks` xattr blob |
| `xrd_adler32_tool.py` | Main CLI entry point; orchestrates read-from-cache or compute-and-store workflow |
| `run_checksum.sh` | Thin Bash wrapper called by XRootD as an external checksum helper |

## Requirements

- Linux with xattr support (`user.*` namespace)
- Python 3.6.8 or later
- No third-party packages

## Usage

### Called by XRootD

XRootD invokes `run_checksum.sh <lfn>` when it needs a checksum. The script resolves the local path and delegates to `xrd_adler32_tool.py`.

### Direct CLI

```bash
# Return cached checksum, or compute and cache it
python xrd_adler32_tool.py /path/to/file

# Verify the cached checksum against the file content
python xrd_adler32_tool.py --verify /path/to/file

# Force recompute and overwrite cached value
python xrd_adler32_tool.py --override /path/to/file

# Remove cached checksum
python xrd_adler32_tool.py --remove /path/to/file

# Compute only, do not read or write xattr
python xrd_adler32_tool.py --compute-only /path/to/file
```

On success the checksum hex string is written to stdout and the process exits 0. On error nothing is written to stdout; a message goes to stderr (or a log file) and the process exits with an errno-derived code.

### Tuning

```bash
python xrd_adler32_tool.py --block-mib 32 --threads 8 /path/to/file
```

Default block size is 4 MiB; default thread count is 4. `run_checksum.sh` uses 32 MiB blocks and 4 threads.

## XRootD Integration

Add to your XRootD configuration:

```
xrd.chksum adler32 /etc/xrootd/cephsum/run_checksum.sh
```

Ensure the script and Python files are deployed to the path referenced in `run_checksum.sh` and that the `xrootd` user can execute them.

## xattr Format

Checksums are stored under `user.XrdCks.adler32`. The value is a 96-byte big-endian binary blob matching the `XrdCks` struct:

| Field | Size | Description |
|-------|------|-------------|
| fmtid | 16 B | Format identifier |
| mtime | 8 B | File mtime at compute time |
| tdelta | 4 B | Seconds elapsed during computation |
| cslen | 2 B | Digest length in bytes |
| ncslen | 1 B | Name length |
| cstype | 1 B | Checksum type byte |
| digest | 64 B | Raw digest bytes |

## License

TBD
