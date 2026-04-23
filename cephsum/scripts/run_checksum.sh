#!/bin/sh
#
# run_checksum.sh — XRootD external checksum helper for cephsumfs.
#
# XRootD calls this script as:
#
#   run_checksum.sh <logical-file-name>
#
# where <logical-file-name> is the LFN relative to the storage root.
# The script must print exactly the checksum hex string followed by a
# newline to stdout, and exit 0 on success or non-zero on failure.
#
# Configuration via environment variables
# ----------------------------------------
# CEPHSUM_PREFIX      Filesystem prefix prepended to the LFN to form the
#                     full local path.  Trailing slash is stripped.
#                     Default: /mnt
#
# CEPHSUM_ALGO        Checksum algorithm passed to --algo.
#                     Default: adler32
#
# CEPHSUM_BLOCK_MIB   Read block size in MiB passed to --block-mib.
#                     Should align with the CephFS stripe_unit (query with
#                     `getfattr -n ceph.file.layout <file>`; default is
#                     4 MiB).  Misaligned block sizes cause the kernel
#                     client to split or merge RADOS object reads.
#                     Default: 4
#
# CEPHSUM_THREADS     Concurrent read threads passed to --threads.
#                     For CephFS, set this to match the pool stripe_count
#                     so that concurrent reads hit independent OSDs.
#                     Default: 4
#
# CEPHSUM_INFLIGHT    Maximum outstanding read requests.  Higher values
#                     pipeline more RADOS RPCs to hide OSD round-trip
#                     latency on networked CephFS.  If unset, the Python
#                     code defaults to 2 × CEPHSUM_THREADS; for networked
#                     Ceph a value of 4 × CEPHSUM_THREADS is recommended.
#                     Default: (unset — let cephsumfs choose 2 × threads)
#
# CEPHSUM_LOG_FILE    If set, diagnostic log is appended to this path.
#                     Paths containing spaces are handled correctly.
#
# XRootD configuration example
# -----------------------------
#   xrd.chksum adler32 /usr/libexec/cephsumfs/run_checksum.sh
#
# CephFS tuning example (stripe_count=4, stripe_unit=4 MiB)
# ----------------------------------------------------------
#   CEPHSUM_BLOCK_MIB=4
#   CEPHSUM_THREADS=4
#   CEPHSUM_INFLIGHT=16   # 4 × threads
#

CEPHSUM_PREFIX="${CEPHSUM_PREFIX:-/mnt}"
CEPHSUM_ALGO="${CEPHSUM_ALGO:-adler32}"
CEPHSUM_BLOCK_MIB="${CEPHSUM_BLOCK_MIB:-4}"
CEPHSUM_THREADS="${CEPHSUM_THREADS:-4}"

# Strip any trailing slash from the prefix so we never produce double slashes.
CEPHSUM_PREFIX="${CEPHSUM_PREFIX%/}"

if [ -z "$1" ]; then
    echo "usage: run_checksum.sh <lfn>" >&2
    exit 1
fi

FULL_PATH="${CEPHSUM_PREFIX}/${1}"

# Build the argument list using positional parameters so that values
# containing spaces (e.g. CEPHSUM_LOG_FILE) are passed as single tokens.
set -- \
    --algo "${CEPHSUM_ALGO}" \
    --block-mib "${CEPHSUM_BLOCK_MIB}" \
    --threads "${CEPHSUM_THREADS}"

if [ -n "${CEPHSUM_INFLIGHT}" ]; then
    set -- "$@" --inflight "${CEPHSUM_INFLIGHT}"
fi

if [ -n "${CEPHSUM_LOG_FILE}" ]; then
    set -- "$@" --log-file "${CEPHSUM_LOG_FILE}"
fi

exec cephsumfs "$@" "${FULL_PATH}"
exit $?
