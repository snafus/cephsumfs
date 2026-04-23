"""
reader.py — multithreaded file reader for checksum computation.

Design
------
Adler-32 and all other streaming checksums must receive bytes in file order.
Parallelism therefore applies only to *reading*, not to the checksum update
step.

A pool of threads issues concurrent os.pread() calls (pread is thread-safe: it
takes an explicit offset and does not move the file-descriptor position).  The
main thread consumes completed blocks strictly in ascending index order and
feeds each block to the algorithm's update() method.

An "inflight" cap bounds the number of outstanding read futures so that memory
usage stays proportional to (inflight × block_size) regardless of file size.

Allowed block sizes are powers of two from 1 MiB to 64 MiB.  The default of
4 MiB is a reasonable balance between per-call overhead and memory pressure;
32 MiB is recommended for large Ceph objects over a fast network.
"""

import errno
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, Optional

from .algorithms import ChecksumAlgorithm

log = logging.getLogger(__name__)

# O_NOATIME suppresses atime updates when reading files for checksumming.
# It is Linux-specific; on other POSIX systems the flag is silently absent.
_O_NOATIME = getattr(os, "O_NOATIME", 0)

ALLOWED_BLOCK_MIB = (1, 2, 4, 8, 16, 32, 64)
DEFAULT_BLOCK_MIB = 4
DEFAULT_THREADS = 4


def compute_checksum(
    path: str,
    algo: ChecksumAlgorithm,
    block_mib: int = DEFAULT_BLOCK_MIB,
    threads: int = DEFAULT_THREADS,
    inflight: Optional[int] = None,
) -> bytes:
    """
    Compute a checksum over the entire file at *path* and return the raw
    digest bytes.

    Parameters
    ----------
    path:
        Absolute or relative path to the file.
    algo:
        A fresh (not previously updated) ChecksumAlgorithm instance.
    block_mib:
        Read block size in MiB.  Must be one of ALLOWED_BLOCK_MIB.
    threads:
        Number of concurrent read threads.  Values above the storage
        concurrency limit yield diminishing returns.
    inflight:
        Maximum number of outstanding read futures.  Defaults to
        max(2, 2 * threads).  Increasing this can hide read latency at the
        cost of higher memory usage.

    Returns
    -------
    bytes
        Raw digest as returned by algo.digest().

    Raises
    ------
    ValueError
        If block_mib is not in ALLOWED_BLOCK_MIB or threads < 1.
    OSError
        On any file I/O error.
    """
    if block_mib not in ALLOWED_BLOCK_MIB:
        raise ValueError(
            "block_mib must be one of {}; got {}".format(ALLOWED_BLOCK_MIB, block_mib)
        )
    if threads < 1:
        raise ValueError("threads must be >= 1; got {}".format(threads))

    if inflight is None:
        inflight = max(2, 2 * threads)
    if inflight < 1:
        raise ValueError("inflight must be >= 1; got {}".format(inflight))

    block_size = block_mib * 1024 * 1024

    log.debug(
        "compute_checksum path=%r algo=%s block_mib=%d threads=%d inflight=%d",
        path, algo.name, block_mib, threads, inflight,
    )

    try:
        fd = os.open(path, os.O_RDONLY | _O_NOATIME)
    except OSError as exc:
        if exc.errno == errno.EPERM and _O_NOATIME:
            # O_NOATIME requires the process to own the file or hold
            # CAP_FOWNER.  Fall back silently when permission is denied so
            # that world-readable files owned by other users still work.
            log.debug("O_NOATIME denied on %r, retrying without", path)
            fd = os.open(path, os.O_RDONLY)
        else:
            raise
    try:
        file_size = int(os.fstat(fd).st_size)

        if file_size == 0:
            log.debug("empty file, returning initial digest")
            return algo.digest()

        num_blocks = (file_size + block_size - 1) // block_size
        futures: Dict[int, Future] = {}

        def _submit(idx: int, executor: ThreadPoolExecutor) -> None:
            offset = idx * block_size
            size = min(block_size, file_size - offset)
            futures[idx] = executor.submit(os.pread, fd, size, offset)

        with ThreadPoolExecutor(max_workers=threads) as executor:
            # Prime the pipeline up to the inflight cap.
            next_submit = 0
            while next_submit < num_blocks and len(futures) < inflight:
                _submit(next_submit, executor)
                next_submit += 1

            for idx in range(num_blocks):
                data = futures.pop(idx).result()
                expected_size = min(block_size, file_size - idx * block_size)
                if len(data) != expected_size:
                    log.warning(
                        "short read on %r at block %d offset %d: "
                        "expected %d bytes, got %d — file was modified during "
                        "checksum computation",
                        path, idx, idx * block_size, expected_size, len(data),
                    )
                    raise OSError(
                        errno.EIO,
                        "file modified during checksum: short read at block {}".format(idx),
                        path,
                    )
                algo.update(data)
                # Refill pipeline.
                while next_submit < num_blocks and len(futures) < inflight:
                    _submit(next_submit, executor)
                    next_submit += 1

        return algo.digest()
    finally:
        os.close(fd)
