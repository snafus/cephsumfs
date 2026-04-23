"""
cli.py — command-line entry point for cephsumfs.

Exit-code contract
------------------
XRootD invokes this tool as an external checksum helper and inspects both
stdout and the exit code.

  stdout  : exactly ``<8-hex-digits>\n`` on success; nothing on error
  exit 0  : success
  exit >0 : errno-based failure code (ENOENT=2, EACCES=13, EIO=5, …)

No other output must appear on stdout under any circumstances.  Diagnostic
messages go to the log file and/or stderr only when explicitly enabled.

Operation modes
---------------
default (no flags)
    Read the cached checksum from xattr.  If absent or stale (file mtime has
    changed since the checksum was stored), compute from file data and write
    the result back to xattr.

--verify
    Read the cached xattr; compute from file data; fail if they differ.
    Requires the xattr to be present.

--remove
    Read the cached xattr, delete it, print the removed checksum.

--override
    Always compute from file data and overwrite xattr regardless of cache.

--compute-only
    Compute from file data; do not read or write xattr.

--dry-run
    Compute from file data if needed; never write xattr.
"""

import argparse
import errno
import logging
import os
import sys
import time
from typing import NoReturn, Optional

from .algorithms import ALGORITHM_REGISTRY, get_algorithm
from .reader import ALLOWED_BLOCK_MIB, DEFAULT_BLOCK_MIB, DEFAULT_THREADS, compute_checksum
from .xattr import XrdCksRecord, delete_xattr, read_xattr, write_xattr

log = logging.getLogger("cephsumfs")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _configure_logging(log_file: Optional[str], log_stderr: bool) -> None:
    """Attach file and/or stderr handlers to the root cephsumfs logger.

    Idempotent with respect to real handlers: if a FileHandler or
    StreamHandler is already attached this function returns immediately.
    A NullHandler added by a prior no-logging call is removed so that a
    subsequent call with real handlers takes effect (relevant in tests that
    call main() more than once with different arguments).
    """
    real = [h for h in log.handlers if not isinstance(h, logging.NullHandler)]
    if real:
        return
    for h in list(log.handlers):
        log.removeHandler(h)

    if not log_file and not log_stderr:
        log.addHandler(logging.NullHandler())
        return

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    log.setLevel(logging.DEBUG)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        log.addHandler(fh)

    if log_stderr:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        log.addHandler(sh)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _exit_errno(code: int, message: str) -> NoReturn:
    log.error("%s", message)
    raise SystemExit(code if 0 < code <= 255 else 1)


def _compute(
    path: str,
    algo_name: str,
    block_mib: int,
    threads: int,
    inflight: Optional[int],
) -> bytes:
    """Compute a fresh checksum from file data and return raw digest bytes."""
    log.debug("computing %s for %r block_mib=%d threads=%d", algo_name, path, block_mib, threads)
    algo = get_algorithm(algo_name)
    return compute_checksum(path, algo, block_mib=block_mib, threads=threads, inflight=inflight)


def _write_result(digest_hex: str) -> None:
    sys.stdout.write(digest_hex + "\n")
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Operation modes
# ---------------------------------------------------------------------------

def _mode_remove(path: str, algo: str, namespace: str) -> None:
    try:
        rec = read_xattr(path, algo, namespace=namespace)
    except OSError as exc:
        _exit_errno(
            exc.errno or errno.ENODATA,
            "xattr not found for remove on {!r}: {}".format(path, exc),
        )
    except ValueError as exc:
        _exit_errno(errno.EINVAL, "malformed xattr on {!r}: {}".format(path, exc))

    try:
        delete_xattr(path, algo, namespace=namespace)
    except OSError as exc:
        _exit_errno(
            exc.errno or errno.EACCES,
            "failed to remove xattr from {!r}: {}".format(path, exc),
        )

    _write_result(rec.digest_hex())


def _mode_verify(
    path: str,
    algo: str,
    namespace: str,
    block_mib: int,
    threads: int,
    inflight: Optional[int],
) -> None:
    try:
        rec = read_xattr(path, algo, namespace=namespace)
    except OSError as exc:
        _exit_errno(
            exc.errno or errno.ENODATA,
            "xattr not found for verify on {!r}: {}".format(path, exc),
        )
    except ValueError as exc:
        _exit_errno(errno.EINVAL, "malformed xattr on {!r}: {}".format(path, exc))

    # is_current() is intentionally not checked here: --verify always
    # recomputes from file data regardless of mtime, so the comparison
    # below is the authoritative integrity check.
    meta_hex = rec.digest_hex()
    data_digest = _compute(path, algo, block_mib, threads, inflight)
    data_hex = data_digest.hex()

    if meta_hex != data_hex:
        _exit_errno(
            errno.EIO,
            "checksum mismatch on {!r}: cached={} computed={}".format(
                path, meta_hex, data_hex
            ),
        )

    _write_result(meta_hex)


def _mode_default(
    path: str,
    algo: str,
    namespace: str,
    block_mib: int,
    threads: int,
    inflight: Optional[int],
    override: bool,
    compute_only: bool,
    dry_run: bool,
) -> None:
    # --override and --compute-only both skip the cache read.
    if not override and not compute_only:
        try:
            rec = read_xattr(path, algo, namespace=namespace)
            if rec.is_current(path):
                _write_result(rec.digest_hex())
                return
            log.debug("cached checksum stale for %r (mtime changed), recomputing", path)
        except OSError:
            log.debug("no cached checksum for %r, computing", path)
        except ValueError as exc:
            # Malformed blob; do not silently ignore — surface as an error so
            # operators know the xattr needs repair.
            _exit_errno(errno.EINVAL, "malformed xattr on {!r}: {}".format(path, exc))

    start = time.monotonic()
    data_digest = _compute(path, algo, block_mib, threads, inflight)
    cs_delta = max(0, int(time.monotonic() - start))
    data_hex = data_digest.hex()

    should_write = not compute_only and not dry_run
    if should_write:
        try:
            write_xattr(path, algo, data_digest, namespace=namespace, cs_delta=cs_delta)
        except OSError as exc:
            _exit_errno(
                exc.errno or errno.EACCES,
                "failed to write xattr on {!r}: {}".format(path, exc),
            )
        except ValueError as exc:
            _exit_errno(errno.EINVAL, str(exc))

    _write_result(data_hex)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    algo_choices = sorted(ALGORITHM_REGISTRY)

    p = argparse.ArgumentParser(
        prog="cephsumfs",
        description=(
            "Compute and cache file checksums for XRootD using Linux xattrs "
            "(XrdCks format).  On success prints the checksum hex to stdout "
            "and exits 0.  On error prints nothing to stdout and exits with "
            "an errno-based code."
        ),
    )
    p.add_argument("path", help="Path to the file to checksum.")
    p.add_argument(
        "--algo",
        default="adler32",
        choices=algo_choices,
        metavar="ALGO",
        help="Checksum algorithm.  Choices: {}.  Default: adler32.".format(
            ", ".join(algo_choices)
        ),
    )
    p.add_argument(
        "--namespace",
        default="user",
        help="xattr namespace prefix (default: user).",
    )
    p.add_argument(
        "--block-mib",
        type=int,
        default=DEFAULT_BLOCK_MIB,
        choices=ALLOWED_BLOCK_MIB,
        metavar="N",
        help="Read block size in MiB.  Choices: {}.  Default: {}.".format(
            ALLOWED_BLOCK_MIB, DEFAULT_BLOCK_MIB
        ),
    )
    p.add_argument(
        "--threads",
        type=int,
        default=DEFAULT_THREADS,
        help="Concurrent read threads (default: {}).".format(DEFAULT_THREADS),
    )
    p.add_argument(
        "--inflight",
        type=int,
        default=None,
        help="Maximum outstanding read requests (default: 2 × threads).",
    )

    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--verify",
        action="store_true",
        help="Verify cached checksum against file data.  Fails if xattr absent or mismatched.",
    )
    mode.add_argument(
        "--remove",
        action="store_true",
        help="Remove the cached xattr and print the removed checksum.",
    )

    p.add_argument(
        "--override",
        action="store_true",
        help="Recompute and overwrite xattr even if a valid cached value exists.",
    )
    p.add_argument(
        "--compute-only",
        action="store_true",
        help="Compute from file data without reading or writing xattr.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute if needed but never write xattr.",
    )

    p.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help="Append diagnostic log to PATH (never written to stdout).",
    )
    p.add_argument(
        "--log-stderr",
        action="store_true",
        help="Also emit diagnostic log to stderr.",
    )

    return p


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    p = _build_parser()
    args = p.parse_args(argv)

    _configure_logging(args.log_file, args.log_stderr)

    # Validate that write-path flags are not combined with read-only modes.
    if args.verify or args.remove:
        mode = "--verify" if args.verify else "--remove"
        for val, name in [
            (args.override,     "--override"),
            (args.compute_only, "--compute-only"),
            (args.dry_run,      "--dry-run"),
        ]:
            if val:
                p.error("{} has no effect with {}".format(name, mode))

    path = args.path
    algo = args.algo

    try:
        if args.remove:
            _mode_remove(path, algo, args.namespace)

        elif args.verify:
            _mode_verify(
                path, algo, args.namespace,
                args.block_mib, args.threads, args.inflight,
            )

        else:
            _mode_default(
                path, algo, args.namespace,
                args.block_mib, args.threads, args.inflight,
                override=args.override,
                compute_only=args.compute_only,
                dry_run=args.dry_run,
            )

    except SystemExit:
        raise
    except FileNotFoundError:
        _exit_errno(errno.ENOENT, "file not found: {!r}".format(path))
    except PermissionError as exc:
        _exit_errno(errno.EACCES, "permission denied: {}".format(exc))
    except OSError as exc:
        _exit_errno(exc.errno or errno.EIO, "OS error: {}".format(exc))
    except Exception as exc:
        _exit_errno(1, "unexpected error: {}".format(exc))

    return 0


if __name__ == "__main__":
    sys.exit(main())
