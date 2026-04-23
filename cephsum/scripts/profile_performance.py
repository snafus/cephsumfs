#!/usr/bin/env python3
"""
profile_performance.py — measure cephsumfs throughput and recommend settings.

Runs compute_checksum across all supported block sizes and a range of thread
counts, then reports:

  - Raw algorithm ceiling (in-memory, no I/O)
  - Per-(block_mib, threads) cold and warm throughput on a real temp file
  - Whether the workload is I/O-bound or CPU-bound
  - Recommended settings for run_checksum.sh

Cold throughput is measured after dropping the OS page cache (requires root).
Warm throughput is the mean of subsequent passes with the file in cache.
For Ceph tuning, cold throughput is the relevant figure — XRootD typically
checksums large files that have not been recently accessed.

Usage
-----
    python3 scripts/profile_performance.py [options]

    --size-mib N        Test file size in MiB (default: 512)
    --algo ALGO         Algorithm to profile (default: adler32)
    --dir PATH          Directory for the temp file; use the actual Ceph
                        mount point to profile against real storage
                        (default: system temp directory)
    --max-threads N     Highest thread count to test (default: min(8, cpus))
    --min-duration S    Minimum seconds for warm passes per measurement (default: 2.0)
    --json              Print results as JSON instead of a table
    --verbose           Show progress during measurement

The script can be run directly from the source tree without installing
the package:

    cd /path/to/cephsumfs
    python3 scripts/profile_performance.py
"""

import argparse
import json
import os
import sys
import tempfile
import time
import zlib

# Allow running directly from the repository root without installing.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from cephsumfs.algorithms import ALGORITHM_REGISTRY, get_algorithm
from cephsumfs.reader import ALLOWED_BLOCK_MIB, compute_checksum


# ---------------------------------------------------------------------------
# System information
# ---------------------------------------------------------------------------

def _cpu_count() -> int:
    return os.cpu_count() or 1


def _zlib_info() -> dict:
    version = zlib.ZLIB_VERSION
    is_ng = "zlib-ng" in version
    return {"version": version, "zlib_ng": is_ng}


def _cpu_info() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "unknown"


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------

_cache_drop_warned = False


def _drop_caches() -> bool:
    """
    Drop the kernel page cache, dentry cache, and inode cache.

    Writes 3 to /proc/sys/vm/drop_caches — requires root.  Returns True on
    success, False if permission is denied.  Prints a warning to stderr on
    the first failure so the caller knows cold-read figures are unreliable.
    """
    global _cache_drop_warned
    try:
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        return True
    except OSError:
        if not _cache_drop_warned:
            print(
                "WARNING: cannot drop page cache (/proc/sys/vm/drop_caches): "
                "not root?  Cold-read figures will reflect warm cache and will "
                "overstate storage throughput.",
                file=sys.stderr,
            )
            _cache_drop_warned = True
        return False


# ---------------------------------------------------------------------------
# Measurement helpers
# ---------------------------------------------------------------------------

def _measure_ceiling(algo_name: str, block_mib: int, min_duration: float) -> float:
    """
    Return algorithm throughput in MB/s with no I/O — data already in memory.

    This is the theoretical maximum the algorithm can achieve on this CPU,
    independent of storage speed.
    """
    block_size = block_mib * 1024 * 1024
    data = os.urandom(block_size)

    # Warm-up pass to populate CPU caches.
    algo = get_algorithm(algo_name)
    algo.update(data)

    elapsed = 0.0
    total_bytes = 0
    algo = get_algorithm(algo_name)
    t_start = time.perf_counter()
    while elapsed < min_duration:
        algo.update(data)
        total_bytes += block_size
        elapsed = time.perf_counter() - t_start

    return total_bytes / 1024 / 1024 / elapsed


def _measure_pipeline(
    path: str,
    file_size_mib: int,
    algo_name: str,
    block_mib: int,
    threads: int,
    min_duration: float,
    verbose: bool,
) -> dict:
    """
    Return cold and warm end-to-end throughput in MB/s including real file I/O.

    Cold: one pass after dropping the OS page cache (requires root; falls back
    to an uncached-best-effort first pass if cache drop fails).

    Warm: mean of subsequent passes until min_duration seconds have elapsed
    (at least one additional pass).  Warm figures reflect the OS page cache
    serving the data — useful as a ceiling for repeated-access workloads.
    """
    # --- Cold pass ---
    _drop_caches()
    algo = get_algorithm(algo_name)
    t0 = time.perf_counter()
    compute_checksum(path, algo, block_mib=block_mib, threads=threads)
    cold_elapsed = time.perf_counter() - t0
    cold_mbs = file_size_mib / cold_elapsed

    # --- Warm passes ---
    warm_times = []
    deadline = time.perf_counter() + min_duration
    while time.perf_counter() < deadline or len(warm_times) < 1:
        algo = get_algorithm(algo_name)
        t0 = time.perf_counter()
        compute_checksum(path, algo, block_mib=block_mib, threads=threads)
        warm_times.append(time.perf_counter() - t0)

    warm_mbs = file_size_mib / (sum(warm_times) / len(warm_times))

    if verbose:
        print(
            "    block_mib={:2d} threads={}: cold={:.0f} MB/s  warm={:.0f} MB/s"
            "  ({} warm passes)".format(
                block_mib, threads, cold_mbs, warm_mbs, len(warm_times),
            ),
            flush=True,
        )

    return {"cold_mbs": cold_mbs, "warm_mbs": warm_mbs}


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def _recommend(results: list, ceiling_mbs: float) -> dict:
    """
    Given a list of {block_mib, threads, cold_mbs, warm_mbs} dicts and the
    in-memory algorithm ceiling, return recommended settings and a diagnosis.

    Recommendations are based on cold throughput — that is the relevant figure
    for XRootD checksumming large, infrequently accessed files.
    """
    best = max(results, key=lambda r: r["cold_mbs"])
    ratio = best["cold_mbs"] / ceiling_mbs if ceiling_mbs > 0 else 0.0

    if ratio > 0.85:
        bound = "cpu"
        diagnosis = (
            "CPU-bound: storage delivers data faster than the algorithm can "
            "process it.  Smaller blocks (4-8 MiB) reduce L3 cache pressure.  "
            "Consider replacing zlib with zlib-ng for SIMD-accelerated adler32 "
            "(10-15 GB/s on AVX2 hardware)."
        )
    elif ratio > 0.50:
        bound = "mixed"
        diagnosis = (
            "Mixed: neither storage I/O nor CPU is clearly dominant.  The "
            "recommended settings below balance both."
        )
    else:
        bound = "io"
        diagnosis = (
            "I/O-bound: storage read speed is the limiting factor.  Larger "
            "blocks (16-32 MiB) amortise per-read RPC overhead on high-latency "
            "networked storage such as Ceph over 10/25 GbE."
        )

    return {
        "bound": bound,
        "diagnosis": diagnosis,
        "best_block_mib": best["block_mib"],
        "best_threads": best["threads"],
        "best_cold_mbs": best["cold_mbs"],
        "best_warm_mbs": best["warm_mbs"],
        "ceiling_utilisation": ratio,
    }


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def _print_throughput_table(
    label: str,
    key: str,
    results: list,
    recommendation: dict,
    best_block: int,
    best_threads: int,
) -> None:
    thread_vals = sorted(set(r["threads"] for r in results))
    block_vals  = sorted(set(r["block_mib"] for r in results))

    print("  {} throughput (MB/s)".format(label))
    print("  " + "-" * 50)
    header = "  {:>10}".format("block\\threads")
    for t in thread_vals:
        header += "  {:>7}".format("t={}".format(t))
    print(header)

    for blk in block_vals:
        row = "  {:>7d} MiB".format(blk)
        for t in thread_vals:
            match = next(
                (r for r in results if r["block_mib"] == blk and r["threads"] == t),
                None,
            )
            if match:
                marker = "*" if (blk == best_block and t == best_threads) else " "
                row += "  {:6.0f}{}".format(match[key], marker)
            else:
                row += "  {:>7}".format("—")
        print(row)
    print()


def _print_table(
    sys_info: dict,
    ceiling: dict,
    results: list,
    recommendation: dict,
    size_mib: int,
    algo: str,
    test_dir: str,
    cache_dropped: bool,
) -> None:
    W = 70
    print("=" * W)
    print("  cephsumfs performance profile")
    print("=" * W)
    print("  CPU        : {}".format(sys_info["cpu"]))
    print("  CPU cores  : {}".format(sys_info["cpu_count"]))
    print("  zlib       : {}{}".format(
        sys_info["zlib"]["version"],
        "  [zlib-ng SIMD active]" if sys_info["zlib"]["zlib_ng"] else
        "  [standard zlib — consider zlib-ng for adler32 speedup]"
    ))
    print("  Algorithm  : {}".format(algo))
    print("  Test file  : {} MiB in {}".format(size_mib, test_dir))
    print("  Cache drop : {}".format(
        "yes (cold figures are true storage throughput)" if cache_dropped
        else "no (root required) — cold figures reflect warm cache"
    ))
    print()

    print("  Algorithm ceiling (in-memory, no I/O)")
    print("  " + "-" * 50)
    for blk, mbs in sorted(ceiling.items()):
        bar = "#" * int(mbs / 100)
        print("    block {:2d} MiB : {:6.0f} MB/s  {}".format(blk, mbs, bar))
    print()

    best_block   = recommendation["best_block_mib"]
    best_threads = recommendation["best_threads"]

    _print_throughput_table(
        "Cold (after cache drop) pipeline", "cold_mbs",
        results, recommendation, best_block, best_threads,
    )
    _print_throughput_table(
        "Warm (page-cache) pipeline", "warm_mbs",
        results, recommendation, best_block, best_threads,
    )

    print("  * = best cold combination (used for recommendation)")
    print()

    rec = recommendation
    print("  Diagnosis: {}".format(rec["bound"].upper()))
    print()
    words = rec["diagnosis"].split()
    line = "  "
    for w in words:
        if len(line) + len(w) + 1 > W:
            print(line)
            line = "  " + w + " "
        else:
            line += w + " "
    if line.strip():
        print(line)
    print()

    print("  Recommended run_checksum.sh settings:")
    print("    CEPHSUM_BLOCK_MIB={}".format(rec["best_block_mib"]))
    print("    CEPHSUM_THREADS={}".format(rec["best_threads"]))
    suggested_inflight = rec["best_threads"] * (2 if rec["bound"] == "cpu" else 4)
    print("    CEPHSUM_INFLIGHT={}  # {}, adjust for network latency".format(
        suggested_inflight,
        "2×threads (CPU-bound)" if rec["bound"] == "cpu" else "4×threads (I/O-bound)",
    ))
    print("    # Expected cold throughput: ~{:.0f} MB/s".format(rec["best_cold_mbs"]))
    print("    # Expected warm throughput: ~{:.0f} MB/s".format(rec["best_warm_mbs"]))
    print("    # CPU utilisation of algorithm ceiling: {:.0f}%".format(
        rec["ceiling_utilisation"] * 100))
    if rec["bound"] != "cpu":
        print("    # Run with --dir /mnt/ceph to profile against real CephFS storage")
    print("=" * W)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(
        description="Profile cephsumfs throughput and recommend run_checksum.sh settings."
    )
    p.add_argument("--size-mib", type=int, default=512,
                   help="Test file size in MiB (default: 512)")
    p.add_argument("--algo", default="adler32",
                   choices=sorted(ALGORITHM_REGISTRY),
                   help="Algorithm to profile (default: adler32)")
    p.add_argument("--dir", default=None,
                   help="Directory for the temp file; set to your Ceph mount "
                        "to profile against real storage (default: system tmp)")
    p.add_argument("--max-threads", type=int, default=None,
                   help="Maximum thread count to test (default: min(8, cpu_count))")
    p.add_argument("--min-duration", type=float, default=2.0,
                   help="Minimum seconds for warm passes per measurement (default: 2.0)")
    p.add_argument("--json", action="store_true",
                   help="Print results as JSON")
    p.add_argument("--verbose", action="store_true",
                   help="Show progress during measurement")
    args = p.parse_args()

    cpus = _cpu_count()
    max_threads = args.max_threads or min(8, cpus)
    thread_counts = sorted(set(
        t for t in [1, 2, 4, 8, max_threads] if 1 <= t <= max_threads
    ))

    # Ceiling: test block sizes 1, 4, 16 MiB (covers cache-resident vs not).
    ceiling_blocks = [1, 4, 16]
    # Pipeline: test all allowed block sizes up to 32 MiB.
    pipeline_blocks = [b for b in ALLOWED_BLOCK_MIB if b <= 32]

    sys_info = {
        "cpu": _cpu_info(),
        "cpu_count": cpus,
        "zlib": _zlib_info(),
    }

    # --- Algorithm ceiling ---
    if args.verbose or not args.json:
        print("Measuring algorithm ceiling (in-memory)...", flush=True)

    ceiling = {}
    for blk in ceiling_blocks:
        mbs = _measure_ceiling(args.algo, blk, args.min_duration)
        ceiling[blk] = mbs

    # Use the 4 MiB block ceiling as the reference (fits in most L3 caches).
    ref_ceiling = ceiling.get(4, max(ceiling.values()))

    # --- Pipeline throughput ---
    if args.verbose or not args.json:
        print("Measuring pipeline throughput on {}-MiB file...".format(args.size_mib),
              flush=True)

    tmp_dir = args.dir or tempfile.gettempdir()
    tmp_fd, tmp_path = tempfile.mkstemp(dir=tmp_dir, prefix="cephsumfs_profile_")
    try:
        # Write test file and flush to storage before any reads.
        chunk = os.urandom(min(args.size_mib, 16) * 1024 * 1024)
        written = 0
        target = args.size_mib * 1024 * 1024
        while written < target:
            n = min(len(chunk), target - written)
            os.write(tmp_fd, chunk[:n])
            written += n
        os.fsync(tmp_fd)
        os.close(tmp_fd)

        # Attempt an initial cache drop so the first measurement pass is cold.
        cache_dropped = _drop_caches()

        results = []
        for block_mib in pipeline_blocks:
            for threads in thread_counts:
                measurement = _measure_pipeline(
                    tmp_path, args.size_mib, args.algo,
                    block_mib, threads,
                    args.min_duration, args.verbose,
                )
                results.append({
                    "block_mib": block_mib,
                    "threads": threads,
                    "cold_mbs": measurement["cold_mbs"],
                    "warm_mbs": measurement["warm_mbs"],
                })
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        try:
            os.close(tmp_fd)
        except OSError:
            pass

    recommendation = _recommend(results, ref_ceiling)

    if args.json:
        output = {
            "system": sys_info,
            "algo": args.algo,
            "test_size_mib": args.size_mib,
            "test_dir": tmp_dir,
            "cache_dropped": cache_dropped,
            "ceiling_mbs": ceiling,
            "results": results,
            "recommendation": recommendation,
        }
        print(json.dumps(output, indent=2))
    else:
        _print_table(
            sys_info, ceiling, results, recommendation,
            args.size_mib, args.algo, tmp_dir, cache_dropped,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
