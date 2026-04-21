#!/usr/bin/env python3
"""
profile_performance.py — measure cephsumfs throughput and recommend settings.

Runs compute_checksum across all supported block sizes and a range of thread
counts, then reports:

  - Raw algorithm ceiling (in-memory, no I/O)
  - Per-(block_mib, threads) throughput on a real temp file
  - Whether the workload is I/O-bound or CPU-bound
  - Recommended settings for run_checksum.sh

Usage
-----
    python3 scripts/profile_performance.py [options]

    --size-mib N        Test file size in MiB (default: 512)
    --algo ALGO         Algorithm to profile (default: adler32)
    --dir PATH          Directory for the temp file; use the actual Ceph
                        mount point to profile against real storage
                        (default: system temp directory)
    --max-threads N     Highest thread count to test (default: min(8, cpus))
    --min-duration S    Minimum seconds per measurement (default: 2.0)
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
) -> float:
    """
    Return end-to-end throughput in MB/s including real file I/O.

    Runs repeatedly until min_duration seconds have elapsed and returns the
    mean throughput.  Uses the best of three runs to reduce noise from page
    cache cold-start effects.
    """
    times = []
    deadline = time.perf_counter() + min_duration

    # Always do at least two passes; stop when min_duration is reached.
    while time.perf_counter() < deadline or len(times) < 2:
        algo = get_algorithm(algo_name)
        t0 = time.perf_counter()
        compute_checksum(path, algo, block_mib=block_mib, threads=threads)
        times.append(time.perf_counter() - t0)

    if verbose:
        print(
            "    block_mib={:2d} threads={}: {:.0f} MB/s ({} passes)".format(
                block_mib, threads,
                file_size_mib / min(times),
                len(times),
            ),
            flush=True,
        )

    # Report best observed (closest to storage ceiling, fewest cache effects).
    return file_size_mib / min(times)


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def _recommend(results: list, ceiling_mbs: float) -> dict:
    """
    Given a list of {block_mib, threads, throughput_mbs} dicts and the
    in-memory algorithm ceiling, return recommended settings and a diagnosis.
    """
    best = max(results, key=lambda r: r["throughput_mbs"])
    ratio = best["throughput_mbs"] / ceiling_mbs if ceiling_mbs > 0 else 0.0

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
        "best_throughput_mbs": best["throughput_mbs"],
        "ceiling_utilisation": ratio,
    }


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def _print_table(
    sys_info: dict,
    ceiling: dict,
    results: list,
    recommendation: dict,
    size_mib: int,
    algo: str,
    test_dir: str,
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
    print()

    print("  Algorithm ceiling (in-memory, no I/O)")
    print("  " + "-" * 50)
    for blk, mbs in sorted(ceiling.items()):
        bar = "#" * int(mbs / 100)
        print("    block {:2d} MiB : {:6.0f} MB/s  {}".format(blk, mbs, bar))
    print()

    print("  Pipeline throughput (real I/O)")
    print("  " + "-" * 50)

    thread_vals = sorted(set(r["threads"] for r in results))
    block_vals  = sorted(set(r["block_mib"] for r in results))

    # Header
    header = "  {:>10}".format("block\\threads")
    for t in thread_vals:
        header += "  {:>7}".format("t={}".format(t))
    print(header)

    for blk in block_vals:
        row = "  {:>7d} MiB".format(blk)
        for t in thread_vals:
            match = next((r for r in results if r["block_mib"] == blk and r["threads"] == t), None)
            if match:
                marker = "*" if (match["block_mib"] == recommendation["best_block_mib"]
                                  and match["threads"] == recommendation["best_threads"]) else " "
                row += "  {:6.0f}{}".format(match["throughput_mbs"], marker)
            else:
                row += "  {:>7}".format("—")
        print(row)

    print()
    print("  * = best observed combination")
    print()

    rec = recommendation
    print("  Diagnosis: {}".format(rec["bound"].upper()))
    print()
    # Word-wrap the diagnosis
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
    print("    # Expected throughput: ~{:.0f} MB/s".format(rec["best_throughput_mbs"]))
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
                   help="Minimum seconds per measurement (default: 2.0)")
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
        # Write test file.
        chunk = os.urandom(min(args.size_mib, 16) * 1024 * 1024)
        written = 0
        target = args.size_mib * 1024 * 1024
        while written < target:
            n = min(len(chunk), target - written)
            os.write(tmp_fd, chunk[:n])
            written += n
        os.close(tmp_fd)

        results = []
        for block_mib in pipeline_blocks:
            for threads in thread_counts:
                mbs = _measure_pipeline(
                    tmp_path, args.size_mib, args.algo,
                    block_mib, threads,
                    args.min_duration, args.verbose,
                )
                results.append({
                    "block_mib": block_mib,
                    "threads": threads,
                    "throughput_mbs": mbs,
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
            "ceiling_mbs": ceiling,
            "results": results,
            "recommendation": recommendation,
        }
        print(json.dumps(output, indent=2))
    else:
        _print_table(
            sys_info, ceiling, results, recommendation,
            args.size_mib, args.algo, tmp_dir,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
