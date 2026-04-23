"""
Tests for cephsumfs.reader.

Uses real temporary files so that os.pread() is exercised genuinely.
All tests are pure filesystem I/O — no xattrs, no XRootD.
"""

import hashlib
import os
import struct
import tempfile
import unittest
import zlib
from unittest import mock

from cephsumfs.algorithms import Adler32, get_algorithm
from cephsumfs.reader import ALLOWED_BLOCK_MIB, compute_checksum


def _write_tmp(data: bytes) -> str:
    """Write data to a temp file and return its path."""
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(data)
    f.close()
    return f.name


class TestComputeChecksumAdler32(unittest.TestCase):
    """Verify that compute_checksum produces the same result as a simple
    sequential adler32 for a variety of file sizes."""

    def _check(self, data: bytes, **kwargs):
        path = _write_tmp(data)
        try:
            algo = Adler32()
            digest = compute_checksum(path, algo, **kwargs)
            expected = zlib.adler32(data) & 0xFFFFFFFF
            self.assertEqual(digest, struct.pack(">I", expected))
        finally:
            os.unlink(path)

    def test_empty_file(self):
        self._check(b"")

    def test_small_file(self):
        self._check(b"hello world")

    def test_exactly_one_block(self):
        block = 1 * 1024 * 1024
        self._check(os.urandom(block), block_mib=1, threads=1)

    def test_multiple_blocks(self):
        # 3.5 MiB with 1 MiB blocks → 4 blocks (last partial)
        self._check(os.urandom(int(3.5 * 1024 * 1024)), block_mib=1, threads=2)

    def test_multi_thread(self):
        self._check(os.urandom(4 * 1024 * 1024), block_mib=1, threads=4)

    def test_single_thread(self):
        self._check(os.urandom(2 * 1024 * 1024), block_mib=1, threads=1)


class TestComputeChecksumMD5(unittest.TestCase):
    def test_known_data(self):
        data = b"The quick brown fox jumps over the lazy dog"
        path = _write_tmp(data)
        try:
            algo = get_algorithm("md5")
            digest = compute_checksum(path, algo, block_mib=1, threads=2)
            self.assertEqual(digest, hashlib.md5(data).digest())
        finally:
            os.unlink(path)


class TestComputeChecksumSHA256(unittest.TestCase):
    def test_known_data(self):
        data = b"cephsumfs sha256 test"
        path = _write_tmp(data)
        try:
            algo = get_algorithm("sha256")
            digest = compute_checksum(path, algo, block_mib=1, threads=1)
            self.assertEqual(digest, hashlib.sha256(data).digest())
        finally:
            os.unlink(path)


class TestComputeChecksumCRC32C(unittest.TestCase):
    def test_known_vector(self):
        # crc32c("123456789") == 0xE3069283
        data = b"123456789"
        path = _write_tmp(data)
        try:
            algo = get_algorithm("crc32c")
            digest = compute_checksum(path, algo, block_mib=1, threads=1)
            self.assertEqual(digest, struct.pack(">I", 0xE3069283))
        finally:
            os.unlink(path)


class TestValidation(unittest.TestCase):
    def test_invalid_block_mib(self):
        path = _write_tmp(b"x")
        try:
            with self.assertRaises(ValueError):
                compute_checksum(path, Adler32(), block_mib=3)
        finally:
            os.unlink(path)

    def test_zero_threads(self):
        path = _write_tmp(b"x")
        try:
            with self.assertRaises(ValueError):
                compute_checksum(path, Adler32(), threads=0)
        finally:
            os.unlink(path)

    def test_missing_file(self):
        with self.assertRaises(OSError):
            compute_checksum("/nonexistent/path/file.dat", Adler32())

    def test_short_read_raises_oserror(self):
        # Simulate a file that is truncated between fstat and pread by
        # patching os.pread to return fewer bytes than requested on the
        # second block.  The reader must raise OSError rather than silently
        # computing a checksum over truncated data.
        data = os.urandom(2 * 1024 * 1024)
        path = _write_tmp(data)
        try:
            real_pread = os.pread
            def _truncating_pread(fd, size, offset):
                result = real_pread(fd, size, offset)
                return result[: len(result) // 2] if offset > 0 else result
            with mock.patch("os.pread", side_effect=_truncating_pread):
                with self.assertRaises(OSError):
                    compute_checksum(path, Adler32(), block_mib=1, threads=1)
        finally:
            os.unlink(path)

    def test_allowed_block_sizes_all_work(self):
        data = os.urandom(1024)
        path = _write_tmp(data)
        try:
            for mib in ALLOWED_BLOCK_MIB:
                algo = Adler32()
                digest = compute_checksum(path, algo, block_mib=mib, threads=1)
                expected = struct.pack(">I", zlib.adler32(data) & 0xFFFFFFFF)
                self.assertEqual(digest, expected, "failed for block_mib={}".format(mib))
        finally:
            os.unlink(path)
