"""
Tests for cephsumfs.cli.

The CLI is tested by calling main() directly with a constructed argv list,
capturing stdout, and inspecting the exit code.  Filesystem xattr tests are
skipped when the kernel/filesystem does not support user xattrs.
"""

import io
import os
import struct
import sys
import tempfile
import unittest
import zlib
from unittest import mock

from cephsumfs.cli import main
from cephsumfs.xattr import write_xattr, read_xattr, XrdCksRecord


def _write_tmp(data: bytes) -> str:
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(data)
    f.close()
    return f.name


def _xattr_supported(path: str) -> bool:
    try:
        os.setxattr(path, "user.cephsumfs_test", b"1")
        os.removexattr(path, "user.cephsumfs_test")
        return True
    except (OSError, AttributeError):
        return False


def _run(argv, *, capture_stdout=True):
    """
    Run main(argv) and return (exit_code, stdout_text).
    Raises SystemExit internally; we catch and return the code.
    """
    buf = io.StringIO()
    exit_code = 0
    with mock.patch("sys.stdout", buf):
        try:
            main(argv)
        except SystemExit as exc:
            exit_code = exc.code if isinstance(exc.code, int) else 1
    return exit_code, buf.getvalue()


class TestComputeOnly(unittest.TestCase):
    def test_correct_checksum(self):
        data = b"hello cephsumfs"
        path = _write_tmp(data)
        try:
            code, out = _run([path, "--compute-only", "--block-mib", "1", "--threads", "1"])
            self.assertEqual(code, 0)
            expected = "{:08x}\n".format(zlib.adler32(data) & 0xFFFFFFFF)
            self.assertEqual(out, expected)
        finally:
            os.unlink(path)

    def test_missing_file_exits_nonzero(self):
        code, out = _run(["/nonexistent/path.dat", "--compute-only"])
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")

    def test_output_is_exactly_hex_newline(self):
        path = _write_tmp(b"abc")
        try:
            code, out = _run([path, "--compute-only", "--block-mib", "1"])
            self.assertEqual(code, 0)
            self.assertRegex(out, r"^[0-9a-f]{8}\n$")
        finally:
            os.unlink(path)

    def test_md5_output_length(self):
        path = _write_tmp(b"test")
        try:
            code, out = _run([path, "--algo", "md5", "--compute-only", "--block-mib", "1"])
            self.assertEqual(code, 0)
            # md5 digest = 16 bytes = 32 hex chars
            self.assertRegex(out, r"^[0-9a-f]{32}\n$")
        finally:
            os.unlink(path)

    def test_sha256_output_length(self):
        path = _write_tmp(b"test")
        try:
            code, out = _run([path, "--algo", "sha256", "--compute-only", "--block-mib", "1"])
            self.assertEqual(code, 0)
            self.assertRegex(out, r"^[0-9a-f]{64}\n$")
        finally:
            os.unlink(path)


@unittest.skipUnless(hasattr(os, "setxattr"), "requires Linux xattr support")
class TestDefaultMode(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(delete=False)
        self._tmp.write(b"cephsumfs default mode test")
        self._tmp.flush()
        self._path = self._tmp.name
        if not _xattr_supported(self._path):
            self.skipTest("filesystem does not support user xattrs")

    def tearDown(self):
        os.unlink(self._path)

    def _argv(self, *extra):
        return [self._path, "--block-mib", "1", "--threads", "1"] + list(extra)

    def test_computes_and_caches(self):
        code, out = _run(self._argv())
        self.assertEqual(code, 0)
        # xattr should now be present
        rec = read_xattr(self._path, "adler32")
        self.assertEqual(rec.digest_hex(), out.strip())

    def test_returns_cached_on_second_call(self):
        code1, out1 = _run(self._argv())
        code2, out2 = _run(self._argv())
        self.assertEqual(code1, 0)
        self.assertEqual(code2, 0)
        self.assertEqual(out1, out2)

    def test_recomputes_after_mtime_change(self):
        code1, out1 = _run(self._argv())
        self.assertEqual(code1, 0)

        # Modify file content and advance mtime by at least one second so that
        # int(mtime) changes even if the write completes within the same clock tick.
        new_content = b"modified content"
        with open(self._path, "wb") as f:
            f.write(new_content)
        old_mtime = int(os.stat(self._path).st_mtime)
        os.utime(self._path, (old_mtime + 2, old_mtime + 2))

        code2, out2 = _run(self._argv())
        self.assertEqual(code2, 0)
        # Checksum must reflect new content.
        expected = "{:08x}\n".format(zlib.adler32(new_content) & 0xFFFFFFFF)
        self.assertEqual(out2, expected)


@unittest.skipUnless(hasattr(os, "setxattr"), "requires Linux xattr support")
class TestVerifyMode(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(delete=False)
        self._tmp.write(b"verify test data")
        self._tmp.flush()
        self._path = self._tmp.name
        if not _xattr_supported(self._path):
            self.skipTest("filesystem does not support user xattrs")

    def tearDown(self):
        os.unlink(self._path)

    def _argv(self, *extra):
        return [self._path, "--block-mib", "1", "--threads", "1"] + list(extra)

    def test_verify_passes_when_matching(self):
        _run(self._argv())  # populate cache
        code, out = _run(self._argv("--verify"))
        self.assertEqual(code, 0)
        self.assertRegex(out, r"^[0-9a-f]+\n$")

    def test_verify_fails_when_no_xattr(self):
        code, out = _run(self._argv("--verify"))
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")

    def test_verify_fails_on_mismatch(self):
        # Write a deliberately wrong digest.
        wrong_digest = b"\xff\xff\xff\xff"
        write_xattr(self._path, "adler32", wrong_digest)
        code, out = _run(self._argv("--verify"))
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")


@unittest.skipUnless(hasattr(os, "setxattr"), "requires Linux xattr support")
class TestRemoveMode(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(delete=False)
        self._tmp.write(b"remove test")
        self._tmp.flush()
        self._path = self._tmp.name
        if not _xattr_supported(self._path):
            self.skipTest("filesystem does not support user xattrs")

    def tearDown(self):
        os.unlink(self._path)

    def test_remove_prints_and_deletes(self):
        _run([self._path, "--block-mib", "1"])  # populate cache
        code, out = _run([self._path, "--remove"])
        self.assertEqual(code, 0)
        self.assertRegex(out, r"^[0-9a-f]+\n$")
        # xattr must be gone
        with self.assertRaises(OSError):
            read_xattr(self._path, "adler32")

    def test_remove_without_xattr_exits_nonzero(self):
        code, out = _run([self._path, "--remove"])
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")


@unittest.skipUnless(hasattr(os, "setxattr"), "requires Linux xattr support")
class TestOverrideMode(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(delete=False)
        self._tmp.write(b"override test")
        self._tmp.flush()
        self._path = self._tmp.name
        if not _xattr_supported(self._path):
            self.skipTest("filesystem does not support user xattrs")

    def tearDown(self):
        os.unlink(self._path)

    def test_override_replaces_wrong_cached_value(self):
        # Plant a deliberately wrong digest.
        write_xattr(self._path, "adler32", b"\xff\xff\xff\xff")
        code, out = _run([self._path, "--override", "--block-mib", "1"])
        self.assertEqual(code, 0)
        expected = "{:08x}\n".format(zlib.adler32(b"override test") & 0xFFFFFFFF)
        self.assertEqual(out, expected)
        rec = read_xattr(self._path, "adler32")
        self.assertEqual(rec.digest_hex(), out.strip())
