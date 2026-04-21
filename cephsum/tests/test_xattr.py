"""
Tests for cephsumfs.xattr.

Serialisation round-trips and mtime validation logic are tested in isolation
from the filesystem using the blob encode/decode methods directly.  Tests that
require actual xattr syscalls are skipped when the filesystem does not support
user xattrs (e.g. tmpfs on some kernels, or a non-Linux platform).
"""

import os
import struct
import tempfile
import unittest

from cephsumfs.xattr import XrdCksRecord, _attr_key, read_xattr, write_xattr, delete_xattr


class TestXrdCksRecordSerialisation(unittest.TestCase):
    def _make_record(self, algo="adler32", fm=1700000000, delta=3, digest=b"\xde\xad\xbe\xef"):
        return XrdCksRecord(algo=algo, file_mtime=fm, cs_delta=delta, digest=digest)

    def test_round_trip(self):
        rec = self._make_record()
        blob = rec.to_blob()
        self.assertEqual(len(blob), 96)
        rec2 = XrdCksRecord.from_blob(blob)
        self.assertEqual(rec, rec2)

    def test_algo_stored_lowercase(self):
        rec = XrdCksRecord(algo="ADLER32", file_mtime=0, cs_delta=0, digest=b"\x00\x00\x00\x01")
        rec2 = XrdCksRecord.from_blob(rec.to_blob())
        self.assertEqual(rec2.algo, "adler32")

    def test_digest_hex_padded(self):
        # 4-byte digest must produce exactly 8 hex chars.
        rec = self._make_record(digest=b"\x00\x00\x00\x01")
        self.assertEqual(rec.digest_hex(), "00000001")
        self.assertEqual(len(rec.digest_hex()), 8)

    def test_digest_hex_md5(self):
        digest = bytes(range(16))
        rec = self._make_record(algo="md5", digest=digest)
        self.assertEqual(len(rec.digest_hex()), 32)

    def test_blob_size(self):
        rec = self._make_record()
        self.assertEqual(len(rec.to_blob()), 96)

    def test_algo_too_long_raises(self):
        long_algo = "a" * 16
        rec = XrdCksRecord(algo=long_algo, file_mtime=0, cs_delta=0, digest=b"\x00")
        with self.assertRaises(ValueError):
            rec.to_blob()

    def test_digest_too_long_raises(self):
        rec = XrdCksRecord(algo="adler32", file_mtime=0, cs_delta=0, digest=b"\x00" * 65)
        with self.assertRaises(ValueError):
            rec.to_blob()

    def test_from_blob_wrong_size_raises(self):
        with self.assertRaises(ValueError):
            XrdCksRecord.from_blob(b"\x00" * 95)

    def test_from_blob_invalid_length_field_raises(self):
        rec = self._make_record()
        blob = bytearray(rec.to_blob())
        # length field is at byte offset 31; set it > 64
        blob[31] = 65
        with self.assertRaises(ValueError):
            XrdCksRecord.from_blob(bytes(blob))

    def test_equality(self):
        r1 = self._make_record()
        r2 = self._make_record()
        self.assertEqual(r1, r2)

    def test_inequality_digest(self):
        r1 = self._make_record(digest=b"\x00\x00\x00\x01")
        r2 = self._make_record(digest=b"\x00\x00\x00\x02")
        self.assertNotEqual(r1, r2)

    def test_repr(self):
        rec = self._make_record()
        r = repr(rec)
        self.assertIn("adler32", r)
        self.assertIn("deadbeef", r)


class TestMtimeValidation(unittest.TestCase):
    def test_is_current_true(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name
            f.write(b"data")
        try:
            mtime = os.stat(path).st_mtime_ns // 1_000_000_000
            rec = XrdCksRecord(algo="adler32", file_mtime=mtime, cs_delta=0,
                               digest=b"\x00\x00\x00\x01")
            self.assertTrue(rec.is_current(path))
        finally:
            os.unlink(path)

    def test_is_current_false_after_touch(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name
        try:
            old_mtime = int(os.stat(path).st_mtime)
            rec = XrdCksRecord(algo="adler32", file_mtime=old_mtime - 10,
                               cs_delta=0, digest=b"\x00\x00\x00\x01")
            self.assertFalse(rec.is_current(path))
        finally:
            os.unlink(path)


def _xattr_supported(path):
    """Return True if user xattrs work on the filesystem containing path."""
    try:
        os.setxattr(path, "user.cephsumfs_test", b"1")
        os.removexattr(path, "user.cephsumfs_test")
        return True
    except (OSError, AttributeError):
        return False


@unittest.skipUnless(hasattr(os, "setxattr"), "requires Linux xattr support")
class TestXattrIO(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(delete=False)
        self._tmp.write(b"hello cephsumfs")
        self._tmp.flush()
        self._path = self._tmp.name
        if not _xattr_supported(self._path):
            self.skipTest("filesystem does not support user xattrs")

    def tearDown(self):
        try:
            os.unlink(self._path)
        except OSError:
            pass

    def test_write_then_read(self):
        digest = b"\xde\xad\xbe\xef"
        write_xattr(self._path, "adler32", digest)
        rec = read_xattr(self._path, "adler32")
        self.assertEqual(rec.digest, digest)
        self.assertEqual(rec.algo, "adler32")

    def test_read_missing_raises_oserror(self):
        with self.assertRaises(OSError):
            read_xattr(self._path, "adler32")

    def test_delete_removes_xattr(self):
        write_xattr(self._path, "adler32", b"\x00\x00\x00\x01")
        delete_xattr(self._path, "adler32")
        with self.assertRaises(OSError):
            read_xattr(self._path, "adler32")

    def test_algo_mismatch_raises(self):
        # Plant an adler32 blob under the crc32 xattr key directly, so that
        # read_xattr("crc32") finds a blob whose embedded algo name disagrees
        # with what was requested — this must raise ValueError.
        digest = b"\xde\xad\xbe\xef"
        rec = XrdCksRecord(algo="adler32", file_mtime=0, cs_delta=0, digest=digest)
        os.setxattr(self._path, "user.XrdCks.crc32", rec.to_blob())
        with self.assertRaises(ValueError):
            read_xattr(self._path, "crc32")

    def test_write_records_file_mtime(self):
        digest = b"\xde\xad\xbe\xef"
        expected_mtime = int(os.stat(self._path).st_mtime)
        rec = write_xattr(self._path, "adler32", digest)
        self.assertEqual(rec.file_mtime, expected_mtime)

    def test_cs_delta_stored(self):
        digest = b"\x00\x01\x02\x03"
        rec = write_xattr(self._path, "adler32", digest, cs_delta=7)
        stored = read_xattr(self._path, "adler32")
        self.assertEqual(stored.cs_delta, 7)


class TestAttrKey(unittest.TestCase):
    def test_default_namespace(self):
        self.assertEqual(_attr_key("adler32"), "user.XrdCks.adler32")

    def test_custom_namespace(self):
        self.assertEqual(_attr_key("md5", namespace="trusted"), "trusted.XrdCks.md5")

    def test_algo_lowercased(self):
        self.assertEqual(_attr_key("ADLER32"), "user.XrdCks.adler32")
