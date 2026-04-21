"""
Tests for cephsumfs.algorithms.

All tests use known vectors so they catch both logic errors and accidental
changes to algorithm selection (e.g. registering the wrong factory).
"""

import struct
import unittest

from cephsumfs.algorithms import (
    Adler32,
    CRC32,
    CRC32C,
    HashlibAlgorithm,
    get_algorithm,
    register_algorithm,
    ALGORITHM_REGISTRY,
    ChecksumAlgorithm,
)


class TestAdler32(unittest.TestCase):
    def test_empty(self):
        a = Adler32()
        # adler32("") == 1
        self.assertEqual(a.digest(), struct.pack(">I", 1))

    def test_known_vector(self):
        # Wikipedia: adler32("Wikipedia") == 0x11E60398
        a = Adler32()
        a.update(b"Wikipedia")
        self.assertEqual(a.digest(), struct.pack(">I", 0x11E60398))

    def test_incremental_matches_single(self):
        a1 = Adler32()
        a1.update(b"hello world")

        a2 = Adler32()
        a2.update(b"hello ")
        a2.update(b"world")

        self.assertEqual(a1.digest(), a2.digest())

    def test_hexdigest_length(self):
        a = Adler32()
        a.update(b"test")
        self.assertEqual(len(a.hexdigest()), 8)

    def test_name(self):
        self.assertEqual(Adler32.name, "adler32")


class TestCRC32(unittest.TestCase):
    def test_empty(self):
        c = CRC32()
        # crc32("") == 0
        self.assertEqual(c.digest(), struct.pack(">I", 0))

    def test_known_vector(self):
        import zlib
        c = CRC32()
        data = b"123456789"
        c.update(data)
        expected = zlib.crc32(data) & 0xFFFFFFFF
        self.assertEqual(c.digest(), struct.pack(">I", expected))

    def test_incremental(self):
        import zlib
        data = b"hello world"
        c1 = CRC32()
        c1.update(data)

        c2 = CRC32()
        c2.update(b"hello ")
        c2.update(b"world")

        self.assertEqual(c1.digest(), c2.digest())

    def test_name(self):
        self.assertEqual(CRC32.name, "crc32")


class TestCRC32C(unittest.TestCase):
    # CRC-32C of b"123456789" == 0xE3069283  (standard test vector)
    VECTOR_DATA = b"123456789"
    VECTOR_EXPECTED = 0xE3069283

    def test_known_vector(self):
        c = CRC32C()
        c.update(self.VECTOR_DATA)
        self.assertEqual(c.digest(), struct.pack(">I", self.VECTOR_EXPECTED))

    def test_empty(self):
        c = CRC32C()
        # crc32c("") == 0
        self.assertEqual(c.digest(), struct.pack(">I", 0))

    def test_incremental(self):
        c1 = CRC32C()
        c1.update(self.VECTOR_DATA)

        c2 = CRC32C()
        for byte in self.VECTOR_DATA:
            c2.update(bytes([byte]))

        self.assertEqual(c1.digest(), c2.digest())

    def test_name(self):
        self.assertEqual(CRC32C.name, "crc32c")


class TestHashlibAlgorithm(unittest.TestCase):
    def test_md5_known_vector(self):
        import hashlib
        h = HashlibAlgorithm("md5")
        h.update(b"")
        self.assertEqual(h.digest(), hashlib.md5(b"").digest())

    def test_sha256_known_vector(self):
        import hashlib
        h = HashlibAlgorithm("sha256")
        h.update(b"abc")
        self.assertEqual(h.digest(), hashlib.sha256(b"abc").digest())

    def test_name_normalised(self):
        h = HashlibAlgorithm("MD5")
        self.assertEqual(h.name, "md5")

    def test_hexdigest_length_md5(self):
        h = HashlibAlgorithm("md5")
        h.update(b"data")
        self.assertEqual(len(h.hexdigest()), 32)

    def test_hexdigest_length_sha256(self):
        h = HashlibAlgorithm("sha256")
        h.update(b"data")
        self.assertEqual(len(h.hexdigest()), 64)


class TestRegistry(unittest.TestCase):
    def test_all_expected_algorithms_present(self):
        for name in ("adler32", "crc32", "crc32c", "md5", "sha256"):
            self.assertIn(name, ALGORITHM_REGISTRY)

    def test_get_algorithm_returns_fresh_instance(self):
        a1 = get_algorithm("adler32")
        a2 = get_algorithm("adler32")
        self.assertIsNot(a1, a2)

    def test_get_algorithm_unknown_raises(self):
        with self.assertRaises(ValueError) as ctx:
            get_algorithm("notanalgo")
        self.assertIn("notanalgo", str(ctx.exception))
        self.assertIn("adler32", str(ctx.exception))

    def test_get_algorithm_case_insensitive(self):
        a = get_algorithm("ADLER32")
        self.assertIsInstance(a, Adler32)

    def test_register_custom_algorithm(self):
        class Noop(ChecksumAlgorithm):
            name = "noop"
            def update(self, data):
                pass
            def digest(self):
                return b"\x00"

        register_algorithm(Noop)
        algo = get_algorithm("noop")
        self.assertIsInstance(algo, Noop)
        # Clean up so other tests are not affected.
        del ALGORITHM_REGISTRY["noop"]

    def test_register_non_subclass_raises(self):
        with self.assertRaises(TypeError):
            register_algorithm(object)

    def test_register_unnamed_raises(self):
        class Unnamed(ChecksumAlgorithm):
            name = ""
            def update(self, data): pass
            def digest(self): return b""

        with self.assertRaises(ValueError):
            register_algorithm(Unnamed)
