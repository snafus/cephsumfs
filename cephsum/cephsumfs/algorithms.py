"""
algorithms.py — checksum algorithm registry and implementations.

All algorithms share a common ABC so the reader pipeline can work with any of
them without knowing the underlying implementation.

Supported algorithms
--------------------
adler32   zlib.adler32 — standard XRootD default
crc32     zlib.crc32   — ISO 3309 / ITU-T V.42
crc32c    Castagnoli CRC-32C — preferred for storage (hardware-accelerated on
          x86 via SSE4.2).  Loaded via ctypes from libcrc32c if available on
          the system; falls back to a pure-Python table implementation that is
          correct but significantly slower (~10× at large block sizes).
          Install libcrc32c (RHEL: crc32c-devel or libcrc32c) to get the fast
          path.  A one-time WARNING is emitted when the fallback is active.
md5       hashlib.md5
sha256    hashlib.sha256

Adding new algorithms
---------------------
Subclass ChecksumAlgorithm, implement update/digest, then call
register_algorithm(MyAlgo) or add the factory to ALGORITHM_REGISTRY.
"""

import abc
import ctypes
import ctypes.util
import hashlib
import logging
import struct
import zlib
from typing import Callable, Dict, Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class ChecksumAlgorithm(abc.ABC):
    """
    Stateful checksum accumulator.

    Usage::

        algo = get_algorithm("adler32")
        algo.update(chunk1)
        algo.update(chunk2)
        raw: bytes = algo.digest()
        hex_str: str = algo.hexdigest()
    """

    #: Lowercase algorithm name as used in xattr keys and CLI --algo argument.
    name: str = ""

    @abc.abstractmethod
    def update(self, data: bytes) -> None:
        """Feed the next chunk of data into the accumulator."""

    @abc.abstractmethod
    def digest(self) -> bytes:
        """Return the raw digest bytes.  Must not alter accumulator state."""

    def hexdigest(self) -> str:
        """Return the digest as a lowercase hex string."""
        return self.digest().hex()


# ---------------------------------------------------------------------------
# Concrete implementations
# ---------------------------------------------------------------------------

class Adler32(ChecksumAlgorithm):
    """Adler-32 via zlib.  Digest is 4 bytes big-endian."""

    name = "adler32"

    def __init__(self) -> None:
        self._value = zlib.adler32(b"") & 0xFFFFFFFF

    def update(self, data: bytes) -> None:
        self._value = zlib.adler32(data, self._value) & 0xFFFFFFFF

    def digest(self) -> bytes:
        return struct.pack(">I", self._value)


class CRC32(ChecksumAlgorithm):
    """CRC-32 (ISO 3309) via zlib.  Digest is 4 bytes big-endian."""

    name = "crc32"

    def __init__(self) -> None:
        self._value = 0

    def update(self, data: bytes) -> None:
        self._value = zlib.crc32(data, self._value) & 0xFFFFFFFF

    def digest(self) -> bytes:
        return struct.pack(">I", self._value)


# ---------------------------------------------------------------------------
# CRC-32C — try ctypes first, fall back to pure Python
# ---------------------------------------------------------------------------

def _build_crc32c_table() -> "list":
    """Pre-compute the 256-entry CRC-32C (Castagnoli) lookup table."""
    poly = 0x82F63B78  # reflected Castagnoli polynomial
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        table.append(crc)
    return table


_CRC32C_TABLE = _build_crc32c_table()


def _crc32c_pure(data: bytes, value: int = 0) -> int:
    crc = value ^ 0xFFFFFFFF
    for byte in data:
        crc = (crc >> 8) ^ _CRC32C_TABLE[(crc ^ byte) & 0xFF]
    return crc ^ 0xFFFFFFFF


_CRC32C_KNOWN_INPUT = b"123456789"
_CRC32C_KNOWN_VALUE = 0xE3069283


def _try_load_libcrc32c() -> "Optional[Callable[[bytes, int], int]]":
    """
    Attempt to load libcrc32c from the system and return a callable with the
    same signature as _crc32c_pure.  Returns None if unavailable or if the
    loaded function produces an incorrect result for the standard test vector.

    The wrapper assumes the C function uses the Linux kernel convention:
    the crc argument is the pre-final-XOR running state (pass 0xFFFFFFFF for
    a fresh computation; the function returns the running state, not the
    finalised value).  The self-test guards against libraries that export a
    "crc32c" symbol but use a different calling convention.
    """
    lib_name = ctypes.util.find_library("crc32c")
    if lib_name is None:
        # Common RHEL path not in ldconfig search path
        for candidate in ("libcrc32c.so.1", "libcrc32c.so"):
            try:
                lib = ctypes.CDLL(candidate)
                break
            except OSError:
                pass
        else:
            return None
    else:
        try:
            lib = ctypes.CDLL(lib_name)
        except OSError:
            return None

    try:
        fn = lib.crc32c
        fn.restype = ctypes.c_uint32
        fn.argtypes = [ctypes.c_uint32, ctypes.c_char_p, ctypes.c_size_t]
    except AttributeError:
        return None

    def _crc32c_ctypes(data: bytes, value: int = 0) -> int:
        return fn(value ^ 0xFFFFFFFF, data, len(data)) ^ 0xFFFFFFFF

    # Validate against the standard CRC-32C test vector before committing.
    probe = _crc32c_ctypes(_CRC32C_KNOWN_INPUT) & 0xFFFFFFFF
    if probe != _CRC32C_KNOWN_VALUE:
        log.warning(
            "libcrc32c loaded but failed self-test (got 0x%08x, expected 0x%08x); "
            "falling back to pure-Python CRC-32C.",
            probe, _CRC32C_KNOWN_VALUE,
        )
        return None

    return _crc32c_ctypes


_crc32c_impl: Optional[Callable] = None
_crc32c_warned = False


def _get_crc32c_impl() -> Callable:
    global _crc32c_impl, _crc32c_warned
    if _crc32c_impl is None:
        fast = _try_load_libcrc32c()
        if fast is not None:
            _crc32c_impl = fast
        else:
            if not _crc32c_warned:
                log.warning(
                    "libcrc32c not found; using pure-Python CRC-32C (slow). "
                    "Install libcrc32c for hardware-accelerated performance."
                )
                _crc32c_warned = True
            _crc32c_impl = _crc32c_pure
    return _crc32c_impl


class CRC32C(ChecksumAlgorithm):
    """
    CRC-32C (Castagnoli) — hardware-accelerated via libcrc32c when available,
    otherwise pure-Python table lookup.  Digest is 4 bytes big-endian.
    """

    name = "crc32c"

    def __init__(self) -> None:
        self._value = 0
        self._impl = _get_crc32c_impl()

    def update(self, data: bytes) -> None:
        self._value = self._impl(data, self._value) & 0xFFFFFFFF

    def digest(self) -> bytes:
        return struct.pack(">I", self._value)


class HashlibAlgorithm(ChecksumAlgorithm):
    """
    Generic wrapper around any hashlib algorithm (md5, sha256, sha1, …).
    Digest length is determined by the underlying hash function.
    """

    def __init__(self, algo_name: str) -> None:
        self.name = algo_name.lower()
        self._h = hashlib.new(algo_name)

    def update(self, data: bytes) -> None:
        self._h.update(data)

    def digest(self) -> bytes:
        return self._h.digest()


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

#: Maps lowercase algorithm names to zero-argument factory callables.
ALGORITHM_REGISTRY: Dict[str, Callable[[], ChecksumAlgorithm]] = {
    "adler32": Adler32,
    "crc32":   CRC32,
    "crc32c":  CRC32C,
    "md5":     lambda: HashlibAlgorithm("md5"),
    "sha256":  lambda: HashlibAlgorithm("sha256"),
}


def register_algorithm(algo_class: "type") -> None:
    """Register a ChecksumAlgorithm subclass by its .name attribute."""
    if not issubclass(algo_class, ChecksumAlgorithm):
        raise TypeError("algo_class must be a subclass of ChecksumAlgorithm")
    if not algo_class.name:
        raise ValueError("algo_class.name must be a non-empty string")
    ALGORITHM_REGISTRY[algo_class.name.lower()] = algo_class


def get_algorithm(name: str) -> ChecksumAlgorithm:
    """
    Return a fresh ChecksumAlgorithm instance for *name*.

    Raises ValueError listing supported names if *name* is unknown.
    """
    key = name.lower()
    factory = ALGORITHM_REGISTRY.get(key)
    if factory is None:
        supported = ", ".join(sorted(ALGORITHM_REGISTRY))
        raise ValueError(
            "Unknown algorithm {!r}. Supported: {}".format(name, supported)
        )
    return factory()
