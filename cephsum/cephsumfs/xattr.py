"""
xattr.py — XRootD XrdCks checksum extended-attribute storage.

Wire format
-----------
XRootD stores checksum metadata as a 96-byte binary blob under the xattr key
``<namespace>.XrdCks.<algo>`` (default namespace: ``user``).

Blob layout (big-endian, 96 bytes total):

  Offset  Size  Type    Field
  ------  ----  ------  --------------------------------------------------
       0    16  char[]  algo name, NUL-padded (e.g. b"adler32\x00...")
      16     8  uint64  fmTime  — file mtime (seconds since Unix epoch) at
                                 the moment the checksum was computed
      24     4  uint32  csTime  — elapsed seconds from fmTime to end of
                                 computation (cs_delta); 0 when not measured
      28     2  uint16  reserved
      30     1  uint8   reserved
      31     1  uint8   length  — number of meaningful bytes in value[]
      32    64  char[]  value   — raw digest bytes, zero-padded to 64 bytes

Cache invalidation
------------------
A stored checksum is only valid when the file's current mtime matches the
recorded fmTime.  Always call XrdCksRecord.is_current(path) before trusting
a cached value.  This check was absent in earlier implementations and could
cause stale checksums to be silently returned after a file was modified.
"""

import logging
import os
import struct
from typing import NamedTuple, Optional

log = logging.getLogger(__name__)

_BLOB_FMT = "!16sQIHBB64s"
_BLOB_SIZE = struct.calcsize(_BLOB_FMT)  # 96

_NS_PER_S = 1_000_000_000


def _mtime_s(st: os.stat_result) -> int:
    """Return file mtime as integer seconds using nanosecond precision."""
    return st.st_mtime_ns // _NS_PER_S


class XrdCksRecord(NamedTuple):
    """
    Parsed representation of an XrdCks xattr blob.

    Attributes
    ----------
    algo:
        Lowercase algorithm name (e.g. ``"adler32"``).  Must be lowercase;
        use the module-level factory functions rather than constructing
        directly to ensure normalisation.
    file_mtime:
        Unix timestamp of the file's mtime (integer seconds) when the
        checksum was computed.
    cs_delta:
        Seconds elapsed between file_mtime and the end of checksum
        computation.  Zero when not measured.
    digest:
        Raw digest bytes (length determined by the algorithm).
    """

    algo: str
    file_mtime: int
    cs_delta: int
    digest: bytes

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def is_current(self, path: str, follow_symlinks: bool = True) -> bool:
        """
        Return True if the file's current mtime matches the recorded fmTime.

        A False result means the file has been modified since the checksum was
        stored; the cached value must not be used.
        """
        st = os.stat(path, follow_symlinks=follow_symlinks)
        current_mtime = _mtime_s(st)
        if current_mtime != self.file_mtime:
            log.debug(
                "mtime mismatch for %r: stored=%d current=%d",
                path, self.file_mtime, current_mtime,
            )
            return False
        return True

    # ------------------------------------------------------------------
    # Digest helpers
    # ------------------------------------------------------------------

    def digest_hex(self) -> str:
        """
        Return the digest as a zero-padded lowercase hex string.

        The string length is always ``2 * len(self.digest)`` so that a 4-byte
        adler32 digest always yields exactly 8 characters.
        """
        return self.digest.hex()

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_blob(self) -> bytes:
        """Serialise to the 96-byte XrdCks wire format."""
        algo_bytes = self.algo.encode("ascii")
        if len(algo_bytes) >= 16:
            raise ValueError(
                "algo name must be fewer than 16 ASCII bytes; got {!r}".format(self.algo)
            )
        name_field = algo_bytes + b"\x00" * (16 - len(algo_bytes))

        if not (0 <= self.file_mtime <= 0xFFFFFFFFFFFFFFFF):
            raise ValueError("file_mtime out of uint64 range")
        if not (0 <= self.cs_delta <= 0xFFFFFFFF):
            raise ValueError("cs_delta out of uint32 range")

        if len(self.digest) > 64:
            raise ValueError(
                "digest too long: {} bytes (max 64)".format(len(self.digest))
            )
        value_field = self.digest + b"\x00" * (64 - len(self.digest))

        return struct.pack(
            _BLOB_FMT,
            name_field,
            self.file_mtime,
            self.cs_delta,
            0,  # reserved
            0,  # reserved
            len(self.digest),
            value_field,
        )

    @staticmethod
    def from_blob(blob: bytes) -> "XrdCksRecord":
        """Deserialise from the 96-byte XrdCks wire format."""
        if len(blob) != _BLOB_SIZE:
            raise ValueError(
                "unexpected blob size: {} bytes (expected {})".format(
                    len(blob), _BLOB_SIZE
                )
            )
        name_f, fm_time, cs_time, _r1, _r2, length, value_f = struct.unpack(
            _BLOB_FMT, blob
        )
        algo = name_f.split(b"\x00", 1)[0].decode("ascii").lower()
        if length > 64:
            raise ValueError(
                "invalid digest length {} in blob (max 64)".format(length)
            )
        return XrdCksRecord(
            algo=algo,
            file_mtime=int(fm_time),
            cs_delta=int(cs_time),
            digest=bytes(value_f[:length]),
        )

    def __repr__(self) -> str:
        return (
            "XrdCksRecord(algo={!r}, file_mtime={}, cs_delta={}, digest={})".format(
                self.algo, self.file_mtime, self.cs_delta, self.digest_hex()
            )
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, XrdCksRecord):
            return NotImplemented
        return tuple.__eq__(self, other)


# ---------------------------------------------------------------------------
# xattr key helpers
# ---------------------------------------------------------------------------

def _attr_key(algo: str, namespace: str = "user") -> str:
    return "{}.XrdCks.{}".format(namespace, algo.lower())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_xattr(
    path: str,
    algo: str,
    namespace: str = "user",
    follow_symlinks: bool = True,
) -> XrdCksRecord:
    """
    Read and decode the XrdCks blob for *algo* from *path*.

    Raises
    ------
    OSError
        If the attribute is absent or unreadable (e.g. ENODATA, EACCES).
    ValueError
        If the blob is malformed or the stored algo name does not match.
    """
    key = _attr_key(algo, namespace)
    blob = os.getxattr(path, key, follow_symlinks=follow_symlinks)
    rec = XrdCksRecord.from_blob(blob)
    if rec.algo != algo.lower():
        raise ValueError(
            "algo mismatch: requested={!r} stored={!r}".format(algo, rec.algo)
        )
    return rec


def write_xattr(
    path: str,
    algo: str,
    digest: bytes,
    namespace: str = "user",
    file_mtime: Optional[int] = None,
    cs_delta: Optional[int] = None,
    follow_symlinks: bool = True,
) -> XrdCksRecord:
    """
    Write an XrdCks blob for *algo* to *path*.

    Parameters
    ----------
    path:
        Target file path.
    algo:
        Algorithm name (e.g. ``"adler32"``).
    digest:
        Raw digest bytes from ChecksumAlgorithm.digest().
    namespace:
        xattr namespace prefix (default ``"user"``).
    file_mtime:
        File mtime (integer seconds) to record.  If None, read from stat().
        Note: the stat is taken at write time, not at computation time.  If
        the file is modified between the end of checksum computation and this
        call, the stored mtime will reflect the new modification, causing the
        cached entry to be invalid on the next read.  This window is
        inherently racy without external file locking.
    cs_delta:
        Seconds elapsed during computation.  Defaults to 0.
    follow_symlinks:
        Whether to follow symlinks when stat-ing / setting the xattr.

    Returns
    -------
    XrdCksRecord
        The record as written.
    """
    if file_mtime is None:
        st = os.stat(path, follow_symlinks=follow_symlinks)
        file_mtime = _mtime_s(st)

    rec = XrdCksRecord(
        algo=algo.lower(),
        file_mtime=int(file_mtime),
        cs_delta=int(cs_delta) if cs_delta is not None else 0,
        digest=bytes(digest),
    )
    key = _attr_key(algo, namespace)
    os.setxattr(path, key, rec.to_blob(), follow_symlinks=follow_symlinks)
    log.debug("wrote xattr %s on %r: %s", key, path, rec.digest_hex())
    return rec


def delete_xattr(
    path: str,
    algo: str,
    namespace: str = "user",
    follow_symlinks: bool = True,
) -> None:
    """
    Remove the XrdCks xattr for *algo* from *path*.

    Raises OSError if the attribute does not exist or cannot be removed.
    """
    key = _attr_key(algo, namespace)
    os.removexattr(path, key, follow_symlinks=follow_symlinks)
    log.debug("removed xattr %s from %r", key, path)
