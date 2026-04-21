"""
cephsumfs — external checksum helper for XRootD on Ceph-backed POSIX storage.

Computes and caches file checksums via Linux extended attributes using the
XRootD XrdCks wire format.  Supports adler32, crc32, crc32c, md5, sha256.
"""

__version__ = "0.1.0"
