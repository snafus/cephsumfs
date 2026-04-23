Name:           cephsumfs
Version:        0.2.1
Release:        1%{?dist}
Summary:        External checksum helper for XRootD on Ceph-backed POSIX storage

License:        Apache-2.0
Source0:        %{name}-%{version}.tar.gz

BuildArch:      noarch
BuildRequires:  python3-devel
BuildRequires:  python3-setuptools
BuildRequires:  python3-pytest

Requires:       python3 >= 3.6.8
# libcrc32c provides hardware-accelerated CRC-32C via SSE4.2.
# Without it cephsumfs falls back to a pure-Python implementation that is
# correct but ~10x slower.  Install it for production deployments.
Recommends:     libcrc32c

%description
cephsumfs computes and caches file checksums for XRootD using Linux extended
attributes in the XrdCks wire format.  It supports adler32, crc32, crc32c,
md5, and sha256, and is designed to be called by XRootD as an external
checksum helper via the "xrd.chksum" directive.

The tool reads cached checksums from xattr when available and validates them
against the file's current mtime before trusting them.  Stale or absent
entries are recomputed and written back automatically.


%prep
%autosetup -n %{name}-%{version}


%build
%py3_build


%install
%py3_install

install -Dm 0755 scripts/run_checksum.sh \
    %{buildroot}%{_libexecdir}/cephsumfs/run_checksum.sh


%check
# Run the unit test suite during package build.
# xattr tests are automatically skipped if the build filesystem does not
# support user xattrs (common in mock/koji chroots).
%{python3} -m pytest tests/ -v


%files
%license LICENSE
%doc README.md
%{python3_sitelib}/cephsumfs/
%{python3_sitelib}/cephsumfs-*.egg-info/
%{_bindir}/cephsumfs
%{_libexecdir}/cephsumfs/run_checksum.sh


%changelog
* Thu Apr 23 2026 Package Maintainer <maintainer@example.com> - 0.2.1-1
- Fix run_checksum.sh: call installed cephsumfs binary instead of python3 -m cephsumfs
- Remove unused CEPHSUM_PYTHON variable from run_checksum.sh

* Wed Apr 23 2026 Package Maintainer <maintainer@example.com> - 0.2.0-1
- O_NOATIME fallback for files not owned by the calling process
- Short-read detection: log WARNING and abort with EIO on truncated pread
- cs_delta now uses monotonic clock to avoid NTP skew
- Flag conflict validation: --override/--compute-only/--dry-run rejected with --verify/--remove
- Logging idempotency fix: NullHandler no longer blocks subsequent real handlers
- Expanded README: caching behaviour, exit codes, known limitations, Ubuntu install

* Tue Apr 21 2026 Package Maintainer <maintainer@example.com> - 0.1.0-1
- Initial release
