#!/bin/sh
# Builds the syncfs .deb from a release binary, on the Debian suite it is
# for (the libc6 dependency is the build system's):
#
#   scripts/build-deb.sh target/release/syncfs bookworm OUTDIR
#
# Version: Cargo's, with SYNCFS_PRE (e.g. "pre") as a ~ pre-release suffix.
set -eu
bin=$1
suite=$2
out=${3:-.}
repo=$(cd "$(dirname "$0")/.." && pwd)
arch=$(dpkg --print-architecture)
version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$repo/Cargo.toml" | head -1)
[ -n "${SYNCFS_PRE:-}" ] && version="$version~$SYNCFS_PRE"
case $suite in
  bookworm) debver=12 ;;
  trixie) debver=13 ;;
  *) echo "unknown suite $suite" >&2; exit 1 ;;
esac
full="$version-1+deb$debver"
libc=$(dpkg-query -W -f '${Version}' libc6 | sed 's/-.*//')

root=$(mktemp -d)
chmod 0755 "$root"
trap 'rm -rf "$root"' EXIT
install -D -m 0755 "$bin" "$root/usr/bin/syncfs"
# mount(8) looks in /sbin, which is /usr/sbin on merged-/usr systems.
install -d "$root/usr/sbin"
ln -s ../bin/syncfs "$root/usr/sbin/mount.syncfs"
ln -s ../bin/syncfs "$root/usr/sbin/mount.fuse.syncfs"
install -D -m 0644 "$repo/packaging/syncfs.yaml" "$root/etc/syncfs/syncfs.yaml"
install -D -m 0644 "$repo/packaging/syncfs-daemon.service" "$root/usr/lib/systemd/system/syncfs-daemon.service"
install -D -m 0644 "$repo/packaging/syncfs@.service" "$root/usr/lib/systemd/system/syncfs@.service"
doc="$root/usr/share/doc/syncfs"
install -D -m 0644 "$repo/README.md" "$doc/README.md"
install -D -m 0644 "$repo/examples/syncfs.yaml" "$doc/examples/syncfs.yaml"
{
  echo "Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/"
  echo "Upstream-Name: syncfs"
  echo "Source: https://github.com/oliverclaude/syncfs"
  echo
  echo "Files: *"
  echo "Copyright: syncfs authors"
  echo "License: MIT or Apache-2.0"
  echo
  echo "License: MIT"
  sed 's/^/ /; s/^ $/ ./' "$repo/LICENSE-MIT"
  echo
  echo "License: Apache-2.0"
  echo " On Debian systems the full text is in /usr/share/common-licenses/Apache-2.0."
} > "$doc/copyright"

install -d "$root/DEBIAN"
echo /etc/syncfs/syncfs.yaml > "$root/DEBIAN/conffiles"
size=$(du -sk --exclude=DEBIAN "$root" | cut -f1)
cat > "$root/DEBIAN/control" <<CONTROL
Package: syncfs
Version: $full
Architecture: $arch
Maintainer: syncfs maintainers <syncfs@users.noreply.github.com>
Installed-Size: $size
Depends: libc6 (>= $libc), fuse3, procps
Section: utils
Priority: optional
Homepage: https://github.com/oliverclaude/syncfs
Description: replicating FUSE filesystem with quorum writes
 syncfs keeps a full local copy of a directory tree on every node of a
 cluster and replicates changes between them. Writes need a quorum and a
 whole-file lock; reads are served locally. Includes the mount.syncfs
 helper for /etc/fstab, a systemd unit for voter-only daemons
 (syncfs-daemon.service) and a template unit for mounts (syncfs@NAME,
 configured in /etc/syncfs/NAME.yaml).
CONTROL
for s in postinst postrm; do
  cat > "$root/DEBIAN/$s" <<'SCRIPT'
#!/bin/sh
set -e
if [ -d /run/systemd/system ]; then
  systemctl daemon-reload >/dev/null 2>&1 || true
fi
SCRIPT
  chmod 0755 "$root/DEBIAN/$s"
done
mkdir -p "$out"
deb="$out/syncfs_${full}_${arch}.deb"
dpkg-deb --root-owner-group -Zxz --build "$root" "$deb" >/dev/null
echo "$deb"
