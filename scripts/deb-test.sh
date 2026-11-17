#!/bin/sh
# Installs a syncfs .deb in a clean Debian container and checks it:
#
#   scripts/deb-test.sh bookworm dist/syncfs_..._amd64.deb
#
# Inside: apt installs the package (and fuse3), the binaries, helper links,
# conffile and units are checked, root mounts from /etc/fstab (types
# `syncfs` and `fuse.syncfs`) and a non-root `user` fstab mount are made
# and used, then the package is removed (conffile kept) and purged.
# Needs docker, /dev/fuse and CAP_SYS_ADMIN. APT_RELAY as in
# container-test.sh.
set -eu
suite=$1
deb=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
repo=$(cd "$(dirname "$0")/.." && pwd)
exec docker run --rm --network host \
  --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined --security-opt seccomp=unconfined \
  -e APT_RELAY="${APT_RELAY:-}" -v "$deb":/pkg.deb:ro -v "$repo/scripts":/scripts:ro \
  "debian:$suite" sh /scripts/deb-test-inside.sh
