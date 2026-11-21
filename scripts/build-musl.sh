#!/bin/sh
# Builds the static (musl) portable binary for this machine's architecture
# and packs it with the docs, example config and systemd units:
#
#   SYNCFS_PRE=pre scripts/build-musl.sh OUTDIR
#   -> OUTDIR/syncfs-0.2.0-pre-x86_64-linux-musl.tar.gz
#
# Needs rustup, musl-tools (musl-gcc) and binutils (readelf).
set -eu
out=${1:-dist}
repo=$(cd "$(dirname "$0")/.." && pwd)
target="$(uname -m)-unknown-linux-musl"
version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$repo/Cargo.toml" | head -1)
[ -n "${SYNCFS_PRE:-}" ] && version="$version-$SYNCFS_PRE"
rustup target add "$target" >/dev/null
cd "$repo"
cargo build --release --locked --target "$target"
bin="target/$target/release/syncfs"
# Static, and it runs.
if readelf -ld "$bin" | grep -Eq 'INTERP|NEEDED'; then
  readelf -ld "$bin" | grep -E 'INTERP|NEEDED'; echo "not static" >&2; exit 1
fi
"$bin" --version
name="syncfs-$version-$(uname -m)-linux-musl"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
d="$stage/$name"
install -D -m 0755 "$bin" "$d/syncfs"
install -m 0644 README.md LICENSE-MIT LICENSE-APACHE "$d/"
install -D -m 0644 examples/syncfs.yaml "$d/examples/syncfs.yaml"
install -D -m 0644 packaging/syncfs-daemon.service packaging/syncfs@.service -t "$d/systemd/"
mkdir -p "$out"
tar -C "$stage" --owner=0 --group=0 -czf "$out/$name.tar.gz" "$name"
echo "$out/$name.tar.gz"
