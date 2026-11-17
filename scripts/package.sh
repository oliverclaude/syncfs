#!/bin/sh
# Builds the release binary and the .deb for a Debian suite inside a clean
# debian:SUITE container (native architecture), into OUTDIR:
#
#   SYNCFS_PRE=pre scripts/package.sh bookworm dist
#
# APT_RELAY as in container-test.sh.
set -eu
suite=$1
out=${2:-dist}
repo=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$out"
out=$(cd "$out" && pwd)
exec docker run --rm --network host \
  -e APT_RELAY="${APT_RELAY:-}" -e SYNCFS_PRE="${SYNCFS_PRE:-}" -e HOST_UID="$(id -u)" \
  -v "$repo":/src:ro -v "$out":/out -v "syncfs-target-$suite-release":/build \
  "debian:$suite" sh -euc '
    if [ -n "$APT_RELAY" ]; then
      sed -i "s#http://deb.debian.org#$APT_RELAY/deb.debian.org#" /etc/apt/sources.list.d/debian.sources
    fi
    apt-get update -q >/dev/null
    DEBIAN_FRONTEND=noninteractive apt-get install -y -q curl ca-certificates gcc libc6-dev >/dev/null
    curl -sSf https://sh.rustup.rs | sh -s -- -y -q --profile minimal >/dev/null
    . "$HOME/.cargo/env"
    cp -a /src /work && cd /work
    CARGO_TARGET_DIR=/build/target cargo build --release --locked -q
    scripts/build-deb.sh /build/target/release/syncfs '"$suite"' /out
    chown -R "$HOST_UID" /out
  '
