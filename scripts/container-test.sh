#!/bin/sh
# Builds and tests syncfs inside a clean Debian container.
#
#   scripts/container-test.sh bookworm            # native architecture
#   scripts/container-test.sh trixie arm64        # emulated via qemu-user/binfmt
#
# Runs the whole test suite as root and the FUSE mount tests again as an
# unprivileged user (fusermount3 path). Needs docker; FUSE needs
# /dev/fuse and CAP_SYS_ADMIN in the container.
# Set APT_RELAY=http://HOST:PORT to fetch Debian packages through a local
# relay that maps /deb.debian.org/... to the mirror, if apt cannot reach it.
set -eu
suite=${1:-bookworm}
arch=${2:-}
repo=$(cd "$(dirname "$0")/.." && pwd)
native=$(docker version -f '{{.Server.Arch}}')
platform="--platform linux/${arch:-$native}"
target_vol="syncfs-target-$suite-${arch:-native}"

exec docker run --rm $platform --network host \
  --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined --security-opt seccomp=unconfined \
  -e APT_RELAY="${APT_RELAY:-}" \
  -v "$repo":/src:ro -v "$target_vol":/build \
  "debian:$suite" sh -euc '
    if [ -n "$APT_RELAY" ]; then
      sed -i "s#http://deb.debian.org#$APT_RELAY/deb.debian.org#" /etc/apt/sources.list.d/debian.sources
    fi
    apt-get update -q >/dev/null
    DEBIAN_FRONTEND=noninteractive apt-get install -y -q curl ca-certificates gcc libc6-dev fuse3 jq >/dev/null
    echo user_allow_other >> /etc/fuse.conf
    curl -sSf https://sh.rustup.rs | sh -s -- -y -q --profile minimal >/dev/null
    . "$HOME/.cargo/env"
    export CARGO_TARGET_DIR=/build/target
    cp -a /src /work && cd /work
    echo "== $(. /etc/os-release; echo $PRETTY_NAME) $(uname -m), $(rustc --version)"
    cargo test 2>&1 | grep -E "^test result|FAILED|panicked|SKIPPED" 
    bin=$(cargo test --test mount --no-run --message-format=json 2>/dev/null | jq -r "select(.executable != null) | .executable" | grep /mount-)
    useradd -m tester
    chmod -R a+rX /build /work
    echo "== mount tests as unprivileged user"
    su tester -c "cd /tmp && $bin -q" > /tmp/user.log 2>&1 || { grep -m3 -iE "fusermount|error|exited" /tmp/user.log; }; grep -E "^test result" /tmp/user.log
  '
