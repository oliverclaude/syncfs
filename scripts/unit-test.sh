#!/bin/sh
# Runs the packaged systemd units under a real systemd (PID 1 in a
# privileged debian:SUITE container):
#
#   scripts/unit-test.sh trixie dist/syncfs_..._amd64.deb
#
# Starts and stops syncfs-daemon.service and syncfs@data.service (a mount
# configured in /etc/syncfs/data.yaml). APT_RELAY as in container-test.sh.
set -eu
suite=$1
deb=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
# Optional third argument: an architecture to emulate (qemu-user/binfmt).
platform=${3:+--platform linux/$3}
name=syncfs-unit-test-$$
img=syncfs-systemd-$suite${3:+-$3}
relay=${APT_RELAY:-}
docker build --network host $platform -q -t "$img" - >/dev/null <<DOCKERFILE
FROM debian:$suite
RUN if [ -n "$relay" ]; then sed -i "s#http://deb.debian.org#$relay/deb.debian.org#" /etc/apt/sources.list.d/debian.sources; fi \
 && apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends systemd systemd-sysv fuse3 >/dev/null
CMD ["/sbin/init"]
DOCKERFILE
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
docker run -d $platform --name "$name" --network host --privileged --cgroupns=private \
  --tmpfs /run --tmpfs /run/lock -v "$deb":/pkg.deb:ro "$img" >/dev/null
x() { docker exec "$name" sh -euc "$1"; }
for _ in $(seq 100); do
  s=$(docker exec "$name" systemctl is-system-running 2>/dev/null || true)
  case $s in running|degraded) break ;; esac
  sleep 0.2
done
echo "== systemd: $s"
x 'apt-get install -y -q /pkg.deb >/dev/null 2>&1; dpkg-query -W syncfs'
echo "== syncfs-daemon.service"
x 'cat > /etc/syncfs/syncfs.yaml <<Y
node_id: 1
listen: 127.0.0.1:7431
Y
systemctl start syncfs-daemon
# journald does not run under qemu-user emulation; then only the state counts.
if [ -n "$(journalctl -q -n 1 2>/dev/null)" ]; then
  for i in $(seq 100); do journalctl -u syncfs-daemon --no-pager -o cat | grep -q "voter node 1 running" && break; sleep 0.1; done
  journalctl -u syncfs-daemon --no-pager -o cat | grep "voter node 1 running"
else
  sleep 3; echo "(no journal)"
fi
systemctl is-active syncfs-daemon
test -d /var/lib/syncfs
echo "lock_wait: 6s" >> /etc/syncfs/syncfs.yaml
systemctl reload syncfs-daemon
# status.json is rewritten at least every 5 heartbeats.
for i in $(seq 150); do grep -q "\"lock_wait\": *\"6s\"" /var/lib/syncfs/status.json && break; sleep 0.1; done
grep -q "\"lock_wait\": *\"6s\"" /var/lib/syncfs/status.json
systemctl is-active syncfs-daemon
echo reloaded
systemctl stop syncfs-daemon
! systemctl is-active syncfs-daemon
systemctl show -p Result --value syncfs-daemon | grep -x success'
echo "== syncfs@data.service"
x 'mkdir -p /srv/data /mnt/data && echo hi > /srv/data/f
cat > /etc/syncfs/data.yaml <<Y
node_id: 1
listen: 127.0.0.1:7432
backing_dir: /srv/data
mountpoint: /mnt/data
Y
systemctl start syncfs@data
for i in $(seq 100); do findmnt /mnt/data >/dev/null && break; sleep 0.1; done
systemctl is-active syncfs@data
findmnt -no FSTYPE /mnt/data
test "$(cat /mnt/data/f)" = hi
echo w > /mnt/data/g && test "$(cat /srv/data/g)" = w
echo "lock_wait: 6s" >> /etc/syncfs/data.yaml
systemctl reload syncfs@data
for i in $(seq 50); do grep -q "^lock_wait: 6s" /mnt/data/.syncfs/config && break; sleep 0.1; done
grep "^lock_wait: 6s" /mnt/data/.syncfs/config
systemctl is-active syncfs@data
findmnt /mnt/data >/dev/null
echo reloaded, still mounted
test -d /var/lib/syncfs/data
systemctl stop syncfs@data
! findmnt /mnt/data
echo unmounted on stop
systemctl start syncfs@missing
test "$(systemctl show -p ConditionResult --value syncfs@missing)" = no
echo "syncfs@missing skipped (no /etc/syncfs/missing.yaml)"'
echo "== all unit checks passed"
