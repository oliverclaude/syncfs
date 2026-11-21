#!/bin/sh
# The part of deb-test.sh that runs in the container.
set -eu
step() { echo "== $*"; }
fail() { echo "FAILED: $*"; exit 1; }
if [ -n "${APT_RELAY:-}" ]; then
  sed -i "s#http://deb.debian.org#$APT_RELAY/deb.debian.org#" /etc/apt/sources.list.d/debian.sources
fi
apt-get update -q >/dev/null
step "$(. /etc/os-release; echo $PRETTY_NAME) $(uname -m): installing $(dpkg-deb -f /pkg.deb Package Version Architecture | tr '\n' ' ')"
DEBIAN_FRONTEND=noninteractive apt-get install -y -q /pkg.deb >/dev/null
syncfs --version
for f in /usr/bin/syncfs /sbin/mount.syncfs /sbin/mount.fuse.syncfs /etc/syncfs/syncfs.yaml \
         /usr/lib/systemd/system/syncfs-daemon.service /usr/lib/systemd/system/syncfs@.service; do
  [ -e "$f" ] || fail "$f missing"
done
dpkg-query -W -f '${Conffiles}\n' syncfs | grep -q /etc/syncfs/syncfs.yaml || fail "conffile not registered"
syncfs --print-config /srv/a /mnt/a | grep -q 'peers: \[\]' || fail "default config is not standalone"

step "root fstab mounts (syncfs and fuse.syncfs)"
mkdir -p /srv/a /mnt/a /srv/b /mnt/b
echo hello > /srv/a/file
cat >> /etc/fstab <<FSTAB
/srv/a /mnt/a syncfs      allow_other,state_dir=/var/lib/syncfs/a,noauto 0 0
/srv/b /mnt/b fuse.syncfs state_dir=/var/lib/syncfs/b,noauto 0 0
FSTAB
mount /mnt/a
mount /mnt/b
findmnt -no FSTYPE /mnt/a | grep -q fuse.syncfs || fail "/mnt/a type: $(findmnt -no FSTYPE /mnt/a)"
[ "$(cat /mnt/a/file)" = hello ] || fail "read through /mnt/a"
echo written > /mnt/b/new
[ "$(cat /srv/b/new)" = written ] || fail "write through /mnt/b"
grep -q '^mode: mount' /mnt/a/.syncfs/node || fail "status file"

step "mount -o remount reloads the configuration"
# The daemon reloads after the helper signals it: wait for the change.
waitfor() { for _ in $(seq 50); do grep -q "$1" "$2" && return 0; sleep 0.1; done; return 1; }
exec 3>>/mnt/a/file
echo appended >&3
mount -o remount,lock_wait=7s /mnt/a || fail "remount /mnt/a (fstab, mount.syncfs)"
waitfor '^lock_wait: 7s' /mnt/a/.syncfs/config || fail "remount /mnt/a not applied: $(cat /mnt/a/.syncfs/config)"
echo again >&3
exec 3>&-
[ "$(cat /srv/a/file)" = "$(printf 'hello\nappended\nagain')" ] || fail "file open across the remount: $(cat /srv/a/file)"
mount -o remount,lock_wait=8s /mnt/b || fail "remount /mnt/b (fstab, fuse.syncfs)"
waitfor '^lock_wait: 8s' /mnt/b/.syncfs/config || fail "remount /mnt/b not applied"
mount -o remount,cluster_name=other /mnt/b || fail "a refused remount must still exit 0"
sleep 1
grep -q '^cluster_name: syncfs' /mnt/b/.syncfs/config || fail "refused remount was applied"
mkdir -p /srv/c /mnt/c
mount -t syncfs /srv/c /mnt/c -o state_dir=/var/lib/syncfs/c
mount -v -o remount,lock_wait=9s /mnt/c || fail "remount without an fstab entry (mount.fuse.syncfs)"
waitfor '^lock_wait: 9s' /mnt/c/.syncfs/config || fail "remount /mnt/c not applied"
mount -o remount,lock_wait=soon /mnt/c 2>/dev/null && fail "bad remount option accepted"
findmnt /mnt/c >/dev/null || fail "remount unmounted /mnt/c"
umount /mnt/c
umount /mnt/a
umount /mnt/b
mount | grep -q /mnt/a && fail "still mounted"

step "user fstab mount (option user, as a normal user)"
useradd -m tester
su tester -c 'mkdir -p ~/b ~/m && echo mine > ~/b/f'
echo "/home/tester/b /home/tester/m syncfs user,noauto 0 0" >> /etc/fstab
su tester -c 'mount /home/tester/m' || fail "user mount"
su tester -c 'cat ~/m/f && echo more > ~/m/g' || fail "user mount not usable"
[ "$(stat -c %U /home/tester/b/g)" = tester ] || fail "file owner"
for c in /proc/[0-9]*/comm; do
  case "$(cat "$c" 2>/dev/null)" in *syncfs) echo "served by uid $(stat -c %u "${c%/comm}")" ;; esac
done
su tester -c 'umount /home/tester/m' || fail "user unmount"
findmnt /home/tester/m >/dev/null && fail "still mounted"

step "systemd units"
DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends systemd >/dev/null
mkdir -p /srv/u /mnt/u
printf 'backing_dir: /srv/u\nmountpoint: /mnt/u\n' > /etc/syncfs/u.yaml
cp /usr/lib/systemd/system/syncfs@.service /tmp/syncfs@u.service
systemd-analyze verify --man=no /usr/lib/systemd/system/syncfs-daemon.service /tmp/syncfs@u.service || fail "unit verify"
# Run the template's command line for instance u (no systemd running here).
cmd=$(sed -n 's/^ExecStart=//p' /usr/lib/systemd/system/syncfs@.service | sed 's/%i/u/g')
mkdir -p /var/lib/syncfs/u
$cmd > /tmp/unit.log 2>&1 &
pid=$!
for _ in $(seq 50); do findmnt /mnt/u >/dev/null && break; sleep 0.1; done
findmnt /mnt/u >/dev/null || { cat /tmp/unit.log; fail "template ExecStart did not mount"; }
echo via-unit > /mnt/u/x && [ "$(cat /srv/u/x)" = via-unit ] || fail "template mount not usable"
kill -TERM $pid && wait $pid || fail "template stop: $(cat /tmp/unit.log)"
findmnt /mnt/u >/dev/null && fail "template mount left behind"
rm /etc/syncfs/u.yaml

step "remove keeps the conffile, purge drops it"
apt-get remove -y -q syncfs >/dev/null
[ -e /etc/syncfs/syncfs.yaml ] || fail "conffile removed by remove"
[ -e /sbin/mount.syncfs ] && fail "helper left after remove"
apt-get purge -y -q syncfs >/dev/null
[ -e /etc/syncfs/syncfs.yaml ] && fail "conffile left after purge"
step "all deb checks passed"
