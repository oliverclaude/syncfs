# syncfs

`syncfs` is a replicating FUSE filesystem for Linux. Several `syncfs` nodes
form a cluster and keep a full copy of the same filesystem on every node.

> **Status:** early development. Steps 1 (configuration) and 2 (single-node
> passthrough FUSE filesystem, mount helper, fstab) are done. Clustering is
> not implemented yet: a mount today is a local mirror of its backing
> directory.

## Design

- **Local source of truth.** Every mounting node stores its data in a local
  backing directory. Reads are always served from that local copy.
- **Quorum + whole-file locks.** A write is accepted only when the node can
  see a quorum (strict majority of all configured nodes) and holds the
  whole-file write lock. Locks are taken on open-for-write or first write,
  released on close, and expire after a lease timeout if the holder dies.
  Each node keeps the lock table in memory.
- **Async replication.** After the local write completes, changes are shipped
  to the other nodes in the background, driven by a persisted per-file
  dirty-block bitmap and an ordered metadata journal (create, rename, delete,
  chmod, ...).
- **No quorum: read-only.** Without quorum writes fail with `EROFS`
  ("read-only file system") and reads keep working from the local copy.
- **Rejoin.** A node that was offline catches up from its peers before it
  accepts writes again. The latest committed version wins.
- **Voter-only daemon mode.** A node can run with no backing directory and no
  mount. It only takes part in quorum and lock voting, which lets you build
  a 3-node quorum from 2 data nodes and 1 cheap voter.
- **Cluster size.** Static peer list in the config, 1 to 99 nodes. 3 is the
  intended starting point.
- **Transport.** Plaintext TCP. There is no authentication or encryption yet,
  so run it only on a trusted network.
- **Introspection.** A hidden, read-only `.syncfs` directory at the root of
  the mount exposes node, peer, lock, queue and replication stats.
- **Mounting.** `-o debug`, `allow_other` / `allow_root`, user and root
  mounts, and a `mount.syncfs` helper so it works from `/etc/fstab`.

Targets: Debian 12 (bookworm) and Debian 13 (trixie), on x86_64 and aarch64.

## Configuration

Configuration comes from four layers. Later layers override earlier ones:

1. built-in defaults
2. the YAML config file (default `/etc/syncfs/syncfs.yaml`)
3. mount options: `-o key=value,flag,...` (what fstab passes)
4. command-line flags (`--node-id 2`, `--listen ...`, ...)

Every option has a default, so a single standalone node needs only a backing
directory and a mount point. See [`examples/syncfs.yaml`](examples/syncfs.yaml)
for every option with its default and an explanation.

Show the effective configuration without mounting anything:

```sh
syncfs --print-config /srv/syncfs-data /mnt/syncfs
syncfs --config examples/syncfs.yaml -o node_id=2 --print-config /srv/data /mnt/sync
syncfs --mode daemon --node-id 3 --print-config
```

### Options

| YAML key             | `-o` option                   | CLI flag                 | Default                         |
|----------------------|-------------------------------|--------------------------|---------------------------------|
| (file path)          | `config=PATH`                 | `-c`, `--config PATH`    | `/etc/syncfs/syncfs.yaml`       |
| `node_id`            | `node_id=N`                   | `--node-id N`            | `1`                             |
| `mode`               | `mode=mount\|daemon`          | `--mode mount\|daemon`   | `mount`                         |
| `listen`             | `listen=IP:PORT`              | `--listen IP:PORT`       | `0.0.0.0:7420`                  |
| `peers`              | `peer=ID@HOST:PORT` (repeat)  | `--peer ID@HOST:PORT`    | empty (standalone node)         |
| `backing_dir`        | `backing_dir=PATH`            | `--backing-dir`, 1st arg | none (required in mount mode)   |
| `mountpoint`         | `mountpoint=PATH`             | `--mountpoint`, 2nd arg  | none (required in mount mode)   |
| `state_dir`          | `state_dir=PATH`              | `--state-dir PATH`       | `/var/lib/syncfs` (root) or `$XDG_STATE_HOME/syncfs` |
| `lock_lease_timeout` | `lock_lease_timeout=30s`      | `--lock-lease-timeout`   | `30s`                           |
| `heartbeat_interval` | `heartbeat_interval=1s`       | `--heartbeat-interval`   | `1s`                            |
| `block_size`         | `block_size=64K`              | `--block-size`           | `64KiB`                         |
| `debug`              | `debug`                       | `-d`, `--debug`          | `false`                         |
| `allow_other`        | `allow_other`                 | `--allow-other`          | `false`                         |
| `allow_root`         | `allow_root`                  | `--allow-root`           | `false`                         |
| `log_level`          | `log_level=LEVEL`             | `--log-level LEVEL`      | `info` (`debug` if debug is on) |
| `foreground`         | `foreground`                  | `-f`, `--foreground`     | `false` (`true` if debug is on) |

Booleans in `-o` accept `flag`, `flag=true|false|yes|no|on|off|1|0`, and
`noflag` to turn a flag off. A `peers` list given via `-o` or the CLI
replaces the YAML list rather than being merged into it. Generic mount
options such as `rw`, `defaults`, `noauto`, `nofail`, `_netdev`, `user`,
`users` and `x-systemd.*` are accepted and ignored. The kernel mount flags
`nosuid`, `nodev`, `noexec`, `noatime`, `sync`, `dirsync` (and their
opposites) are applied to the FUSE mount.

### Peers and quorum

`peers` is the full cluster membership and must include this node's own
entry. All nodes should use the same list. Quorum is
`floor(nodes / 2) + 1`. An empty list means a standalone single-node cluster.

## Mounting

```sh
syncfs /srv/data /mnt/data                 # mount and go to the background
syncfs /srv/data /mnt/data -f              # stay in the foreground
syncfs /srv/data /mnt/data -o debug        # foreground + FUSE request log on stderr
syncfs /srv/data /mnt/data -o allow_other  # let other users in
fusermount3 -u /mnt/data                   # unmount (or: umount /mnt/data as root)
```

- Without `-f`/`debug`, the command returns once the mount is up (or fails
  with the reason) and a detached daemon keeps serving, logging to syslog.
  SIGINT, SIGTERM or SIGHUP unmount cleanly.
- **Normal users** mount through `fusermount3` (package `fuse3`).
  `allow_other` additionally needs `user_allow_other` in `/etc/fuse.conf`.
- **Root** mounts with mount(2) directly; the mount shows up as type
  `fuse.syncfs`. The kernel checks permissions (`default_permissions`),
  and with `allow_other` files created by other users belong to them.

### /etc/fstab

Install the binary as `/usr/bin/syncfs` and link the helper:
`ln -s /usr/bin/syncfs /sbin/mount.syncfs` (and optionally
`/sbin/mount.fuse.syncfs`). Then either type works:

```
/srv/data  /mnt/data  syncfs       node_id=1,_netdev,allow_other  0 0
/srv/data  /mnt/data  fuse.syncfs  node_id=1,_netdev,allow_other  0 0
```

`fuse.syncfs` also works through fuse3's `mount.fuse3` when `syncfs` is on
`PATH`. The source field is the backing directory; a source without a `/`
(e.g. `syncfs`) is only a label and the backing directory comes from the
config file. The helper supports mount's `-f` (fake), `-s` (sloppy:
ignore unknown options), `-v` and `-n`.

## Internals

- **Inodes.** Each inode the kernel holds is an `O_PATH` descriptor on the
  backing file, so renames (ours, a peer's or an admin's) never make an
  inode point at the wrong file. FUSE inode numbers come from a counter and
  are keyed by the backing `(st_dev, st_ino)`: hard links share a number,
  nested filesystems cannot collide, numbers stay stable for the whole mount
  and are never reused. Descriptors are closed when the kernel forgets the
  inode. See `src/fs/inode.rs`.
- **Write boundary.** Every change (create, write, truncate, rename,
  unlink, chmod/chown/utimens, xattrs, fallocate) is described as a
  `Mutation` and passed through the `WriteHooks` trait before and after it
  is applied, and open-for-write / release go through
  `open_write`/`release_write`. Step 2 uses `LocalOnly`; locking, quorum and
  dirty tracking plug in there. See `src/fs/hooks.rs`.

## Testing

- `cargo test` runs unit tests plus FUSE mount tests (as the current user;
  they skip with a message when `/dev/fuse` or `fusermount3` is missing).
  Run the mount test binary with `sudo` too for the root and multi-user
  cases; CI does both on x86_64 and arm64.
- `scripts/container-test.sh bookworm|trixie [arm64]` builds and tests in a
  clean Debian container, natively or emulated through qemu-user/binfmt.
- [pjdfstest](https://github.com/pjd/pjdfstest) as root on a mount with
  `allow_other`: 238 files, 8798 tests, all passing.

## Build plan

Each step ends with passing tests and a commit.

1. **Skeleton and config** (done): Cargo project, CI, YAML config, `-o`
   options, CLI, precedence and validation.
2. **Local passthrough FUSE** (done): single-node mirror of the backing directory,
   `debug`, `allow_other`, user/root mounts, `mount.syncfs` for fstab.
   Integration tests and `pjdfstest`.
3. **Hidden `.syncfs` folder** with stats.
4. **Wire protocol and peer membership**: framed TCP, heartbeats, peer
   up/down tracking.
5. **Quorum and lock table**: whole-file locks, leases, voter-only daemon
   mode.
6. **Write path**: lock, quorum check, local write, mark dirty.
7. **Async replication**: dirty-block bitmap and metadata journal.
8. **Rejoin and recovery**.
9. **Packaging**: `.deb` for bookworm and trixie on amd64 and arm64, static
   musl portable binaries, systemd unit for daemon mode.
10. **Soak test**: randomized multi-node run with restarts and partitions.

## Building

```sh
cargo build --release
cargo test
```

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.
