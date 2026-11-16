# syncfs

`syncfs` is a replicating FUSE filesystem for Linux. Several `syncfs` nodes
form a cluster and keep a full copy of the same filesystem on every node.

> **Status:** early development. Steps 1 (configuration), 2 (single-node
> passthrough FUSE filesystem, mount helper, fstab), 3 (the `.syncfs`
> status directory) and 4 (wire protocol, peer membership, daemon mode) are
> done. Nodes find each other and track quorum, but nothing is replicated
> yet: a mount today is a local mirror of its backing directory.

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
- **Transport.** Plaintext TCP in a full mesh (see
  [Cluster networking](#cluster-networking)). There is no authentication or
  encryption yet, so run it only on a trusted network.
- **Introspection.** A hidden, read-only `.syncfs` directory at the root of
  the mount exposes node, peer, lock, queue and replication stats (see
  [below](#the-syncfs-status-directory)).
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
| `cluster_name`       | `cluster_name=NAME`           | `--cluster-name NAME`    | `syncfs`                        |
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

## The `.syncfs` status directory

Every mount has a hidden, read-only directory `.syncfs` at its root. It is
not listed by `ls -a` of the root, but any path through it works:

```sh
cat /mnt/data/.syncfs/stats
jq .peers /mnt/data/.syncfs/status.json
```

| File          | Contents (YAML)                                                                  |
|---------------|----------------------------------------------------------------------------------|
| `node`        | node id, mode, version, hostname, pid, start time, uptime, paths, mount options   |
| `config`      | the effective configuration (all layers merged)                                  |
| `stats`       | per-operation call and error counts, bytes read/written, open files/dirs, inodes |
| `peers`       | cluster size, quorum, reachable nodes, quorum verdict, every peer's live state  |
| `locks`       | held whole-file locks and the lease timeout (step 5)                             |
| `replication` | dirty files/blocks and journal queue length (step 7)                             |
| `status.json` | all of the above as one JSON object, keyed by file name, plus `format_version`   |

Sections whose subsystem does not exist yet say `implemented: false`.
Field names and nesting are the same in the YAML files and in
`status.json`; new fields may be added, and `format_version` changes only
for incompatible changes.

- **Contents are generated when a file is opened.** Each open takes one
  snapshot and every read serves it (direct I/O), so `cat`, `less` and
  `jq` see consistent text regardless of the size `stat` reported.
- **Read-only.** Any change to the directory or its files (write, create,
  truncate, unlink, rename into or out of it, link, chmod, xattrs) fails with
  `EROFS`. Files are mode 0444 and the directory 0555, owned by the owner of
  the backing directory, so for users other than root the kernel's
  permission check usually refuses first with `EACCES`. Removing or
  renaming `.syncfs` itself fails with `EROFS`.
- **Nothing is stored in the backing directory.** If the backing directory
  already contains a real `.syncfs`, it is shadowed: hidden from the mount
  and untouchable through it (but left intact on disk). syncfs logs a warning
  at mount and `node` reports `shadowed_backing_entry: true`. Rename it in
  the backing directory to reach it through the mount.
- **Counters** are relaxed atomics (one uncontended add per request).
  Requests for `.syncfs` itself are counted only in `status_requests`, not
  in the per-operation or byte counters. Errors are counted per operation,
  including expected ones such as `ENOENT` lookups.
- **Adding a file.** A subsystem implements `status::Provider` (a name and
  a `render()` returning a JSON value) and registers it in
  `mount::register_status`; it then appears as a YAML file and a
  `status.json` key. See `src/status/`.

## Cluster networking

Nodes talk plaintext TCP on `listen` (default port 7420). Run it only on a
trusted network: there is no authentication or encryption.

### Topology

Full mesh: one TCP connection per pair of nodes, opened by the node with the
lower `node_id` and accepted by the higher one. All traffic between two
nodes uses that one connection in both directions, so messages between them
arrive in order. A 99-node cluster has 4851 connections, 98 per node. Each
connection has a reader and a writer thread, and each lower-id node has a
dialer thread per higher peer, so a node runs at most about 250 small
threads. That is fine at this size, so there is no gossip or relaying.

### Wire format

Every connection starts with an 8-byte preamble in each direction:
`SYNCFS` and the protocol version as a big-endian u16 (currently 1). The
preamble never changes, so nodes on different protocol versions recognise
the mismatch, log it and close. After the preamble come frames:

```
u32 big-endian payload length | payload = one postcard-encoded Message
```

- The length must be from 1 to 32 MiB. Before the handshake completes the
  limit is 64 KiB.
- The payload must decode to exactly one message with nothing left over.
- Anything else (bad magic, oversized, undecodable or trailing bytes)
  closes the connection.
- Messages are identified by position, so within one protocol version
  variants and fields are only ever appended. Anything else bumps the
  version.

The message set already names what later steps need. Only the handshake,
heartbeats and `Goodbye` are used so far; the others are logged and ignored.

| Messages | Used by |
|----------|---------|
| `Hello`, `Welcome`, `Reject` | handshake (step 4) |
| `Heartbeat {seq, up}`, `Goodbye {reason}` | liveness (step 4) |
| `LockRequest`, `LockGrant`, `LockDeny`, `LockRenew`, `LockRelease`, `VoteRequest`, `Vote` | locks and quorum (step 5) |
| `JournalEntry`, `JournalAck`, `BlockPull`, `BlockData`, `BlockPush` | replication (step 7) |
| `CatchUpRequest`, `CatchUpDone` | rejoin (step 8) |

### Handshake

1. The dialer sends `Hello`: its node id, `cluster_name`, the sorted node ids
   of its `peers`, a random incarnation number (new on every start) and its
   software version.
2. The acceptor refuses with `Reject {reason, detail}` and closes when:
   - `cluster_name` differs (`ClusterName`)
   - the dialer uses the acceptor's own id (`SameNodeId`)
   - the dialer's id is not in the acceptor's peers (`UnknownNode`)
   - the two sets of node ids differ (`Membership`)
   - the dialer has the higher id (`WrongDirection`)
3. Otherwise the acceptor answers `Welcome`, with its id and incarnation.
4. The dialer checks that the node that answered is the one it meant to
   reach at that address (otherwise `expected node N ..., node M answered`).

Membership is compared by node ids only, not addresses, because nodes may
legitimately reach the same peer by different names or addresses. Refused
peers show as `rejected` with the reason; the refusal is logged at warn
level at most once a minute.

### Liveness and peer states

- Both sides send a `Heartbeat` every `heartbeat_interval`. It carries the
  ids the sender sees up, shown as `sees` in `.syncfs/peers`.
- Any frame counts as a sign of life. A connection silent for 3 intervals
  is closed and the peer becomes `down` ("heartbeat timeout").
- A closed socket marks the peer down at once ("connection closed"), and so
  does a `Goodbye` from a clean shutdown ("left: shutting down").
- The dialer reconnects with exponential backoff: from
  `heartbeat_interval / 2`, doubling, capped at 5 x the interval (between
  1 s and 30 s), with +-20% jitter. A successful handshake resets it.
- A new connection from a peer replaces the old one, which covers a peer
  that restarted before the old connection timed out.

| State | Meaning |
|-------|---------|
| `unknown` | never connected since this node started |
| `connecting` | first connection attempt in progress (dialing side) |
| `up` | connected and heartbeating |
| `down` | was up, or connection attempts fail; `last_error` says why |
| `rejected` | the handshake was refused because the configurations disagree |

`reachable` is this node plus its `up` peers. `have_quorum` is
`reachable >= quorum`, where quorum is a strict majority of all configured
nodes, voters included.

### Daemon mode

`syncfs --mode daemon` (or `mode: daemon` in the YAML) runs a voter-only
node: no backing directory, no mount, just membership (and, from step 5,
lock and quorum votes). It daemonizes unless `--foreground` is given. With
no `.syncfs` directory to read, it writes the same status as JSON to
`<state_dir>/status.json`. The file is replaced atomically on every
membership change and at least every 5 heartbeat intervals, and removed on
a clean exit.

```sh
syncfs --mode daemon --node-id 3 --listen 0.0.0.0:7420 \
  --peer 1@10.0.0.1:7420 --peer 2@10.0.0.2:7420 --peer 3@10.0.0.3:7420
jq .peers /var/lib/syncfs/status.json
```

### Why threads and postcard

- **Threads:** at most 98 connections per node is a small number of
  threads, and the FUSE side is synchronous too, so an async runtime would
  add a dependency and two programming models for no gain. Later steps can
  queue messages to a peer with `Network::send`.
- **postcard:** compact, serde-based (the messages are plain Rust types)
  and has a documented, stable wire format, unlike bincode 2's
  configuration-dependent one. Frame decoding is fuzzed: random bytes,
  mutated frames, and streams split at random points must never panic.

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
- **`.syncfs` inodes.** The directory is inode `2^63` and its files
  `2^63 + 1 + n`, far from the backing counter. See `src/fs/virt.rs`.

## Testing

- `cargo test` runs unit tests plus FUSE mount tests (as the current user;
  they skip with a message when `/dev/fuse` or `fusermount3` is missing).
  Run the `mount`, `status` and `cluster` test binaries with `sudo` too for
  the root and multi-user cases; CI does both on x86_64 and arm64.
- `tests/net.rs` runs membership in process: handshake rejections, leave
  and rejoin, and a 25-node mesh. `tests/cluster.rs` runs 3- and 5-node
  clusters of real processes (mounts and daemons), kills, stops and
  restarts nodes, and requires every survivor's view to converge within
  8 s. `cargo test --release --test cluster -- --ignored` adds a 99-process
  cluster.
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
3. **Hidden `.syncfs` folder** (done): read-only status files, op counters,
   provider registry for later steps.
4. **Wire protocol and peer membership** (done): framed TCP, handshake,
   heartbeats, peer states, reconnect, voter daemon mode.
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
