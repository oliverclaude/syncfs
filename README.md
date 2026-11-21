# syncfs

`syncfs` is a replicating FUSE filesystem for Linux. Several `syncfs` nodes
form a cluster and keep a full copy of the same filesystem on every node.

> **Status:** pre-release. Build plan steps 1 to 9 are done: configuration,
> the FUSE filesystem, the `.syncfs` status directory, membership, quorum
> locks, the write path, replication, rejoin and recovery, and packaging
> (`.deb` and static binaries, see [Installing](#installing)). Step 10, a
> randomized soak test, is next.

## Installing

Builds of each `v*` tag are on the
[releases page](https://github.com/oliverclaude/syncfs/releases), with a
`SHA256SUMS` file (`sha256sum -c SHA256SUMS`). Asset names spell the
package version `0.1.0~pre` as `0.1.0.pre`, because GitHub rewrites `~`.

**Debian 12 (bookworm) and 13 (trixie)**, amd64 and arm64: take the
package for your suite (`+deb12` or `+deb13`) and architecture, then

```sh
sudo apt install ./syncfs_0.1.0.pre-1+deb13_amd64.deb
```

It depends on `fuse3` and `procps` (`kill`, for `systemctl reload`) and installs:

| Path | What |
| --- | --- |
| `/usr/bin/syncfs` | the binary |
| `/sbin/mount.syncfs`, `/sbin/mount.fuse.syncfs` | mount helpers for `/etc/fstab` (links to the binary, in `/usr/sbin` on merged-`/usr` systems) |
| `/etc/syncfs/syncfs.yaml` | configuration, a conffile: every setting is commented out, so it describes a standalone node until you edit it. `apt remove` keeps it, `apt purge` deletes it |
| `/usr/lib/systemd/system/syncfs-daemon.service` | voter-only daemon (`--mode daemon`) configured by `/etc/syncfs/syncfs.yaml`, state in `/var/lib/syncfs` |
| `/usr/lib/systemd/system/syncfs@.service` | template for mounts: `syncfs@NAME` mounts what `/etc/syncfs/NAME.yaml` configures (it must set `backing_dir` and `mountpoint`), state in `/var/lib/syncfs/NAME` |
| `/usr/share/doc/syncfs/` | this README and the fully commented example config |

The package enables and starts nothing. To run a voter daemon, set
`cluster_name`, `node_id`, `listen` and `peers` in
`/etc/syncfs/syncfs.yaml` and `systemctl enable --now syncfs-daemon`. To
mount through systemd instead of fstab, write `/etc/syncfs/data.yaml`
(with the same cluster settings, plus `backing_dir` and `mountpoint`) and
`systemctl enable --now syncfs@data`; stopping the unit unmounts cleanly,
and `systemctl reload` re-reads the config file in place (see
[Reloading the configuration](#reloading-the-configuration)).

**Any Linux on x86_64 or aarch64**: the `syncfs-*-linux-musl.tar.gz`
archives hold a static binary (no libraries needed; the kernel's FUSE and,
for non-root mounts, `fusermount3` from fuse3), the docs, the example
config and the two systemd units. Copy `syncfs` to `/usr/bin` and link the
helpers as described under [/etc/fstab](#etcfstab).

To build the packages yourself (needs Docker): `scripts/package.sh
bookworm|trixie OUTDIR` builds the `.deb` for this machine's architecture
in a clean `debian:SUITE` container, `scripts/deb-test.sh SUITE DEB`
test-installs it in another one, and `scripts/build-musl.sh OUTDIR`
builds the static archive (needs `musl-tools`). Set `SYNCFS_PRE=pre` for
a `~pre` version. The `Package` workflow does all of this on x86_64 and
arm64 runners and publishes a pre-release for each `v*` tag.

**Nightly builds.** Every day at 20:00 UTC (22:00 SAST), and when run by
hand (`gh workflow run Package --ref main`), the `Package` workflow builds
`main` with the same jobs and publishes the result as the pre-release
`nightly-YYYYMMDD` (the UTC date). It is never marked latest. If `main` is
still at the commit of the newest release (a `v*` tag or the last
nightly), it builds nothing. The package version is
`0.1.0~nightly.YYYYMMDD` (`0.1.0.nightly.YYYYMMDD` in asset names,
`0.1.0-nightly.YYYYMMDD` for the musl archives): each day's nightly
upgrades the one before, and the `0.1.0` release upgrades every nightly.
Note that it also sorts below `0.1.0~pre` (`nightly` < `pre`). A second
nightly on the same day (after `main` moved) replaces that day's release
under the same version, so `apt` needs `--reinstall` to pick it up.
The workflow keeps the 14 newest `nightly-YYYYMMDD` pre-releases and
deletes older ones (and their tags). It never touches `v*` releases.
`NIGHTLY_KEEP` in the workflow sets the number.
Scheduled runs can start late, and GitHub disables scheduled workflows
after 60 days without activity in the repository.

## Design

- **Local source of truth.** Every mounting node stores its data in a local
  backing directory. Reads are always served from that local copy.
- **Quorum + whole-file locks.** A write is accepted only when the node can
  see a quorum (strict majority of all configured nodes) and holds the
  whole-file write lock. Locks are taken on open-for-write (or for the
  single operation, for changes without an open handle), released on
  close, and expire after a lease timeout if the holder dies. Each node
  keeps its lock table in memory. See [Write locks](#write-locks).
- **Async replication.** After the local write completes, changes are shipped
  to the other nodes in the background, driven by a persisted per-file
  dirty-block bitmap and an ordered metadata journal (create, rename, delete,
  chmod, ...). Peers pull the changed blocks. See [Replication](#replication).
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
| `lock_wait`          | `lock_wait=10s`               | `--lock-wait`            | `10s` (`0` = fail at once)      |
| `freshness_timeout`  | `freshness_timeout=30s`       | `--freshness-timeout`    | `30s`                           |
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
  SIGINT or SIGTERM unmount cleanly; SIGHUP reloads the configuration
  (see [Reloading the configuration](#reloading-the-configuration)). A
  root mount that is busy
  (a file open, a process inside it) keeps serving after the first signal
  (logged); a second signal detaches it lazily, like `umount -l`, and the
  daemon exits when the last open file is closed. A user mount is
  detached lazily by `fusermount3` on the first signal.
- **Normal users** mount through `fusermount3` (package `fuse3`).
  `allow_other` additionally needs `user_allow_other` in `/etc/fuse.conf`.
- **Root** mounts with mount(2) directly; the mount shows up as type
  `fuse.syncfs`. The kernel checks permissions (`default_permissions`),
  and with `allow_other` files created by other users belong to them.

### /etc/fstab

The `.deb` installs the helpers `/sbin/mount.syncfs` and
`/sbin/mount.fuse.syncfs`. With the static binary, install it as
`/usr/bin/syncfs` and link them yourself:
`ln -s /usr/bin/syncfs /sbin/mount.syncfs` (and optionally
`/sbin/mount.fuse.syncfs`). Then either type works:

```
/srv/data  /mnt/data  syncfs       node_id=1,_netdev,allow_other  0 0
/srv/data  /mnt/data  fuse.syncfs  node_id=1,_netdev,allow_other  0 0
```

With the `user` option a normal user can mount the entry (`mount
/home/me/m`; the mount point and backing directory must be theirs, and it
runs as that user, through `fusermount3`) and unmount it again (`umount
/home/me/m`):

```
/home/me/b  /home/me/m  syncfs  user,noauto  0 0
```

mount(8) clears the environment for such a mount, so the default state
directory comes from the user's passwd home (`~/.local/state/syncfs`).

`fuse.syncfs` also works through fuse3's `mount.fuse3` when `syncfs` is on
`PATH`. The source field is the backing directory; a source without a `/`
(e.g. `syncfs`) is only a label and the backing directory comes from the
config file. The helper supports mount's `-f` (fake), `-s` (sloppy:
ignore unknown options), `-v` and `-n`.

### Reloading the configuration

A running mount or voter daemon reloads its configuration in place, with
no unmount and without stopping the applications using it:

```sh
mount -o remount,lock_wait=20s /mnt/data   # new -o options, then reload
mount -o remount /mnt/data                 # re-read the config file
systemctl reload syncfs@data               # the same for a unit (SIGHUP)
kill -HUP "$(cat /var/lib/syncfs/syncfs.pid)"   # or by hand
```

Open files, held locks and cluster connections are not touched. The new
configuration is built like the first one: defaults < config file < `-o`
options < the command-line flags the daemon was started with (so a
remount cannot override a flag; it logs a warning if it tries). The `-o`
options of a remount are added after the ones in effect: options given at
mount time stay unless the remount overrides them. A plain SIGHUP re-reads
the config file with the options in effect.

| Settings | On reload |
|----------|-----------|
| `log_level`, `lock_wait`, `freshness_timeout` | applied at once |
| `lock_lease_timeout`, `heartbeat_interval` | applied at once, with a warning: every node relies on them, so change them to the same value on all nodes (and in the config file or fstab, or a restart brings the old value back). Until all agree, a voter refuses lock requests and renewals asking for a longer lease than its own, and nodes with different heartbeats may call each other down. Lowering the lease is safe in any order |
| everything else (`cluster_name`, `node_id`, `mode`, `listen`, `peers`, `backing_dir`, `mountpoint`, `state_dir`, `block_size`, `allow_other`, `allow_root`, `debug`, `foreground`) | refused: these are the node's identity or fixed when the kernel mount is made (`debug` also turns on the FUSE request log and the foreground; `foreground` is whether the process daemonized). Unmount and mount again to change them |

All or nothing: if any refused setting differs, the daemon logs an error
naming each one and changes nothing; otherwise it logs every change (`old
-> new`), or that nothing changed. The outcome is only in the daemon's
log (syslog or the journal; stderr in the foreground). `mount -o remount`
exits 0 once the daemon has the request, and fails only if it cannot
find or signal the daemon, or the options do not parse. Kernel mount
flags (`nosuid`, `noexec`, ...) are not changed by a remount.

How it works: a signal carries no options, so the helper writes them to
`<state_dir>/remount.json` (atomically, by rename) and sends SIGHUP. It
finds the daemon by the mountpoint, through the registration every mount
writes when it starts serving: `/run/syncfs/mount-HASH.json` for root
(`$XDG_RUNTIME_DIR/syncfs`, or `/run/user/UID/syncfs`, or
`~/.local/state/syncfs/run` for a user), holding its pid and state
directory. Every daemon also writes `<state_dir>/syncfs.pid`; both are
removed on exit. util-linux runs `mount.syncfs` with the fstab entry's
options, or, for a mount with no fstab entry, `mount.fuse.syncfs` with
the kernel's options for the mount (`rw,nosuid,...,user_id=0,...`), then
`remount`, then the new options (util-linux 2.38 in Debian 12 can put
`remount` between the per-mount options and FUSE's:
`rw,nosuid,nodev,relatime,remount,user_id=0,...,NEW`); the helper drops
the options that only describe the mount as the kernel shows it, and
FUSE's own (`user_id=` ...), wherever they are. It never
calls mount(2) for a remount: the kernel's FUSE mount accepts no changes
(`mount -i -o remount`, which skips the helper, fails with "No changes
allowed in reconfigure"). `mount -f -o remount` checks the options and
signals nothing.

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
| `stats`       | per-operation call and error counts, bytes read/written, open files/dirs, inodes, conflict copies |
| `peers`       | cluster size, quorum, reachable nodes, quorum verdict, every peer's live state  |
| `locks`       | locks this node holds, the votes it has given, lease settings, counters          |
| `replication` | disk id, dirty files and blocks per peer, journal position and lag, open sessions, crash window and suspects, freshness counters, the voter version table, `in_sync` |
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
- **Only the disk marker is stored in the backing directory.** Cluster
  mounts keep `<backing_dir>/.syncfs/disk` (see [Disk id](#disk-id)), hidden
  by the virtual directory. Any other real `.syncfs` entry in the backing
  directory is shadowed: hidden from the mount and untouchable through it
  (but left intact on disk). syncfs logs a warning at mount and `node`
  reports `shadowed_backing_entry: true`. Rename it in the backing directory
  to reach it through the mount.
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

Full mesh: one main TCP connection per pair of nodes, opened by the node
with the lower `node_id` and accepted by the higher one. Locks, heartbeats
and other control messages between two nodes use that one connection in
both directions, so they arrive in order. Two data nodes (mounts) also open
a **bulk lane**: a second connection, dialed the same way right after the
main one is up (its `Hello` says `lane: Bulk`), that carries journal
entries, blocks and hash replies, so a large transfer never delays a lock
or a heartbeat. The lane belongs to the main connection (same
incarnations) and fails with it: either one closing closes both, and the
peer reconnects both. Until the lane is up, bulk messages queue behind
control messages on the main connection; receivers cope with bulk messages
of one session arriving over both (journal entries are ordered by sequence
number, `BlockDone` says how many blocks precede it). `.syncfs/peers`
shows `bulk_lane` per peer.

A 99-node cluster has 4851 main connections, 98 per node. Each connection
has a reader and a writer thread, and each lower-id node has a dialer
thread per higher peer, so a daemon runs at most about 250 small threads
(a mount two more per data peer, for the bulk lane). That is fine at this size, so there is
no gossip.

### Wire format

Every connection starts with an 8-byte preamble in each direction:
`SYNCFS` and the protocol version as a big-endian u16 (currently 6). The
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

The message set already names what later steps need; messages of later
steps are logged and ignored.

| Messages | Used by |
|----------|---------|
| `Hello`, `Welcome`, `Reject` | handshake (step 4) |
| `Heartbeat {seq, up}`, `Goodbye {reason}` | liveness (step 4) |
| `LockRequest`, `LockGrant`, `LockDeny`, `LockConfirm`, `LockConfirmed`, `LockRenew`, `LockRenewed`, `LockRelease` | write locks (step 5) |
| `VersionHolders` | file versions (step 7): nodes that hold a version |
| `JournalEntry`, `JournalAck`, `BlockPull`, `BlockData`, `BlockDone`, `HashRequest`, `HashReply` | replication (step 7) |
| `Positions`, `CatchUpRequest`, `CatchUpDone` | forwarding journal entries of other origins |
| `LockFreshQuery`, `LockFreshReply` | start-up grace probe (step 5) |

`Hello` and `Welcome` carry the node's role: whether it stores data (a
mount) and its disk id (0 for daemons). `Hello` also names the lane (main
or bulk).

### Handshake

1. The dialer sends `Hello`: its node id, `cluster_name`, the sorted node ids
   of its `peers`, a random incarnation number (new on every start) and its
   software version.
2. The acceptor refuses with `Reject {reason, detail}` and closes when:
   - `cluster_name` differs (`ClusterName`)
   - the dialer uses the acceptor's own id (`SameNodeId`)
   - the dialer's id is not in the acceptor's peers (`UnknownNode`): the
     acceptor has no address or slot for it
   - the dialer has the higher id (`WrongDirection`)
3. Otherwise the acceptor answers `Welcome`, with its id, incarnation and
   sorted node ids.
4. The dialer checks that the node that answered is the one it meant to
   reach at that address (otherwise `expected node N ..., node M answered`).

`cluster_name` is the cluster's identity. If the two nodes' sets of node ids
differ, the connection is still accepted, and both sides log a warning (at
most once a minute) and report it: the peer's ids appear as
`members_mismatch` on that peer and a sentence in the top-level `warnings`
list of `.syncfs/peers` and `status.json` (and the daemon's status file).
Each node keeps computing cluster size and quorum from its own peer list,
so mismatched lists should be fixed. Node ids are compared, not addresses,
because nodes may legitimately reach the same peer by different names or
addresses. Refused peers show as `rejected` with the reason; the refusal is
logged at warn level at most once a minute.

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
node: no backing directory, no mount, just membership and lock votes (it
counts towards every quorum but never takes a lock). It daemonizes unless `--foreground` is given. With
no `.syncfs` directory to read, it writes the same status as JSON to
`<state_dir>/status.json`. The file is replaced atomically on every
membership change and at least every 5 heartbeat intervals, and removed on
a clean exit. SIGINT and SIGTERM stop it; SIGHUP (`systemctl reload
syncfs-daemon`) reloads its configuration like a mount's, minus the
mount-only settings.

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
  and has a documented, stable wire format.
  Frame decoding is fuzzed: random bytes,
  mutated frames, and streams split at random points must never panic.

## Write locks

Every node has an in-memory lock table and votes on lock requests,
voter-only daemons included. A node may change a file only while it holds
that file's lock, which needs the votes of a quorum (a strict majority of
all configured nodes). Reads never lock.

### What is locked

The lock key is the file's **path** relative to the mount root, not an
inode number (inode numbers differ between nodes). A key covers everything
below it: a lock on `a/b` conflicts with `a/b`, with `a/b/c` and with `a`,
but not with `a/bc` or `a/x`. Locks of the same node never conflict with
each other: the node serializes its own operations locally.

| Operation | Lock |
|-----------|------|
| open for writing (including `O_CREAT`, `O_TRUNC`) | the file's path, until the handle is closed |
| write, truncate, chmod, chown, utimens, xattr, fallocate on an open writable file | checks that the lock is still valid; re-acquires it if the lease lapsed |
| the same without a writable handle (`truncate(1)`, `chmod`, `setfattr`) | the path, for that one operation |
| create, mkdir, mknod, symlink, link | the new path, for the operation |
| unlink, rmdir | the path, for the operation (so a file open for writing elsewhere cannot be removed) |
| rename | source and destination, for the operation, in a fixed order. If another node holds a lock on either path, inside the source's subtree, or on an ancestor, the rename fails at once with `EBUSY` (it does not wait for `lock_wait`, and the other node's lock is never moved). This node's own write locks under the source are re-taken under the new path |

Consequences of path keys, by design:

- Renaming a file or directory that another node has locked (or that
  contains a locked file), or renaming onto such a path, fails with
  `EBUSY` immediately. Only a held lease counts: a rename that races with
  another node's *attempt* is decided by ballots and retried within
  `lock_wait` as usual.
- Removing a directory waits for (or, after `lock_wait`, fails on) every
  lock below it anywhere in the cluster.
- A file this node renames while it is open keeps its lock, moved to the
  new path.
- A file deleted while open needs no lock for further writes (it is no
  longer shared); its old path stays locked until it is closed, so another
  node cannot create a new file there in the meantime.
- A file whose backing path is longer than `PATH_MAX` cannot be named by
  the kernel; changes to it lock the root (`/`), which is safe but blocks
  every other node's writes for the duration.
- Hard links are locked per path: two nodes writing the same inode through
  different names are not serialized. This is a known limitation.

### Errors

| Situation | Result |
|-----------|--------|
| No quorum reachable | `EROFS` at once (reads keep working) |
| Lock held by another node | wait up to `lock_wait` (default 10 s), then `EBUSY`; `lock_wait: 0` fails at once |
| Rename involving a path another node holds a lock on | `EBUSY` at once |
| The previous holder's changes not applied here within `freshness_timeout` although it is reachable (see [Lock handoff](#lock-handoff)) | `EBUSY` |
| Lease lapsed (quorum lost while holding) | writes try to re-acquire; `EROFS` if that fails |
| Voters still in their start-up grace period (below), or not answering, until `lock_wait` | `EROFS` |

Waits are bounded by `lock_wait`, so two nodes waiting for each other's
locks (say, each renaming onto a path the other has open) cannot deadlock:
one of them gets `EBUSY`.

### Algorithm

A lock is a lease granted by a quorum, acquired in two phases.

- **Ballots.** Every acquisition gets a ballot `(counter, node id)`, where
  `counter` is a Lamport clock (one more than the largest counter this node
  has seen). Lower ballots are older and win; the node id breaks ties.
  Retries of the same acquisition keep their ballot.
- **Phase 1, request.** The requester sends `LockRequest(key, ballot)` to
  every node, itself included. A voter refuses with `Held` if another node
  has a confirmed lease on a conflicting key, and with `Older` if another
  node has a provisional grant with an older ballot. Otherwise it removes
  any younger conflicting provisional grants (telling their requesters
  `Preempted`) and records a provisional grant for this ballot, which
  expires after a few request timeouts.
- **Phase 2, confirm.** Once a quorum granted, the requester notes the time
  `t` and sends `LockConfirm` to those voters (and to any that grant
  later). A voter confirms only if it still has the provisional grant for
  exactly this ballot; the lease then runs `lock_lease_timeout` from when
  the confirm arrived. With a quorum of confirmations the requester holds
  the lock **until `t + lease - margin`**, where `margin` is a tenth of the
  lease (at least 50 ms).
- **Failure.** If a quorum becomes impossible (refusals, unreachable
  nodes) or the attempt times out, the requester sends `LockRelease` for
  the ballot to everyone, backs off (20 ms doubling to 500 ms, jittered)
  and retries with the same ballot until `lock_wait` is used up.
- **Renewal.** The holder sends `LockRenew` every third of a lease. A voter
  that still has the confirmed lease for that ballot extends it from
  receipt. With a quorum of answers the holder's validity becomes `t' +
  lease - margin`, where `t'` is when this round was sent. Without one the
  validity simply runs out; the holder considers the lock invalid from then
  on and writes have to re-acquire it.
- **Release.** Closing the last writable handle sends `LockRelease` to
  every node.
- **Restarts.** A voter grants nothing during its first lease after
  starting (the *start-up grace*), because it has forgotten any lease it
  gave before a restart. One restarted node does not block anyone: the
  others still form a quorum.
- **Cold start.** The grace is skipped when no live lease can depend on
  what this node forgot. While in its grace, a node asks every other node,
  once per heartbeat, whether it is *fresh* for it: it has acquired no lock
  since its own start, and no attempt it has in progress counts a vote
  from the asking node. When every other member is reachable and all
  answer fresh in the same round, the grace ends (`grace_skipped: true` in
  `.syncfs/locks`).

  Why this is safe: a lease this node granted before restarting only
  matters if its holder still runs and believes it holds the lock. That
  holder acquired the lock in its current run, so it is not fresh; if it
  restarted since, it holds nothing. An attempt still in progress could
  yet succeed with the old vote, so it counts too; the asking node, being
  in its grace, cannot have voted since restarting, so any vote from it in
  an attempt is an old one. Attempts started after the answer reach only
  the new incarnation, which remembers what it grants. Every member must
  answer, because an unreachable node could be a live holder.

  Effect: after a cold start of the whole cluster, nodes skip the grace
  within a heartbeat or two and writes work at once. A node restarting in
  a cluster where some node has taken a lock since starting keeps its full
  grace (the others form the quorum meanwhile). Once a lock has been taken,
  nodes still probing keep their grace too.

**Safety (at most one holder of conflicting keys).** A voter holds at most
one confirmed lease among conflicting keys of different nodes, and it
confirms only an existing provisional grant, of which it also holds at most
one per conflict set. Two holders would need two confirm quorums, which
share a voter; that voter would have confirmed both while both leases were
running, which it never does. Time: a holder's validity ends at `t + lease -
margin`, and every voter's lease ended no earlier than `t + lease` (it
started on arrival, after `t`), so the holder stops before any voter could
grant the key to someone else, provided clocks run at about the same rate
(the margin absorbs drift and a write that began just before the deadline).
A restarted voter's lost memory is covered by its one-lease grace, or by
the cold-start probe (above).

**No deadlock or livelock.** An attempt with the oldest ballot among the
competitors is never refused `Older` and never preempted; it can only be
refused `Held`, by a lease that ends (released or expired). Counters only
grow, so a node only ever sees finitely many ballots older than a waiting
one: every waiter eventually becomes the oldest and wins. Younger attempts
that lose release their votes at once, so nothing waits on a loser.

**Model check.** `src/lock/model.rs` explores every interleaving of the real
voter and requester state machines for 3 voters and 2 requesters (equal
and nested keys, with and without arbitrary attempt timeouts), with FIFO channels per node pair as TCP provides. It checks
that no two nodes ever hold conflicting locks and no voter confirms two
conflicting leases (about 8 million states in total), and that without timeouts
every run ends with nobody stuck mid-attempt and the oldest ballot holding
the lock. Lease timing is outside the model; it is covered by the argument
above and by the expiry tests.

The model also covers the start-up grace and the cold-start probe: one
node may restart (losing its voter table, its requester and its
connections; leases never expire, the worst case), and a restarted voter
grants nothing until the probe succeeds. Results, all with no two holders:
a voter-only node restarting, about 4.9 million states; a requester's node
restarting, about 0.76 million; a cold start with every voter in its
grace, about 23.6 million (opt-in test, 3 minutes optimized). Two
deliberately broken variants are run to show the check has teeth, and
both are caught with two holders: no grace after a restart, and a probe
that ignores attempts in progress.

### Timings

| Value | Default with `lock_lease_timeout: 30s`, `heartbeat_interval: 1s` |
|-------|------------------------------------------------|
| renewal | every lease / 3 (10 s) |
| safety margin | lease / 10, at least 50 ms (3 s) |
| one attempt's timeout | 2 × request timeout; request timeout = 2 × heartbeat, 200 ms to 2 s (4 s) |
| provisional grant lifetime | 4 × request timeout (8 s) |
| voter start-up grace | one lease (30 s) |

All nodes should use the same `lock_lease_timeout`. A voter refuses to
confirm a lease longer than its own (`LockDeny Lease`, logged), since its
start-up grace would not cover it.

A reload may change `lock_lease_timeout` and `heartbeat_interval` while
locks are held. Each attempt fixes the lease it asks for when it starts,
and each renewal round extends a lease only by the lease that round asked
for, so a holder never counts on a longer lease than the voters granted.
Held leases keep their deadlines, the renewer reschedules at once, and the
derived values above (request timeout, provisional grant lifetime) follow
the new heartbeat. Open connections get the new down timeout (3 ×
heartbeat) without reconnecting.

## Write path

On a cluster node every change goes through, in this order:

1. **Lock**: the file's write lock (taken at open, see above) or a lock
   for just the operation.
2. **Quorum**: the lock is a quorum lease; without one the change fails
   with `EROFS` before anything is touched.
3. **Local write** to the backing directory.
4. **Record for replication**, while the lock is still held (see
   [Replication](#replication)):
   - writes and `fallocate`: the blocks (`block_size`) they touch are
     marked dirty for every data peer;
   - truncate: marks past the new size are dropped;
   - chmod, chown, utimens on a file: sent with the session's entry;
     on directories, symlinks and special files: a journal entry at once;
   - xattrs, create, link, unlink, rmdir, rename: a journal entry each.

   A write that failed half way is still recorded (some bytes may have
   changed); a change refused by the lock or quorum records nothing.

## Replication

Mount nodes replicate to every other mount node; voter-only daemons store
no data and only keep the version table. Standalone nodes replicate
nothing. All state lives in a [redb](https://github.com/cberner/redb) store,
`<state_dir>/store.redb`.

### File identity

Every file and directory has a 128-bit id that stays with it across
renames (the backing inode number is not used: it can be reused). Files
created through syncfs get a random id that travels in the `Create` entry;
files that were already in the backing directory get an id derived from
their path, so identical pre-existing trees on two nodes agree without
talking. Files that were already hard-linked are found by a scan of the
backing directory on the first start with a new store and get the id of
their first path (sorted bytewise), so all their links stay one file. The store maps ids to paths (all of a hard-linked file's names)
and to the file's version; the map is in memory too, which costs memory
per file.

### Versions

A version is `(seq, node, disk)`, ordered by `seq` and then node and disk.

- **One bump per lock session.** A new lock session on a regular file
  writes version `max(local, every granting voter's latest and committed) +
  1`. The holder announces it in `LockConfirm`; voters record it as
  *pending*. Re-acquiring a lapsed lock continues the session and keeps
  its version.
- **Release.** `LockRelease` carries the outcome: written (the writer now
  holds the version) or not (voters go back to the previous entry).
- **Committed.** When a peer acknowledges the session's final journal
  entry, it holds the version; the writer broadcasts `VersionHolders` and a
  voter marks the version committed once its holders are a majority of
  all nodes (daemons count in the denominator but never hold data).
- **Persisted on every voter** (daemons included), and grants carry the
  voter's entry for the file and the writer's disk id.

The model check `src/repl/model.rs` explores every interleaving of
sessions (with any majority granting, any set of nodes reachable, releases
and holder announcements reaching any subset of voters, and crashes) and
checks that every written version is newer than every committed one and
that no session writes on top of a copy older than a committed version. A
variant whose fallback only asks the holders the voters know of fails it.

### Tombstones

Removing the last link of a regular file is a lock session like a write:
it gets a version, the `Remove` entry carries it (and the lineage), and
the file's record stays in the index as a *tombstone* for 7 days.

- A peer applies the remove only if its copy is not newer (a write made
  after a freshness fallback wins: the file comes back everywhere). A
  local change the remove's lineage lacks is kept as a conflict copy
  first.
- Freshness knows removes: a node with a stale copy that opens the file
  for writing learns from the remover that it is gone, removes its copy
  and gets `ENOENT`.
- An entry for a file this node holds the lock of waits until its session
  ends.
- Applying a peer's remove or rename drops the kernel's cached directory
  entries, so the next open looks the path up again.

### Lineage and conflict copies

Every copy remembers the last 16 versions it went through (its
*lineage*), and `Data` and `Attr` entries and hash replies carry it. Taking
a peer's copy takes its lineage; a local session adds the version it
replaces.

- **Changed blocks need the same base.** A copy takes an incoming
  version's changed blocks only if it is that version or one in its
  lineage. Otherwise (the writer started from another copy, e.g. after a
  freshness fallback) the whole file is compared by block hashes.
- **Conflict copies.** If the local copy holds a version this node wrote
  (on this disk) that the incoming copy's lineage lacks, the incoming
  writer worked without this node's change. Before it is replaced, the
  local copy is kept as `name.syncfs-conflict-<node>-<UTC time>` (for
  example `report.txt.syncfs-conflict-1-20261117T081500Z`), a new file
  that replicates like any other, and a warning is logged. The original
  name gets the newer copy. Only the node that made the change keeps a
  copy, so there is one per lost change. Counted in `conflict_copies` in
  `.syncfs/stats` and in the replication counters.

### Freshness

Before a new lock session on a file takes writes (at open for writing, or
for the single operation), the node brings its copy up to the newest
version the granting voters report:

1. If its copy is at least that version, nothing happens. Neither does a
   version that only this node announced, on this disk, and never wrote
   (it records the version durably before the first write; e.g. it
   stopped between the lock and the write).
2. Otherwise it compares block hashes with a node that has the version
   (the writer while pending, else the holders) and pulls the blocks that
   differ, then takes the size, attributes, extended attributes (all of
   them: names missing there are removed here) and version.
3. If no such node answers, `open()` (or the operation) blocks for up to
   `freshness_timeout` (default 30 s), retrying.
4. Then it falls back: it catches up from the newest copy any reachable
   node has (the holders of the newest committed version first). If its
   copy is still older than the newest committed version the voters
   report, the session refuses to write (`EBUSY`): a committed version is
   never overwritten from an older copy. Otherwise it continues; the
   writes of the unreachable node are kept as conflict copies when it
   rejoins (step 8).

Reads take no lock and are served from the local copy, which may lag by
the replication delay.

### Lock handoff

Versions order changes to one file; namespace changes (create, unlink,
rename, mkdir) travel in each origin's journal, which applies
asynchronously. Without more, a node could take a lock right after
another node released it and act on a namespace that does not show the
previous holder's change yet (e.g. create a file the other node just
created, giving two files at one path). So:

- `LockRelease` carries the releasing node's journal head (its last
  entry). Each voter keeps, per lock key, the newest head each node
  released it with (at most 65,536 keys; the oldest are folded into one
  floor that applies to every key).
- `LockGrant` carries the heads recorded for keys that conflict with the
  requested one (the key itself, its ancestors and its descendants).
- After acquiring, the new holder waits until it has applied each of
  those origins' journals up to the head, for at most `freshness_timeout`
  (`handoff_waits` in `status.json` counts the waits).
- If the entries are still missing after `freshness_timeout` although the
  origin, or a connected peer that has them, is reachable, the node gives
  the lock back and the operation fails with `EBUSY` (a warning in the
  log; `handoff_refused` in the lock counters). The application can retry.
- If nobody connected has them (the previous holder is down or cut off),
  it goes on without them at once, as the freshness check's fallback does
  (`handoff_gaps`).
- A create that finds the name taken in the meantime (a peer's create
  applied during the wait) opens the existing file instead, unless the
  application asked for `O_EXCL`.
- An open, truncate or unlink whose file the wait removed or moved away
  from the path (e.g. a peer renamed another file onto it) fails with
  `ESTALE`; the kernel then looks the path up again and retries once, so
  the application opens or removes the file that is there now.
- The same holds without a wait: the kernel may still have a name cached
  for a moment after a peer's rename applied here (its cache is
  invalidated asynchronously). The first open of a file replication
  renamed (or replaced), and the first unlink of a name it renamed a file
  away from, fail with `ESTALE` until the kernel looks the name up again,
  so the retry reaches whatever is at the name now, not the renamed file.
  This costs at most one extra lookup per renamed file.
- Unlinking a file's last link does not fetch its newest content first:
  the tombstone lists the newest version the lock knows of in its lineage,
  so peers that wrote it don't keep a conflict copy. If the unlink fails,
  nothing is recorded and the local copy keeps its version.

The wait can stall behind entries this node defers because it holds a
lock on their file itself; it then ends at `freshness_timeout` with
`EBUSY`.

### Journal and dirty blocks

Each node keeps an ordered journal of its own changes:

| Entry | Carries |
|-------|---------|
| `Create` | id, path, kind, mode/owner/times, device number, symlink target |
| `Link`, `Remove`, `Rename` | id, paths, rename flags |
| `Attr` | id, path, version, mode/owner/times |
| `Xattr` | id, path, name, value (or removal) |
| `Data` | id, path, version, size, mode/owner/times |

A lock session's data changes become one `Data` entry when the session
ends (last close or the end of the operation); a session open for longer
sends an interim `Data` entry every second. Dirty blocks are tracked per
peer, per file and per version: a peer pulls the blocks it is missing
(`BlockPull`), each block with its BLAKE3 hash, checked on arrival.
Blocks in flight are cleared when the peer acknowledges an entry and
marked again if the connection drops or the peer starts over.

- **Order.** Entries go out only once durable in the store, in order, at
  most 256 unacknowledged per peer, resent from the last acknowledgement
  after a reconnect or 10 s of silence. Each entry lists the entries of
  other origins its node had applied when it was made; a receiver applies
  each origin's entries in order and waits for those dependencies (so a
  write to a file created elsewhere never arrives before the create).
- **Apply.** The receiver applies the entry to its backing directory
  (paths from peers are checked and resolved beneath the backing directory
  without following symlinks), flushes the store and only then
  acknowledges. A `Data` entry pulls the dirty blocks, sets the size,
  attributes and version, and drops the kernel's cached attributes and
  pages of the file. An entry older than the local copy is skipped and the
  origin told to drop its marks. A path that now names another file (by
  id) is left alone.
- **Trim.** An entry is dropped once every data peer has acknowledged it.
- **Forwarding.** Nothing waits on a peer that is down. Every node keeps
  the entries of other origins that it applied (the `relay` table) until
  every data node besides the origin has applied them; nodes tell each
  other what they have applied (`Positions`, on each connection, on a
  change at most once a second, and every 5 s). A node that cannot reach
  an origin asks a connected data peer that is further along
  (`CatchUpRequest`); the peer answers with up to 256 entries per origin,
  each with its origin's sequence number and dependencies, then
  `CatchUpDone`. Copies are deduplicated by (origin, sequence): the
  origin's copy is preferred, a copy whose sender went away is dropped
  (the origin resends on its next connection; another peer can forward
  it), and each origin's entries still apply in order and after their
  dependencies. A forwarded `Data` entry copies the forwarder's content by
  block hashes (the forwarder applied the entry, so it has at least that
  version). The origin drops its dirty marks for a peer when the peer
  acknowledges the session's final entry, however the peer got it; an
  interim entry's acknowledgement does not clear them, since later writes
  of the session may still be marked. Requests to a peer fail as soon as
  its connection drops.
- **Bulk lane.** Journal entries, blocks and hash replies go over the
  bulk lane to the peer (see [Topology](#topology)).

### Rejoin

Local changes (writes, opens for writing, namespace changes) wait until
the node has applied every origin's entries as far as each connected data
peer reports applied (`Positions`, sent on every new connection), so a
node that was away does not change a namespace it has not caught up with:

- after a start, for at most `freshness_timeout` (or until a node whose
  other nodes are all voter daemons sees them connected);
- after a new connection to a peer, for at most 2 s (or
  `freshness_timeout` if shorter);
- after this process did not run for longer than the peers' down timeout
  (3 heartbeats: stopped, or the machine suspended), for at most 2 s,
  until positions newer than the pause arrive.

A peer leaving costs nothing. Per-file freshness still applies after the
wait. `rejoining` in `.syncfs/replication` is true while changes wait.

### Crash window

The store is flushed in batches every 100 ms (each flush is one durable
redb commit). Before a session's first change to a file, the file enters a
persisted *window* (one synchronous flush); it leaves after its session
ended and the entry was flushed. A clean stop records `clean`. After an
unclean stop, every file still in the window is a *suspect*: before it
takes writes (and in the background once a data peer is connected), its
block hashes are compared with every connected data peer, the blocks that
differ are marked dirty and a `Data` entry is sent. With no peer within
`freshness_timeout`, every block is marked.

Namespace changes of the crash window (a create, rename or remove on the
disk whose entry was not flushed) are found by comparing trees: after an
unclean stop the rejoin gate also waits until the node has listed a data
peer's tree at a moment when both had applied the same entries and the
peer had all of this node's. A path only here is sent as a new file
(`Create`, and its data); a path only on the peer is restored from it, so
nothing is lost (a lost rename leaves both names, a lost remove brings the
file back). Counted in `reconcile_sent` and `reconcile_restored`. If no
such moment comes within `freshness_timeout`, the comparison is skipped
with a warning.

### Disk id

A cluster mount keeps `<backing_dir>/.syncfs/disk` with the cluster name,
node id and a random 64-bit disk id, created on first start. A marker for
another node or cluster stops the mount. The disk id goes into versions and
the handshake.

### Rebuild

A node never treats its local data as the truth when:

- its marker is missing although its store ran as a mount (the disk was
  lost or replaced),
- its backing directory is empty although its store knows files, or
- its store ran as a voter-only daemon (restarted with a `backing_dir`),
  or
- it is a new member: a new store and an empty backing directory (a new
  node with files of its own brings them in as before).

It takes a new disk id (the marker says `state = rebuilding` until done,
so an interrupted rebuild resumes) and rebuilds from a data peer that is
not rebuilding itself:

1. It lists that peer's directory tree (`TreeRequest`, 1024 nodes per
   reply) and makes it locally: directories, symlinks, special files and
   hard links as they are, regular files as sparse files of the right
   size, marked missing. Local files the peer does not have move to
   `<backing_dir>/.syncfs/orphans/<UTC time>/` (hidden from the mount).
   The mount waits up to `freshness_timeout` for this, so it shows the
   whole tree from the start (a new member mounts at once: its peers may
   still be starting). Entries from peers wait meanwhile; the
   positions the peer had applied when it listed count as applied.
2. Missing files are copied in the background, four at a time, spread
   over the connected data peers, block by block with BLAKE3 hashes. A
   file opened before then is copied first (`EIO` if no peer has it).
3. Writes fail with `EROFS` until every missing file is copied; then the
   node is a normal writer. A rebuilding node answers `Missing` to hash
   requests for files it lacks, serves no blocks of them and forwards no
   entries.

If every peer is connected and every data peer is rebuilding too, nobody
has more: the rebuild ends with what is there.

A version that only a lost disk had does not block other writers: when
every node that held a committed version answers without it, freshness
continues from the newest copy there is.

Progress is in `rebuild` of `.syncfs/replication`: `active`, `cause`
(`disk_lost`, `emptied`, `daemon_upgrade`, `new_member`, `resume`), `source`,
`files_total`, `files_done`, `bytes_total`, `bytes_done`, `eta_secs`,
`orphans`.

### Status

`.syncfs/replication` on a cluster mount shows: `disk`, `block_size`,
`files_indexed`, `dirty_files`, `dirty_blocks` (most behind peer),
`dirty_bytes_max`, `blocks_in_flight`, `journal_last`, `journal_durable`,
`journal_queue`, `open_sessions`, `sessions` (up to 20: file id, path,
version, ms since the last interim entry), `window`, `suspects`, `unclean_start`,
`in_sync` (nothing dirty, unacknowledged or open), `freshness_timeout`,
`rejoining`, `rebuild` (see Rebuild), `store_commits`, `store_failures`, `versions` (this node's voter table:
`files`, `pending`, `latest_committed`), per peer `stores_data`, `disk`,
`journal_unacked`, `dirty_blocks`, `applied_from`, and `counters`
(`entries_made`, `entries_applied`, `entries_skipped`, `apply_errors`,
`blocks_sent`, `blocks_received`, `bytes_sent`, `bytes_received`,
`fresh_checks`, `fresh_pulls`, `fresh_waits`, `fresh_fallbacks`,
`suspects_resolved`, `entries_relayed` (applied from a forwarder),
`entries_forwarded` (sent for another origin), `conflict_copies`,
`reconcile_sent`, `reconcile_restored`), and `relay_entries` (kept
for forwarding). Daemons show their version table in `status.json`.

### Limits

- After an unclean stop with no data peer to compare with in time,
  namespace changes of the crash window stay local (a warning is logged).
- When every node of a new cluster starts empty, each rebuilds until all
  peers are connected (writes fail with `EROFS` until then).
- Entries for a file this node has open for writing wait until that
  session ends, and so do that origin's later entries.
- A change made on a peer less than one store flush (100 ms) before a
  reconnect may still race the reconnected node's own change.
- Serving each 1024-node tree page lists the whole source tree again, so
  listing a very large tree costs time quadratic in its size.
- Renaming over an existing file and removing one of several hard links
  are not versioned (no tombstone).
- Attribute and data caches are dropped after a peer's change; directory
  entries (a peer's create, rename or remove) show after the 1 s entry TTL.
- With two data nodes there is nobody to forward through: a node waits
  for the origin (reads stay local; locked writes use freshness).
- Paths longer than `PATH_MAX` (4096 bytes) cannot be named and are not
  replicated (a warning is logged); their locks fall back to the root lock.

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
  `open_write`/`release_write`. A standalone node uses `LocalOnly`; a
  cluster node uses `lock::ClusterHooks`, which also records changes for
  replication (`src/repl`). See `src/fs/hooks.rs`.
- **`.syncfs` inodes.** The directory is inode `2^63` and its files
  `2^63 + 1 + n`, far from the backing counter. See `src/fs/virt.rs`.

## Testing

- `cargo test` runs unit tests plus FUSE mount tests (as the current user;
  they skip with a message when `/dev/fuse` or `fusermount3` is missing).
  Mount and daemon-start waits allow 30 s (other waits 10 s);
  `SYNCFS_TEST_WAIT` (seconds) raises those bounds when set.
  Run the `mount`, `status` and `cluster` test binaries with `sudo` too for
  the root and multi-user cases; CI does both on x86_64 and arm64.
- `tests/net.rs` runs membership in process: handshake rejections, leave
  and rejoin, and a 25-node mesh. `tests/cluster.rs` runs 3- and 5-node
  clusters of real processes (mounts and daemons), kills, stops and
  restarts nodes, and requires every survivor's view to converge within
  8 s. `cargo test --release --test cluster -- --ignored` adds a 99-process
  cluster.
- Locks: unit tests of the voter table and the attempt state machine; an
  exhaustive model check of the protocol (`src/lock/model.rs`, run
  optimized in CI); `tests/locks.rs` runs lock managers in process (exclusion,
  waiting, renewal, lease expiry, quorum loss, a randomized 5-node stress
  run); `tests/cluster.rs` has multi-process lock tests on FUSE mounts
  (conflicting opens, 3 and 5 nodes taking turns, a killed holder, a
  partition giving `EROFS` while reads work, voter daemons forming the
  quorum).
- Reload: unit tests of what a reload may change (`config::diff`), of the
  helper's remount options and of the request file; `tests/locks.rs`
  changes lock timings and the heartbeat under a held lock;
  `tests/cluster.rs` remounts a cluster mount through the helper while a
  file is open and its lock held (the new `lock_wait` is live, the lock
  session and the file survive), checks that a refused change leaves the
  configuration alone and the helper still exits 0, and that SIGHUP
  re-reads the YAML on mounts and voters without unmounting.
- Replication: unit tests of the store, journal, dirty bitmaps, file index
  and backing access; the version model check (`src/repl/model.rs`, run
  optimized in CI); `tests/cluster.rs` checks that two mounts' backing trees
  converge after creates, writes, links, symlinks, chmod, xattrs, renames
  and removes, that only changed blocks travel, that a daemon keeps its
  version table across a restart, that a node that missed a write catches
  up when it locks the file (and falls back after `freshness_timeout`), and
  that a node killed while writing resends after restart, and that a
  change reaches a node through a third mount when its origin died, that a
  diverged change is kept as a conflict copy, that removes leave
  tombstones, that a restarted, reconnected or paused node catches up
  before it changes the namespace (and that those waits stay short), that
  a new member rebuilds from peers, that namespace changes lost with an unflushed store are
  reconciled (`SYNCFS_TEST_FLUSH_INTERVAL_MS` widens the crash window),
  and that a lost disk and a daemon upgraded to a mount are
  rebuilt from peers (`SYNCFS_TEST_REBUILD_DELAY_MS` slows the background
  copies so the test sees the rebuild running); an
  exhaustive model check of forwarding and dirty marks
  (`src/repl/relay_model.rs`: three nodes, a session with an interim
  entry, a dependent entry, link loss, a restart and the origin's death;
  it also shows that clearing marks on an interim acknowledgement, or a
  relay that keeps nothing, breaks it).
- Soak (`tests/soak.rs`): 3 mounts, and 5 nodes of which 2 are daemons
  (one upgraded to a mount mid-run), each running random creates, writes,
  read-modify-writes under the lock, reads, unlinks, renames and directory
  changes, while a fault-injecting TCP proxy on every link cuts or stalls
  links and partitions minorities, and nodes are killed, stopped, paused,
  restarted with a lost disk or a new store. After healing it checks that
  no two nodes held conflicting locks at once (from the lock audit log,
  `SYNCFS_TEST_LOCK_LOG`), that no committed write was lost or torn, that
  conflict copies only appear for a node that was cut off or restarted, and
  that all trees converge. The schedule comes from `SYNCFS_SOAK_SEED`;
  `SYNCFS_SOAK_SECS` sets the length (see the file for the other knobs).
- `scripts/container-test.sh bookworm|trixie [arm64]` builds and tests in a
  clean Debian container, natively or emulated through qemu-user/binfmt.
- Packages: `scripts/deb-test.sh` installs a `.deb` in a clean container
  of its suite and checks the files, the conffile, root fstab mounts of
  both types, a non-root `user` fstab mount and unmount, the units
  (`systemd-analyze verify`, and the template's command line mounting and
  unmounting on SIGTERM), and that remove keeps and purge deletes the
  conffile. `scripts/unit-test.sh SUITE DEB` boots systemd in a
  privileged container and starts and stops `syncfs-daemon` and
  `syncfs@data` (mounted, used, reloaded with `systemctl reload`,
  unmounted on stop), and checks that an instance without its config is
  skipped. `deb-test.sh` also reloads root mounts with `mount -o remount`,
  with and without an fstab entry. Both take an optional
  architecture to emulate (e.g. `arm64`); under qemu-user the setuid
  `mount` is not setuid, so the `user` mount check fails there. CI runs both natively for all four packages and smoke-tests the static
  binaries (a mount, and `--version` in an empty container).
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
5. **Quorum and lock table** (done): whole-file locks with quorum leases,
   voter-only daemons voting, `EROFS` without quorum, `.syncfs/locks`.
6. **Write path** (done): lock, quorum check, local write, mark dirty;
   `freshness_timeout` option.
7. **Async replication** (done): persisted dirty-block bitmaps and ordered
   metadata journal, peers pulling blocks, file versions, hash-based
   freshness check on lock acquisition with the timeout fallback, crash
   window.
8. **Rejoin and recovery** (done; see Rejoin, Lineage and conflict copies,
   Tombstones, Rebuild and Crash window). Planned as:
   - A node that rejoins with diverged, unreplicated changes keeps them as
     `name.syncfs-conflict-<node>-<timestamp>` instead of being
     overwritten; the count shows in `.syncfs/stats` and `status.json`.
   - **Backing-disk loss.** Every backing directory carries a marker file
     (cluster name, node id, disk id). If the marker is missing or the
     directory is empty at start, the node never treats its local data
     as the truth: it joins as a voter only, in rebuild mode. It rebuilds
     the directory tree and metadata from peers and mounts right away;
     reads of files not yet local are fetched on demand from a healthy
     peer and verified by checksum, while a background job fills in the
     rest (possibly from several peers at once). Partial loss uses the
     same per-file checksum comparison. Peers see the new disk id as a
     disk loss, not a normal rejoin, so the freshness check does not wait
     for that node's lost writes. The rebuilding node refuses writes
     (`EROFS`) until it has caught up, then becomes a normal writer (it
     takes its new disk id when the rebuild starts). Progress (files and bytes done, ETA) shows
     in `.syncfs/replication` and `status.json`.
   - **Upgrading a voter-only daemon to a mount** (restarting it with a
     `backing_dir`) takes the same rebuild path: mount at once, fetch on
     demand, fill in the background, `EROFS` until caught up. The only
     difference is that a daemon never had unreplicated writes, so there
     is nothing to wait for or keep as a conflict copy. Covered by the
     cluster test `daemon_upgraded_to_mount_rebuilds_from_peers`.
9. **Packaging** (done; see [Installing](#installing)): `.deb` for
   bookworm and trixie on amd64 and arm64 with the fstab helpers, a
   conffile and systemd units (daemon mode, and a template for mounts);
   static musl binaries for x86_64 and aarch64; built, test-installed
   (including root and `user` fstab mounts) and released by CI.
10. **Soak test**: randomized multi-node run with restarts and partitions.

## Issue tracking

Open work is scored and picked as described in [CONTRIBUTING.md](CONTRIBUTING.md):
every issue and enhancement gets `effort` and `likelihood` labels (lower
is better) and a short reason when it is filed. Work is taken in lowest
total first (ties: lower effort, then lower issue number). A bug found
while working on something else is filed as its own issue and left for
later.

## Building

```sh
cargo build --release
cargo test
```

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.
