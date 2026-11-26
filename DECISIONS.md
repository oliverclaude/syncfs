# Design decisions

This file is a log of design choices for syncfs, so people and agents
working on the code can see what was decided, which options were on
the table, and whether the choice is in `main` yet.

Add a new entry at the top (newest first). Same-day entries go in the
order the decisions were taken, newest first, when that order is
known. Each entry should have:

- the date in SAST (Africa/Johannesburg, UTC+2)
- the area
- a link to the issue or pull request, if there is one
- the options that were considered, when they were recorded
- what was chosen
- why, when a reason was recorded

Do not invent options or reasons. If a choice is not in `main` yet,
say so (`decided, not yet merged` or `decided, not yet implemented`).

## 2026-11-26 — Replication: apply Rename after a B4 sideline (#2)

**Issue:** [#2](https://github.com/oliverclaude/syncfs/issues/2)

After B4 relocates an incoming `Create` to a FileId conflict path, a
later journaled `Rename` still named the original `from`. Apply looked
up `index.get(from)`, returned `Ok(false)` when another FileId was
there, and `applied` still advanced, so the file stayed at the conflict
name on some nodes only. Following `path_of(id)` for the source is not
enough: moving the file onto a dest that another live FileId holds would
A1-bury that dest, while other nodes had kept it as a B4 conflict copy.
Options recorded then:

1. Rename follows `path_of(id)`; dest still A1-buried. The moved file
   reaches the dest path, but the dest FileId is tombstoned there and
   remains a conflict copy on nodes that sidelined it at `Create` —
   trees still differ.
2. Rename follows `path_of(id)`; if dest is a different live FileId,
   always B4-sideline it (FileId name and a journaled `Rename`) instead
   of burying. Converges the soak case. Changes rename-over: overwritten
   files become conflict copies instead of disappearing.
3. Rename follows `path_of(id)`; B4-sideline dest only when the source
   currently sits at a conflict-copy path (completing a diverted
   `Create`). Normal rename-over still A1-buries.

**Choice.** 3.

**Status.** Implemented on `bug/grokbot/#2`. Decided, not yet merged.

## 2026-11-26 — Replication: conflict-copy names when two file ids share a path (#2)

**Issue:** [#2](https://github.com/oliverclaude/syncfs/issues/2)

This supersedes the Version-based conflict-copy *name* from the earlier
26 Nov winner-rule entry. The winner rule itself is unchanged: the
greater `Version` still wins the path; an unversioned file loses to
any real `Version`; `FileId` is the last tie-break.

After that entry was implemented, soak runs showed a name split: an
unversioned local file treated as `0-0-0` produced `path.syncfs-conflict-0-0-0`
on one node and `path.syncfs-conflict-<node>-<seq>-<disk>` on a node
that already knew the version. Retrying a file `Create` until a version
was known also deadlocked apply on empty files that never get a
`Data`/`Attr`. Options recorded then:

1. Name B4 conflict copies `path.syncfs-conflict-<fileid-hex>` (the
   FileId is known at `Create`). Stop retrying a file `Create` only
   because its version is unknown.
2. Keep Version-based names, but never emit `0-0-0`, and do not Retry
   forever: Retry only while a `Data`/`Attr` is still ahead in that
   origin's inbox/journal; if no version will appear, use a FileId-based
   suffix; rename an existing `…-0-0-0` copy once a real version is
   recorded.
3. Journal the B4 sideline as a `Rename` (or `Create`) so peers apply
   the same name and the copy replicates, combined with 1 or 2.

A related hole in the same runs: directory `Create` `AlreadyExists` had
been changed to Retry and stalled later journal entries.

**Choice.** 1 plus 3, and Skip for directories. The name uses the full
16-byte lowercase FileId hex. A local loser is journaled as a `Rename`
to that path. Directory `Create` `AlreadyExists` is Skip again, as
before the two-FileId change, so `applied` advances.

**Upgrade.** Existing `path.syncfs-conflict-<node>-<seq>-<disk>` and
`path.syncfs-conflict-0-0-0` names are not rewritten. After upgrade,
new clashes use the FileId name, so a cluster may briefly have both
shapes until the old files are removed.

**Status.** Implemented on `bug/grokbot/#2`. Decided, not yet merged.

## 2026-11-26 — Replication: winner rule when two file ids share a path (#2)

**Issue:** [#2](https://github.com/oliverclaude/syncfs/issues/2)

The 26 Nov choices for the remaining #2 fixes had left the winner
rule open. Options recorded when implementing the two-FileId case:

1. The greater `Version` wins the path (`Version` order: sequence,
   then node, then disk). The loser is kept as a conflict copy that
   is a normal replicating file and reaches every node.
2. The greater `FileId` wins.
3. The lower creator node id wins, with `FileId` as the last tie-break.

**Choice.** 1. An existing rule (`diverged` / incoming-wins for the
same FileId) does not apply to two different FileIds. The chosen
order is the one `Version` already uses elsewhere. A missing version
loses to any real version; `FileId` breaks a remaining tie.

**Status of the 26 Nov #2 choices.** Stalls (apply-side `Remove` when a
peer's Create or Rename drops a last link), same version with different
bytes (verify the whole file on the session-final `Data` entry), and
this winner rule are implemented on `bug/grokbot/#2`. Decided, not
yet merged.

## 2026-11-26 — Replication: remaining #2 fixes


**Issue:** [#2](https://github.com/oliverclaude/syncfs/issues/2)

Three choices, after origin-side tombstones still left stalls and
`in_sync` divergence in crash-and-restart runs.

**Stalls.** When a node applies a peer's `Create` or `Rename` and that
drops a file's last link, the receiving node also buries the old file
and journals a `Remove`. (The alternative was to leave the receiving
side as it is.)

**Same version, different bytes.** Verify the whole file once a write
session's final `Data` entry is applied, before recording the version.
(The alternatives were to check every copied change, or to re-run the
reconcile check after a partition heals.)

**Two file ids at one path after a partition.** Pick a winner and keep
the loser as a replicated conflict copy. (The alternative was to split
this into its own issue.) The rule for picking the winner is still to
be settled.

**Status.** Decided, not yet merged. The winner rule is still open.

## 2026-11-26 — #2 rescore

**Issue:** [#2](https://github.com/oliverclaude/syncfs/issues/2)

Rescored to effort 4, likelihood 3, total 7 (was effort 4, likelihood
2, total 6).

**Why.** After soak work on `bug/grokbot/#2`, origin-side tombstones
were in, but peer-applied `Create`/`Rename` still dropped a file
record silently (stalls in 3 of 12 partition+kill runs), and live
`in_sync`-true divergence remained in 3 of 12 (split file ids at one
path, and the same version with different bytes).

**Status.** Recorded on the issue. The issue is still open.

## 2026-11-25 — Replication: tombstone when a create or rename-over drops a last link (#2)

**Issue:** [#2](https://github.com/oliverclaude/syncfs/issues/2)

A follow-up to the stall investigation below. Options:

1. On the receiving node, skip a stuck change when a later inbox
   entry gives that path to a different file.
2. Add a protocol signal that the change was superseded.
3. At the source, journal a `Remove` / tombstone when a `Create` or
   a rename-over drops a file's last link.

**Choice.** 3 only, not 1 or 2. A remaining hard link is left alone
(only the last link is tombstoned). `RENAME_EXCHANGE` journals nothing
for the swap, because nothing is displaced.

**Status.** Decided, not yet merged. Implemented on `bug/grokbot/#2`.

## 2026-11-25 — Replication: stalls after option A (#2)

**Issue:** [#2](https://github.com/oliverclaude/syncfs/issues/2)

Option A (do not advance `applied` until the bytes are here) caused
journal stalls in 12 of 12 crash-and-restart soak runs. Options:

1. Find out why the bytes are missing on every connected node.
2. After a time limit, skip the change, mark the file as missing, and
   keep `in_sync` false until a later fetch works.
3. Merge only the unclean-start reconcile (E) and hold A.

**Choice.** 1.

**Finding.** The path was taken over by a different file id (`Create`
or a rename-over). The origin dropped the file record with no
tombstone or `Remove`, so it could no longer supply the bytes and
other nodes had no record telling them to stop waiting.

**Status.** The finding led to the source-tombstone choice above.
Not merged on its own.

## 2026-11-25 — Replication: apply path and unclean-start reconcile (#2)

**Issue:** [#2](https://github.com/oliverclaude/syncfs/issues/2)

When a file's bytes are copied and the origin no longer has them:

- **A.** Try the other connected peers, and do not mark the change
  applied until the bytes arrive.
- **B.** Try the other peers, then skip the change anyway (as in
  [PR #24](https://github.com/oliverclaude/syncfs/pull/24)).
- **C.** Skip the change, mark the file as missing, and keep
  `in_sync` false until a later fetch works.

After an unclean start, reconcile should fetch when:

- **D.** The peer has a newer version, or the same version and a
  different size.
- **E.** Also compare block hashes when the version and size match.

**Choice.** A plus E.

**Why.** A node must not claim a version whose content it does not
have. `in_sync` still means the node has no pending work; it does not
start meaning “content matches.”

**Status.** Decided, not yet merged. Implemented on `bug/grokbot/#2`.
Option A later caused the stalls recorded above.

## 2026-11-25 — PR #24 rejected

**Pull request:** [PR #24](https://github.com/oliverclaude/syncfs/pull/24)
(`bug/kommit/#2`, “repl: repair an empty file a restarted node still
claims”).

**Choice.** Rejected and closed without merging. The branch is kept.
Issue [#2](https://github.com/oliverclaude/syncfs/issues/2) stays open.

**Status.** Done. The later #2 apply-path choice (A, not B) is the
alternative to this PR's skip-if-missing behaviour.

## 2026-11-24 — Fault-soak CI job held (#14)

**Issue:** [#14](https://github.com/oliverclaude/syncfs/issues/14)

The issue asked for a short fault soak in CI. It noted the job would
be red until the open fault-run bugs were fixed, and that it could
start with `continue-on-error` or only with seeds that pass.

A try on `main` (8d9863e) found only 1 of 8 faulted `soak_three_mounts`
runs passed, a seed that passed once failed on repeat, and a failing
run takes ~160–190 s, which blows a 5-minute budget.

**Choice.** Option C: hold the job until [#2](https://github.com/oliverclaude/syncfs/issues/2),
[#3](https://github.com/oliverclaude/syncfs/issues/3) and
[#5](https://github.com/oliverclaude/syncfs/issues/5) are fixed, then
re-check which seeds pass reliably and add the short fault soak as a
blocking CI job. The issue stays open. (The lettered alternatives to
C were not written down on the issue.)

**Status.** Held. #5 is fixed; #2 and #3 are still open. The job is
not in CI yet.

## 2026-11-19 to 2026-11-22 — Process

How work is tracked and landed (also summarised in
[CONTRIBUTING.md](CONTRIBUTING.md) and the README's Issue tracking
section):

- Every issue and enhancement is tracked on GitHub, with full detail.
- Each one gets labels `effort:N` and `likelihood:M`, both 1 to 5,
  lower better, and a comment `Score: effort N, likelihood M, total X.
  Reason: …`.
- Work is picked by the lowest total, then lower effort, then lower
  issue number.
- A bug found while working on something else is filed as its own
  scored issue and is not fixed in that change.
- Work is done on a branch `bug/grokbot/#N`, merged `--no-ff` into
  `main` only after tests and CI pass, then the issue is closed with
  a comment. No force-push to `main`.
- At a design point, the owner chooses among two or more options
  before the code is changed.

**Status.** In use. Scoring and pick order are in CONTRIBUTING.md
(since 6da6154, 2026-11-20).

## 2026-11-21 — Releases

**Choice.** Ship `v0.2.0-pre` as a pre-release, with `.deb` packages
and musl binaries. Also publish dated nightly pre-releases tagged
`nightly-YYYYMMDD` (UTC date; package version `0.2.0~nightly.YYYYMMDD`).

**Status.** In main. Described in the README's Installing section.
The `Package` workflow publishes both.

## 2026-11-21 — Remount reloads config in place

**Issues:** [#21](https://github.com/oliverclaude/syncfs/issues/21)
(level 1, done), [#20](https://github.com/oliverclaude/syncfs/issues/20)
(level 2, later).

**Choice.** `mount -o remount` reloads the configuration in place
through the `mount.syncfs` helper, which writes
`<state_dir>/remount.json` and signals the daemon with SIGHUP
(`systemctl reload` does the same). Live-changeable settings are
applied; node-identity and mount-fixed settings are refused. The
outcome is reported only in the daemon's log, not by `mount`'s exit
code: `mount -o remount` exits 0 once the daemon has the request.

What a reload does (from the README, which matches the code):

- Applied at once: `log_level`, `lock_wait`, `freshness_timeout`.
- Applied at once, with a warning to change every node:
  `lock_lease_timeout`, `heartbeat_interval`.
- Refused (the node's identity, or fixed when the kernel mount is
  made): `cluster_name`, `node_id`, `mode`, `listen`, `peers`,
  `backing_dir`, `mountpoint`, `state_dir`, `block_size`,
  `allow_other`, `allow_root`, `debug`, `foreground`. If any
  refused setting differs, nothing is changed.

A full daemon restart that hands over the FUSE connection, so the
binary can be upgraded without unmounting, is a later enhancement
([#20](https://github.com/oliverclaude/syncfs/issues/20)).

**Status.** Level 1 is in main (edffd28 and follow-ups, #21). Level 2
is not implemented.

## 2026-11-19 — Lock handoff timeout

**Choice.** If the previous holder is down or cut off and nobody
connected has its journal entries, the new holder carries on without
them (`handoff_gaps`), as the freshness fallback does; diverged
changes are kept as a conflict copy when that node rejoins. If the
entries are still missing after `freshness_timeout` although the
holder, or a peer that has them, is reachable, the new holder gives
the lock back and the operation fails with `EBUSY` (`handoff_refused`).

**Status.** In main (7353834).

## 2026-11-19 — Fault-run behaviour

Decided together for the fault-soak failures, then filed as separate
issues.

**Killed mount hangs ([#3](https://github.com/oliverclaude/syncfs/issues/3),
option A).** A watchdog that does not hold the mount's FUSE handle
aborts the FUSE connection when the daemon dies, so the mount returns
`ENOTCONN` instead of hanging.

**Rename after a lock wait ([#4](https://github.com/oliverclaude/syncfs/issues/4),
option A).** A rename returns `ESTALE` when a different file sits at
the source name after the lock wait, so the kernel looks the name up
again.

**Handoff gap and the soak checker ([#5](https://github.com/oliverclaude/syncfs/issues/5),
option A).** The handoff gap stays as it is (availability first). The
soak checker excuses a stale locked read that follows a logged
handoff or freshness fallback.

**Status.** The rename `ESTALE` is in main (ae2c039, #4). The soak
excuse is in main (d5be362 and follow-ups, #5). The watchdog is
decided, not yet implemented; #3 is still open. The soak harness
already aborts a hung mount's FUSE connection after 30 s as a
stopgap, so the run fails instead of hanging.

## 2026-11-19 — Rename races return ESTALE

**Choice.** An open or unlink that finds its file renamed by another
node since the kernel's lookup returns `ESTALE`, so the kernel looks
the path up again and retries. A write that races a rename on another
node returns `ESTALE` too.

**Status.** In main (d163781 for open after the lock/handoff; dd4be27
for a name a peer has already renamed away; later ae2c039 for rename
after the lock wait, under #4 above).

## 2026-11-19 — Unlink lineage (option B)

**Choice.** Unlinking a file's last link does not fetch the file's
newest content. The delete's tombstone lists the newest version the
lock knows of in its lineage, so a peer that wrote that version does
not keep a conflict copy. If a different file is now at the path, the
unlink returns `ESTALE`. If the unlink fails, nothing is recorded and
the local copy keeps its version.

Options recorded at the time:

- **A.** Unlink runs the same freshness pull as open, then removes.
  Simpler, but unlink can then fail `EBUSY`, and it pulls data only
  to delete it.
- **B.** Put the voters' newest known version in the `Remove`
  lineage without fetching content.

**Status.** In main (d0809cd).

## 2026-11-17 — Disk loss

**Choice.** A marker file in the backing directory
(`<backing_dir>/.syncfs/disk`: cluster name, node id, disk id)
detects a lost or replaced disk. The node rebuilds the tree from a
peer, mounts immediately, fetches missing files on demand and fills
the rest in the background, and refuses writes (`EROFS`) until it is
fully caught up. The same path is used when a voter-only daemon is
restarted as a mount.

**Status.** In main. Described in the README under Rebuild.

## 2026-11-17 — Install

**Choice.** The `.deb` sets the binary, mount helpers, example
config, docs and systemd units up, but enables and starts nothing
until the user does (`systemctl enable --now` after editing the
config).

**Status.** In main. Described in the README under Installing.

## 2026-11-17 — Rejoin

**Choice.**

- A new empty node rebuilds from a peer.
- Paths missing after a crash are restored from a peer (tree
  comparison in the crash window).
- When a delete races a write, the write wins: a peer applies the
  remove only if its copy is not newer.
- Nodes hold writes until they have caught up after a start and
  after a reconnect.

**Status.** In main. Described in the README under Rejoin, Crash
window, Tombstones and Rebuild.

## 2026-11-17 — Replication

**Choice.**

- Reads take no lock and are served from the local copy, which may
  lag by the replication delay.
- Tombstones waited for step 8 (now done).
- Voter versions are persisted in the store flusher's batches
  (every 100 ms, or sooner when a flush is requested).
- Peers forward each other's journal changes, so a node that cannot
  reach the origin can still catch up.
- Bulk data (journal entries, blocks, hash replies) uses a separate
  TCP connection (the bulk lane) between two data nodes, so a large
  transfer does not delay a lock or a heartbeat.

**Status.** In main.

## 2026-11-17 — Versions

**Choice.** One version bump per lock session. The version is
committed once it has been replicated to a majority of the cluster
(voter-only daemons count in the denominator but never hold data).
Every voter, daemons included, persists a version table.

**Status.** In main. Described in the README under Versions.

## 2026-11-17 — Freshness

**Choice.** A lock acquirer first brings its copy up to the newest
version the granting voters report. If only unreachable nodes have
that version, it waits up to `freshness_timeout`, then continues from
the newest committed copy it can reach (holders of the newest
committed version first). If its copy is still older than the newest
committed version the voters report, the session refuses to write
(`EBUSY`). When the unreachable node rejoins, its diverged changes
are kept as a conflict copy.

Version and dirty-block state live in the sidecar store
`<state_dir>/store.redb`, keyed by file identity (a 128-bit id that
follows the file across renames). Dirty marks and the voter version
table are saved in the store flusher's batches. Missing blocks are
found by comparing BLAKE3 block hashes.

**Status.** In main. Described in the README under Freshness, File
identity, and Journal and dirty blocks.

## 2026-11-17 — Locks

**Choice.**

- `lock_wait` blocks until the wait is used up, then the operation
  fails with `EBUSY` (`lock_wait: 0` fails at once).
- Lock keys are paths relative to the mount root, not inode numbers.
  Renaming a locked path (or a path that contains a locked file, or
  renaming onto one) fails at once with `EBUSY`; it does not wait for
  `lock_wait`, and the other node's lock is never moved.
- A voter's start-up grace is skipped when all nodes start fresh
  (the cold-start probe: no live lease can depend on what this node
  forgot).
- For cluster identity, the handshake rejects a peer only when
  `cluster_name` differs. A mismatch of node-id lists is accepted
  with a warning in the log, `.syncfs/peers` and `status.json` (each
  node still computes quorum from its own peer list). Other handshake
  refusals (`SameNodeId`, `UnknownNode`, `WrongDirection`) are
  routing checks, not identity.

**Status.** In main. The identity warning is 2613de7 (protocol 3,
“identity option B”).
