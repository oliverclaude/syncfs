//! The sidecar store under `state_dir`: one redb database holding
//! everything replication must remember across restarts (file identities
//! and versions, dirty-block bitmaps, the journal, voter version tables).
//!
//! Writers are batched: components record which keys changed, and a
//! [`Flusher`] commits them together, every [`FLUSH_INTERVAL`] or at once
//! when asked ([`Flusher::flush_now`]). Each commit is durable when it
//! returns.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};

/// How often batched changes are committed.
pub const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Table {
    /// Single values: disk id, clean-shutdown flag, sequence counters.
    Meta,
    /// Voter version table: FileId -> version::Record.
    Versions,
    /// Local files: FileId -> FileRec.
    Files,
    /// Dirty bitmaps: FileId -> per-peer, per-version block ranges.
    Dirty,
    /// Files that may have changes not yet recorded durably.
    Window,
    /// This node's journal: seq (BE) -> entry.
    Journal,
    /// Per origin: last applied journal seq; per peer: last acked.
    Positions,
    /// Other origins' entries kept for forwarding: origin (BE u16), seq
    /// (BE) -> entry.
    Relay,
}

impl Table {
    fn def(self) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
        TableDefinition::new(match self {
            Table::Meta => "meta",
            Table::Versions => "versions",
            Table::Files => "files",
            Table::Dirty => "dirty",
            Table::Window => "window",
            Table::Journal => "journal",
            Table::Positions => "positions",
            Table::Relay => "relay",
        })
    }

    const ALL: [Table; 8] = [
        Table::Meta,
        Table::Versions,
        Table::Files,
        Table::Dirty,
        Table::Window,
        Table::Journal,
        Table::Positions,
        Table::Relay,
    ];
}

type OpenTable<'t> = redb::Table<'t, &'static [u8], &'static [u8]>;

/// One change: `None` deletes.
pub type Op = (Table, Vec<u8>, Option<Vec<u8>>);

pub fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    postcard::to_allocvec(v).expect("serializing to a Vec cannot fail")
}

pub fn decode<T: DeserializeOwned>(b: &[u8]) -> std::io::Result<T> {
    postcard::from_bytes(b).map_err(|e| std::io::Error::other(format!("corrupt store entry: {e}")))
}

fn io(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

pub struct Store {
    db: redb::Database,
    path: PathBuf,
    commits: AtomicU64,
}

impl Store {
    /// Opens (or creates) `dir/store.redb`.
    pub fn open(dir: &Path) -> std::io::Result<Store> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("store.redb");
        let db = redb::Database::create(&path).map_err(io)?;
        let tx = db.begin_write().map_err(io)?;
        for t in Table::ALL {
            tx.open_table(t.def()).map_err(io)?;
        }
        tx.commit().map_err(io)?;
        Ok(Store {
            db,
            path,
            commits: AtomicU64::new(0),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn commits(&self) -> u64 {
        self.commits.load(Ordering::Relaxed)
    }

    /// Every entry of `t`, in key order.
    pub fn load(&self, t: Table) -> std::io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tx = self.db.begin_read().map_err(io)?;
        let table = tx.open_table(t.def()).map_err(io)?;
        let mut out = Vec::new();
        for item in table.iter().map_err(io)? {
            let (k, v) = item.map_err(io)?;
            out.push((k.value().to_vec(), v.value().to_vec()));
        }
        Ok(out)
    }

    pub fn get(&self, t: Table, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let tx = self.db.begin_read().map_err(io)?;
        let table = tx.open_table(t.def()).map_err(io)?;
        Ok(table.get(key).map_err(io)?.map(|v| v.value().to_vec()))
    }

    /// Applies `ops` in one durable transaction.
    pub fn commit(&self, ops: &[Op]) -> std::io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let tx = self.db.begin_write().map_err(io)?;
        {
            let mut tables: Vec<(Table, OpenTable<'_>)> = Vec::new();
            for (t, k, v) in ops {
                let i = match tables.iter().position(|(x, _)| x == t) {
                    Some(i) => i,
                    None => {
                        tables.push((*t, tx.open_table(t.def()).map_err(io)?));
                        tables.len() - 1
                    }
                };
                let table = &mut tables[i].1;
                match v {
                    Some(v) => {
                        table.insert(k.as_slice(), v.as_slice()).map_err(io)?;
                    }
                    None => {
                        table.remove(k.as_slice()).map_err(io)?;
                    }
                }
            }
        }
        tx.commit().map_err(io)?;
        self.commits.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Changes a component made since it was last asked, and what to do once
/// they are durable.
#[derive(Default)]
pub struct Batch {
    pub ops: Vec<Op>,
    pub after: Vec<Box<dyn FnOnce() + Send>>,
}

impl From<Vec<Op>> for Batch {
    fn from(ops: Vec<Op>) -> Batch {
        Batch {
            ops,
            after: Vec::new(),
        }
    }
}

pub type Source = Box<dyn Fn() -> Batch + Send + Sync>;

/// Commits the sources' changes periodically and on demand.
pub struct Flusher {
    store: Arc<Store>,
    sources: Mutex<Vec<Source>>,
    /// Serializes flushes; holds the time of the last one.
    flushing: Mutex<Instant>,
    stop: AtomicBool,
    wake: (Mutex<()>, Condvar),
    failures: AtomicU64,
}

impl Flusher {
    pub fn new(store: Arc<Store>) -> Arc<Flusher> {
        Arc::new(Flusher {
            store,
            sources: Mutex::new(Vec::new()),
            flushing: Mutex::new(Instant::now()),
            stop: AtomicBool::new(false),
            wake: (Mutex::new(()), Condvar::new()),
            failures: AtomicU64::new(0),
        })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    pub fn add_source(&self, s: Source) {
        self.sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(s);
    }

    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    /// Commits every pending change now; returns once it is durable.
    pub fn flush_now(&self) -> std::io::Result<()> {
        let mut last = self.flushing.lock().unwrap_or_else(|e| e.into_inner());
        let mut ops = Vec::new();
        let mut after = Vec::new();
        {
            let sources = self.sources.lock().unwrap_or_else(|e| e.into_inner());
            for s in sources.iter() {
                let b = s();
                ops.extend(b.ops);
                after.extend(b.after);
            }
        }
        let r = self.store.commit(&ops);
        match &r {
            Ok(()) => after.into_iter().for_each(|f| f()),
            Err(e) => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                log::error!("store {}: commit failed: {e}", self.store.path().display());
            }
        }
        *last = Instant::now();
        r
    }

    /// Starts the periodic flusher thread.
    pub fn start(self: &Arc<Self>) -> std::thread::JoinHandle<()> {
        let me = Arc::clone(self);
        std::thread::Builder::new()
            .name("syncfs-flush".into())
            .spawn(move || {
                while !me.stop.load(Ordering::SeqCst) {
                    let g = me.wake.0.lock().unwrap_or_else(|e| e.into_inner());
                    let _ = me
                        .wake
                        .1
                        .wait_timeout_while(g, FLUSH_INTERVAL, |_| !me.stop.load(Ordering::SeqCst));
                    let _ = me.flush_now();
                }
            })
            .expect("spawning the flusher")
    }

    /// Stops the thread after a final flush (the caller joins it).
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.wake.1.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commits_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = Store::open(dir.path()).unwrap();
            s.commit(&[
                (Table::Meta, b"a".to_vec(), Some(encode(&7u64))),
                (Table::Files, b"x".to_vec(), Some(b"1".to_vec())),
                (Table::Files, b"y".to_vec(), Some(b"2".to_vec())),
            ])
            .unwrap();
            s.commit(&[(Table::Files, b"x".to_vec(), None)]).unwrap();
        }
        let s = Store::open(dir.path()).unwrap();
        let a: u64 = decode(&s.get(Table::Meta, b"a").unwrap().unwrap()).unwrap();
        assert_eq!(a, 7);
        assert_eq!(
            s.load(Table::Files).unwrap(),
            vec![(b"y".to_vec(), b"2".to_vec())]
        );
    }

    #[test]
    fn flusher_collects_sources() {
        let dir = tempfile::tempdir().unwrap();
        let f = Flusher::new(Arc::new(Store::open(dir.path()).unwrap()));
        let pending = Arc::new(Mutex::new(vec![(
            Table::Window,
            b"k".to_vec(),
            Some(vec![1]),
        )]));
        let p = Arc::clone(&pending);
        f.add_source(Box::new(move || {
            std::mem::take(&mut *p.lock().unwrap()).into()
        }));
        f.flush_now().unwrap();
        assert_eq!(f.store().load(Table::Window).unwrap().len(), 1);
        f.flush_now().unwrap(); // nothing pending: no commit needed
        assert_eq!(f.store().commits(), 1);
    }
}
