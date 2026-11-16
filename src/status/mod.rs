//! Runtime status: low-overhead counters and the providers behind the
//! hidden `.syncfs` directory.
//!
//! Each [`Provider`] becomes one read-only file in `.syncfs`, rendered as
//! YAML when it is opened, and one top-level key in `.syncfs/status.json`.
//! Later subsystems (peers, locks, replication) register their own provider
//! with [`Registry::register`]; files are listed in registration order and
//! keep their inode numbers for the life of the mount.

pub mod providers;
mod stats;

use std::sync::{Arc, RwLock};

pub use stats::{Op, Stats};

/// Version of the `status.json` layout. Bumped on incompatible changes.
pub const STATUS_FORMAT_VERSION: u32 = 1;

/// Name of the aggregated machine-readable file.
pub const STATUS_JSON: &str = "status.json";

/// A source of status information.
pub trait Provider: Send + Sync + 'static {
    /// File name in `.syncfs` and key in `status.json`.
    fn name(&self) -> &str;
    /// Current state as a JSON-compatible value (normally an object).
    fn render(&self) -> serde_json::Value;
}

/// A provider built from a closure.
pub struct FnProvider<F> {
    name: String,
    f: F,
}

impl<F: Fn() -> serde_json::Value + Send + Sync + 'static> FnProvider<F> {
    pub fn new(name: impl Into<String>, f: F) -> Self {
        Self {
            name: name.into(),
            f,
        }
    }
}

impl<F: Fn() -> serde_json::Value + Send + Sync + 'static> Provider for FnProvider<F> {
    fn name(&self) -> &str {
        &self.name
    }

    fn render(&self) -> serde_json::Value {
        (self.f)()
    }
}

/// The set of providers. Append-only.
#[derive(Default)]
pub struct Registry {
    providers: RwLock<Vec<Arc<dyn Provider>>>,
}

impl Registry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Adds a provider. A provider whose name is already taken (or is
    /// `status.json`) is ignored and `false` is returned.
    pub fn register(&self, p: Arc<dyn Provider>) -> bool {
        let mut list = self.providers.write().unwrap_or_else(|e| e.into_inner());
        let name = p.name();
        if name == STATUS_JSON
            || name.is_empty()
            || name.contains('/')
            || list.iter().any(|q| q.name() == name)
        {
            log::warn!("status provider {name:?} rejected (invalid or duplicate name)");
            return false;
        }
        list.push(p);
        true
    }

    /// All file names: `status.json` first, then providers in order.
    pub fn file_names(&self) -> Vec<String> {
        let list = self.providers.read().unwrap_or_else(|e| e.into_inner());
        std::iter::once(STATUS_JSON.to_string())
            .chain(list.iter().map(|p| p.name().to_string()))
            .collect()
    }

    /// Renders file number `index` (as in [`file_names`](Self::file_names)).
    pub fn render_file(&self, index: usize) -> Option<Vec<u8>> {
        if index == 0 {
            return Some(self.render_json());
        }
        let p = {
            let list = self.providers.read().unwrap_or_else(|e| e.into_inner());
            Arc::clone(list.get(index - 1)?)
        };
        let v = p.render();
        Some(
            serde_yaml_ng::to_string(&v)
                .unwrap_or_else(|e| format!("error: {e}\n"))
                .into_bytes(),
        )
    }

    /// The aggregated `status.json`.
    pub fn render_json(&self) -> Vec<u8> {
        let list: Vec<_> = self
            .providers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut obj = serde_json::Map::new();
        obj.insert("format_version".into(), STATUS_FORMAT_VERSION.into());
        for p in list {
            obj.insert(p.name().to_string(), p.render());
        }
        let mut out =
            serde_json::to_vec_pretty(&serde_json::Value::Object(obj)).unwrap_or_default();
        out.push(b'\n');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn registry_files_and_rendering() {
        let r = Registry::new();
        assert!(r.register(Arc::new(FnProvider::new("a", || json!({"x": 1})))));
        assert!(r.register(Arc::new(FnProvider::new("b", || json!({"y": [1, 2]})))));
        assert!(!r.register(Arc::new(FnProvider::new("a", || json!(null)))));
        assert!(!r.register(Arc::new(FnProvider::new("status.json", || json!(null)))));
        assert!(!r.register(Arc::new(FnProvider::new("x/y", || json!(null)))));
        assert_eq!(r.file_names(), ["status.json", "a", "b"]);
        assert_eq!(
            String::from_utf8(r.render_file(1).unwrap()).unwrap(),
            "x: 1\n"
        );
        assert!(r.render_file(3).is_none());
        let v: serde_json::Value = serde_json::from_slice(&r.render_file(0).unwrap()).unwrap();
        assert_eq!(
            v,
            json!({"format_version": 1, "a": {"x": 1}, "b": {"y": [1, 2]}})
        );
    }
}
