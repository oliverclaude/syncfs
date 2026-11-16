//! Lock keys: paths relative to the mount root.
//!
//! Two keys conflict when they are equal or one is an ancestor directory of
//! the other, so a lock on a directory covers everything below it (used for
//! renames and removals of directories).

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

/// A normalized relative path: no leading, trailing or doubled `/`. The
/// empty key is the root.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(Vec<u8>);

impl Key {
    pub fn new(bytes: &[u8]) -> Key {
        let parts: Vec<&[u8]> = bytes
            .split(|&b| b == b'/')
            .filter(|p| !p.is_empty() && *p != b".")
            .collect();
        Key(parts.join(&b'/'))
    }

    pub fn from_path(p: &Path) -> Key {
        Key::new(p.as_os_str().as_bytes())
    }

    /// `parent/name`.
    pub fn child(&self, name: &OsStr) -> Key {
        let mut v = self.0.clone();
        if !v.is_empty() {
            v.push(b'/');
        }
        v.extend_from_slice(name.as_bytes());
        Key::new(&v)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Whether `self` is `other` or an ancestor of it.
    pub fn covers(&self, other: &Key) -> bool {
        self.0.is_empty()
            || other.0 == self.0
            || (other.0.len() > self.0.len()
                && other.0.starts_with(&self.0)
                && other.0[self.0.len()] == b'/')
    }

    pub fn conflicts(&self, other: &Key) -> bool {
        self.covers(other) || other.covers(self)
    }

    /// `self` with prefix `from` replaced by `to` (for renames), if `from`
    /// covers `self`.
    pub fn rebase(&self, from: &Key, to: &Key) -> Option<Key> {
        if !from.covers(self) {
            return None;
        }
        let rest = &self.0[from.0.len()..];
        let mut v = to.0.clone();
        v.extend_from_slice(rest);
        Some(Key::new(&v))
    }
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(&self.0))
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "/{}", String::from_utf8_lossy(&self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Key {
        Key::new(s.as_bytes())
    }

    #[test]
    fn normalizes() {
        assert_eq!(k("/a//b/./c/"), k("a/b/c"));
        assert_eq!(k("a").child(OsStr::new("b")), k("a/b"));
        assert_eq!(k("").child(OsStr::new("b")), k("b"));
        assert_eq!(k("a/b").to_string(), "/a/b");
    }

    #[test]
    fn conflicts_by_ancestry() {
        assert!(k("a").conflicts(&k("a")));
        assert!(k("a").conflicts(&k("a/b/c")));
        assert!(k("a/b/c").conflicts(&k("a")));
        assert!(!k("a").conflicts(&k("ab")));
        assert!(!k("a/b").conflicts(&k("a/c")));
        assert!(k("").conflicts(&k("x")));
    }

    #[test]
    fn rebase() {
        assert_eq!(k("a/b/c").rebase(&k("a/b"), &k("x")), Some(k("x/c")));
        assert_eq!(k("a/b").rebase(&k("a/b"), &k("y/z")), Some(k("y/z")));
        assert_eq!(k("ab").rebase(&k("a"), &k("x")), None);
    }
}
