//! Values that a configuration reload (`mount -o remount`, SIGHUP) can
//! change while the node runs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use super::Config;

/// A duration that any thread can read and a reload can change. Readers
/// see the new value the next time they look; nothing is interrupted.
#[derive(Debug, Default)]
pub struct LiveDuration(AtomicU64);

impl LiveDuration {
    pub fn new(d: Duration) -> Self {
        Self(AtomicU64::new(nanos(d)))
    }

    pub fn get(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::Relaxed))
    }

    pub fn set(&self, d: Duration) {
        self.0.store(nanos(d), Ordering::Relaxed);
    }
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// The configuration in effect, replaced as a whole by a successful reload
/// (shown in `.syncfs/config`).
pub type SharedConfig = Arc<RwLock<Config>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_duration_round_trips_and_saturates() {
        let d = LiveDuration::new(Duration::from_millis(1500));
        assert_eq!(d.get(), Duration::from_millis(1500));
        d.set(Duration::from_secs(3));
        assert_eq!(d.get(), Duration::from_secs(3));
        d.set(Duration::MAX);
        assert_eq!(d.get(), Duration::from_nanos(u64::MAX));
    }
}
