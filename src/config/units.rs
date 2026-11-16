//! Human-friendly value types used in the configuration: durations such as
//! `30s` or `1m 30s`, and byte sizes such as `64K` or `1MiB`.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A duration written as `30s`, `500ms`, `1m 30s`, or a bare number of
/// seconds (`30`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HumanDuration(pub Duration);

impl HumanDuration {
    pub const fn from_secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs))
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

impl FromStr for HumanDuration {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if let Ok(secs) = s.parse::<u64>() {
            return Ok(Self::from_secs(secs));
        }
        humantime::parse_duration(s)
            .map(Self)
            .map_err(|e| format!("invalid duration {s:?}: {e} (examples: 30s, 500ms, 1m 30s)"))
    }
}

impl fmt::Display for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", humantime::format_duration(self.0))
    }
}

impl Serialize for HumanDuration {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(FromStrOrIntVisitor::<Self>::new(
            "a duration like 30s or 500ms",
        ))
    }
}

/// A size in bytes written as a plain integer (`65536`) or with a binary
/// suffix (`64K`, `64KB`, `64KiB`, `1M`, `1G`). All suffixes are powers of
/// 1024: block sizes are always binary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(pub u64);

impl ByteSize {
    pub const KIB: u64 = 1024;
    pub const MIB: u64 = 1024 * 1024;
    pub const GIB: u64 = 1024 * 1024 * 1024;

    pub const fn bytes(self) -> u64 {
        self.0
    }
}

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let t = s.trim();
        let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
        let (num, suffix) = t.split_at(split);
        let err = || format!("invalid size {s:?} (examples: 65536, 64K, 64KiB, 1M)");
        let n: u64 = num.parse().map_err(|_| err())?;
        let mult = match suffix.trim().to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "k" | "kb" | "kib" => Self::KIB,
            "m" | "mb" | "mib" => Self::MIB,
            "g" | "gb" | "gib" => Self::GIB,
            _ => return Err(err()),
        };
        n.checked_mul(mult).map(Self).ok_or_else(err)
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.0;
        for (unit, name) in [(Self::GIB, "GiB"), (Self::MIB, "MiB"), (Self::KIB, "KiB")] {
            if n >= unit && n.is_multiple_of(unit) {
                return write!(f, "{}{}", n / unit, name);
            }
        }
        write!(f, "{n}")
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(FromStrOrIntVisitor::<Self>::new(
            "a size like 65536, 64K or 1MiB",
        ))
    }
}

/// Accepts either a YAML string (parsed with `FromStr`) or a non-negative
/// integer (parsed from its decimal form, so it gets the same meaning as the
/// bare-number string).
struct FromStrOrIntVisitor<T> {
    expecting: &'static str,
    _t: std::marker::PhantomData<T>,
}

impl<T> FromStrOrIntVisitor<T> {
    fn new(expecting: &'static str) -> Self {
        Self {
            expecting,
            _t: std::marker::PhantomData,
        }
    }
}

impl<T> Visitor<'_> for FromStrOrIntVisitor<T>
where
    T: FromStr<Err = String>,
{
    type Value = T;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.expecting)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<T, E> {
        v.parse().map_err(E::custom)
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<T, E> {
        v.to_string().parse().map_err(E::custom)
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<T, E> {
        if v < 0 {
            return Err(E::custom(format!("negative value {v} not allowed")));
        }
        v.to_string().parse().map_err(E::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse() {
        assert_eq!(
            "30s".parse::<HumanDuration>().unwrap(),
            HumanDuration::from_secs(30)
        );
        assert_eq!(
            "30".parse::<HumanDuration>().unwrap(),
            HumanDuration::from_secs(30)
        );
        assert_eq!(
            "500ms".parse::<HumanDuration>().unwrap(),
            HumanDuration(Duration::from_millis(500))
        );
        assert_eq!(
            "1m 30s".parse::<HumanDuration>().unwrap(),
            HumanDuration::from_secs(90)
        );
        assert!("soon".parse::<HumanDuration>().is_err());
        assert!("-3s".parse::<HumanDuration>().is_err());
    }

    #[test]
    fn durations_round_trip_display() {
        for s in ["30s", "500ms", "1m 30s", "2h"] {
            let d: HumanDuration = s.parse().unwrap();
            assert_eq!(d.to_string(), s);
        }
    }

    #[test]
    fn sizes_parse() {
        assert_eq!("65536".parse::<ByteSize>().unwrap(), ByteSize(65536));
        assert_eq!("64K".parse::<ByteSize>().unwrap(), ByteSize(65536));
        assert_eq!("64k".parse::<ByteSize>().unwrap(), ByteSize(65536));
        assert_eq!("64KB".parse::<ByteSize>().unwrap(), ByteSize(65536));
        assert_eq!("64KiB".parse::<ByteSize>().unwrap(), ByteSize(65536));
        assert_eq!("1M".parse::<ByteSize>().unwrap(), ByteSize(1 << 20));
        assert_eq!("2GiB".parse::<ByteSize>().unwrap(), ByteSize(2 << 30));
        assert!("".parse::<ByteSize>().is_err());
        assert!("64X".parse::<ByteSize>().is_err());
        assert!("K".parse::<ByteSize>().is_err());
        assert!("99999999999999999999G".parse::<ByteSize>().is_err());
    }

    #[test]
    fn sizes_display() {
        assert_eq!(ByteSize(65536).to_string(), "64KiB");
        assert_eq!(ByteSize(1 << 20).to_string(), "1MiB");
        assert_eq!(ByteSize(1000).to_string(), "1000");
    }

    #[test]
    fn yaml_accepts_strings_and_ints() {
        let d: HumanDuration = serde_yaml_ng::from_str("45").unwrap();
        assert_eq!(d, HumanDuration::from_secs(45));
        let d: HumanDuration = serde_yaml_ng::from_str("\"2m\"").unwrap();
        assert_eq!(d, HumanDuration::from_secs(120));
        let b: ByteSize = serde_yaml_ng::from_str("4096").unwrap();
        assert_eq!(b, ByteSize(4096));
        let b: ByteSize = serde_yaml_ng::from_str("128K").unwrap();
        assert_eq!(b, ByteSize(128 * 1024));
        assert!(serde_yaml_ng::from_str::<ByteSize>("-1").is_err());
    }
}
