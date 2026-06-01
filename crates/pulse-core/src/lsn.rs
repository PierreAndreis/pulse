use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A Postgres Log Sequence Number — a position in the WAL.
///
/// Postgres prints LSNs as two hex halves, `X/Y` (e.g. `16/B374D848`). We store
/// the combined 64-bit value and (de)serialize in the textual form so it round
/// trips with `pg_lsn` and `Standby Status Update` framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid LSN `{0}`: expected `X/Y` hex form")]
pub struct ParseLsnError(String);

impl FromStr for Lsn {
    type Err = ParseLsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = s
            .split_once('/')
            .ok_or_else(|| ParseLsnError(s.to_string()))?;
        let hi = u64::from_str_radix(hi.trim(), 16).map_err(|_| ParseLsnError(s.to_string()))?;
        let lo = u64::from_str_radix(lo.trim(), 16).map_err(|_| ParseLsnError(s.to_string()))?;
        Ok(Lsn((hi << 32) | lo))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl Serialize for Lsn {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_textual_form() {
        let lsn: Lsn = "16/B374D848".parse().unwrap();
        assert_eq!(lsn.0, (0x16u64 << 32) | 0xB374D848);
        assert_eq!(lsn.to_string(), "16/B374D848");
    }

    #[test]
    fn orders_by_position() {
        let a: Lsn = "0/1000".parse().unwrap();
        let b: Lsn = "0/2000".parse().unwrap();
        assert!(a < b);
    }

    #[test]
    fn rejects_garbage() {
        assert!("not-an-lsn".parse::<Lsn>().is_err());
    }
}
