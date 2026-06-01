//! Core domain types shared across the Pulse engine crates.
//!
//! These are deliberately dependency-light (serde + uuid only) so every other
//! crate — CDC, reactor, SSE, RPC, runtime — can depend on `pulse-core` without
//! pulling in Postgres, HTTP, or the JS runtime.

mod change;
mod lsn;
mod procedure;
mod readset;

pub use change::{Change, ChangeOp, ChangeSet, KeyValue, PrimaryKey, RowValues, TableId};
pub use lsn::{Lsn, ParseLsnError};
pub use procedure::{ProcedureKind, ProcedurePath, SubscriptionId};
pub use readset::{Cond, Filter, FilterOp, IndexBound, IndexRange, ReadSet};
