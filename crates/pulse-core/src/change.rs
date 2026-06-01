use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::lsn::Lsn;

/// Identifies a reactive table. For now this is the schema-qualified name
/// (`public.messages`); it may become an interned id once the schema registry
/// lands.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub String);

impl TableId {
    pub fn new(name: impl Into<String>) -> Self {
        TableId(name.into())
    }
}

impl From<&str> for TableId {
    fn from(s: &str) -> Self {
        TableId(s.to_string())
    }
}

/// A single column value within a primary key. Floats are intentionally absent
/// — real primary keys are integers, text, uuids, or booleans, all of which are
/// `Hash + Eq` so a `PrimaryKey` can live in a `HashSet`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "t", content = "v")]
pub enum KeyValue {
    Int(i64),
    Text(String),
    Uuid(Uuid),
    Bool(bool),
    Null,
}

impl KeyValue {
    /// Total order within a variant (single columns are single-typed in practice).
    /// Returns `None` across variants, where ordered comparison is undefined.
    pub fn order(&self, other: &KeyValue) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (KeyValue::Int(a), KeyValue::Int(b)) => Some(a.cmp(b)),
            (KeyValue::Text(a), KeyValue::Text(b)) => Some(a.cmp(b)),
            (KeyValue::Uuid(a), KeyValue::Uuid(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
            (KeyValue::Bool(a), KeyValue::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// A (possibly composite) primary key for one row.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PrimaryKey(pub Vec<KeyValue>);

impl PrimaryKey {
    pub fn single(value: KeyValue) -> Self {
        PrimaryKey(vec![value])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

/// A column field (camelCase) → value image of the columns any predicate can
/// filter on. Floats/jsonb/large text are omitted (no `KeyValue::Float`); a
/// filter on such a column degrades to a table-wildcard at capture time.
pub type RowValues = std::collections::HashMap<String, KeyValue>;

/// A single committed row change (from an in-engine mutation or, later, the WAL).
/// `new`/`old` carry the post/pre images so predicate filters can be evaluated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub table: TableId,
    pub key: PrimaryKey,
    pub op: ChangeOp,
    /// Post-image: `Some` for Insert/Update, `None` for Delete.
    #[serde(default)]
    pub new: Option<RowValues>,
    /// Pre-image: `Some` for Update/Delete, `None` for Insert.
    #[serde(default)]
    pub old: Option<RowValues>,
}

impl Change {
    /// A change carrying only table + key (no row images) — coarse matching only.
    pub fn point(table: TableId, key: PrimaryKey, op: ChangeOp) -> Self {
        Change { table, key, op, new: None, old: None }
    }
}

/// All row changes from one committed transaction, tagged with its commit LSN.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSet {
    pub commit_lsn: Lsn,
    pub changes: Vec<Change>,
}

impl ChangeSet {
    pub fn new(commit_lsn: Lsn) -> Self {
        ChangeSet { commit_lsn, changes: Vec::new() }
    }

    pub fn push(&mut self, change: Change) {
        self.changes.push(change);
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}
