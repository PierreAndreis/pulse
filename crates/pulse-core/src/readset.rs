use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::change::{Change, ChangeSet, KeyValue, PrimaryKey, RowValues, TableId};

/// One bound of an index range scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexBound {
    pub values: Vec<KeyValue>,
    /// Whether the bound itself is included.
    pub inclusive: bool,
}

/// An index-range read. Reserved for the CDC/true-index-range path; the live
/// matcher operates on `Filter` (predicate) form, not on these.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRange {
    pub index: String,
    pub lower: Option<IndexBound>,
    pub upper: Option<IndexBound>,
}

/// A comparison operator on a captured query predicate. Mirrors `pulse_sql::PredOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    /// SQL `LIKE` (case-sensitive) — `%` = any run, `_` = one char.
    Like,
    /// SQL `ILIKE` (case-insensitive).
    Ilike,
}

/// One analyzed predicate: `field <op> value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cond {
    pub field: String,
    pub op: FilterOp,
    pub value: KeyValue,
}

/// One analyzed read of a table. `conds` are AND-ed; an empty `conds` would be a
/// whole-table read (but we record those as `tables` wildcards instead).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    pub conds: Vec<Cond>,
}

/// What a reactive query read, at increasing precision. A change invalidates the
/// read-set if it touches a referenced table (coarse), an exact key, or a row
/// satisfying one of the analyzed predicate filters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSet {
    /// Coarse fallback: whole tables read (raw SQL / full scan / uncoercible predicate).
    pub tables: HashSet<TableId>,
    /// Exact rows read via point lookups (`get(id)`).
    pub keys: HashMap<TableId, HashSet<PrimaryKey>>,
    /// Analyzed query predicates (the precise path).
    pub filters: HashMap<TableId, Vec<Filter>>,
    /// Index ranges — reserved for CDC/range work; not consulted by the live matcher.
    pub ranges: HashMap<TableId, Vec<IndexRange>>,
}

fn eval(cond: &Cond, row: &RowValues) -> bool {
    match row.get(&cond.field) {
        // Value not captured (e.g. omitted/uncoercible column) → conservatively match.
        None => true,
        Some(v) => match cond.op {
            FilterOp::Eq => v == &cond.value,
            FilterOp::Neq => v != &cond.value,
            FilterOp::Gt => matches!(v.order(&cond.value), Some(Ordering::Greater)),
            FilterOp::Gte => matches!(
                v.order(&cond.value),
                Some(Ordering::Greater | Ordering::Equal)
            ),
            FilterOp::Lt => matches!(v.order(&cond.value), Some(Ordering::Less)),
            FilterOp::Lte => matches!(v.order(&cond.value), Some(Ordering::Less | Ordering::Equal)),
            FilterOp::Like => like_match(v, &cond.value, false),
            FilterOp::Ilike => like_match(v, &cond.value, true),
        },
    }
}

/// SQL `LIKE`/`ILIKE` match between two text values (`%` = any run incl. empty,
/// `_` = exactly one char). Non-text values never match.
fn like_match(value: &KeyValue, pattern: &KeyValue, case_insensitive: bool) -> bool {
    let (KeyValue::Text(s), KeyValue::Text(p)) = (value, pattern) else {
        return false;
    };
    if case_insensitive {
        like_is_match(&s.to_lowercase(), &p.to_lowercase())
    } else {
        like_is_match(s, p)
    }
}

/// SQL LIKE pattern match (no escape handling). DP over chars.
fn like_is_match(s: &str, p: &str) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = p.chars().collect();
    let (n, m) = (s.len(), p.len());
    let mut dp = vec![vec![false; m + 1]; n + 1];
    dp[0][0] = true;
    for j in 1..=m {
        if p[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }
    for i in 1..=n {
        for j in 1..=m {
            dp[i][j] = match p[j - 1] {
                '%' => dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i - 1][j - 1],
                c => dp[i - 1][j - 1] && s[i - 1] == c,
            };
        }
    }
    dp[n][m]
}

fn filter_matches(filter: &Filter, change: &Change) -> bool {
    let hit = |row: &RowValues| filter.conds.iter().all(|c| eval(c, row));
    // OR over images so a row ENTERING (new) or LEAVING (old) the filter invalidates.
    change.new.as_ref().is_some_and(hit) || change.old.as_ref().is_some_and(hit)
}

impl ReadSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, table: TableId) {
        self.tables.insert(table);
    }

    pub fn add_key(&mut self, table: TableId, key: PrimaryKey) {
        self.keys.entry(table).or_default().insert(key);
    }

    pub fn add_filter(&mut self, table: TableId, filter: Filter) {
        self.filters.entry(table).or_default().push(filter);
    }

    pub fn add_range(&mut self, table: TableId, range: IndexRange) {
        self.ranges.entry(table).or_default().push(range);
    }

    /// Does this single change invalidate the read-set?
    pub fn matches_change(&self, change: &Change) -> bool {
        if self.tables.contains(&change.table) {
            return true; // coarse / raw / full-scan reader
        }
        if let Some(keys) = self.keys.get(&change.table) {
            if keys.contains(&change.key) {
                return true; // point get(id) of this row
            }
        }
        if let Some(filters) = self.filters.get(&change.table) {
            if filters.iter().any(|f| filter_matches(f, change)) {
                return true;
            }
        }
        false
    }

    /// Does any change in the set invalidate the read-set?
    pub fn matches(&self, change_set: &ChangeSet) -> bool {
        change_set.changes.iter().any(|c| self.matches_change(c))
    }

    /// Tables referenced at any precision — used to build the coarse table index.
    pub fn referenced_tables(&self) -> HashSet<TableId> {
        let mut out = self.tables.clone();
        out.extend(self.keys.keys().cloned());
        out.extend(self.filters.keys().cloned());
        out.extend(self.ranges.keys().cloned());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::ChangeOp;
    use crate::lsn::Lsn;

    fn insert(table: &str, key: i64) -> Change {
        Change::point(
            TableId::new(table),
            PrimaryKey::single(KeyValue::Int(key)),
            ChangeOp::Insert,
        )
    }

    fn row(pairs: &[(&str, KeyValue)]) -> RowValues {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    fn eq(field: &str, value: KeyValue) -> Filter {
        Filter {
            conds: vec![Cond {
                field: field.to_string(),
                op: FilterOp::Eq,
                value,
            }],
        }
    }

    #[test]
    fn table_level_match() {
        let mut rs = ReadSet::new();
        rs.add_table(TableId::new("messages"));
        let mut cs = ChangeSet::new(Lsn(1));
        cs.push(insert("messages", 7));
        assert!(rs.matches(&cs));
    }

    #[test]
    fn key_level_match_is_precise() {
        let mut rs = ReadSet::new();
        rs.add_key(
            TableId::new("messages"),
            PrimaryKey::single(KeyValue::Int(7)),
        );
        let hit = insert("messages", 7);
        let miss = insert("messages", 8);
        assert!(rs.matches_change(&hit));
        assert!(!rs.matches_change(&miss));
    }

    #[test]
    fn unrelated_table_does_not_match() {
        let mut rs = ReadSet::new();
        rs.add_table(TableId::new("messages"));
        let mut cs = ChangeSet::new(Lsn(1));
        cs.push(insert("users", 1));
        assert!(!rs.matches(&cs));
    }

    #[test]
    fn filter_prunes_other_channel() {
        // Read messages WHERE channelId = A.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            eq("channelId", KeyValue::Text("A".into())),
        );

        let into_a = Change {
            new: Some(row(&[("channelId", KeyValue::Text("A".into()))])),
            ..insert("messages", 1)
        };
        let into_b = Change {
            new: Some(row(&[("channelId", KeyValue::Text("B".into()))])),
            ..insert("messages", 2)
        };
        assert!(rs.matches_change(&into_a)); // same channel → match
        assert!(!rs.matches_change(&into_b)); // foreign channel → pruned
    }

    #[test]
    fn neq_filter_prunes_equal_rows() {
        // Read todos WHERE done <> false (i.e. the done ones).
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter {
                conds: vec![Cond {
                    field: "done".into(),
                    op: FilterOp::Neq,
                    value: KeyValue::Bool(false),
                }],
            },
        );
        let done = Change {
            new: Some(row(&[("done", KeyValue::Bool(true))])),
            ..insert("todos", 1)
        };
        let not_done = Change {
            new: Some(row(&[("done", KeyValue::Bool(false))])),
            ..insert("todos", 2)
        };
        assert!(rs.matches_change(&done)); // done=true ≠ false → in the result → match
        assert!(!rs.matches_change(&not_done)); // done=false → excluded → pruned
    }

    #[test]
    fn like_filter_matches_pattern_precisely() {
        let mk = |op: FilterOp| {
            let mut rs = ReadSet::new();
            rs.add_filter(
                TableId::new("todos"),
                Filter {
                    conds: vec![Cond {
                        field: "title".into(),
                        op,
                        value: KeyValue::Text("%ppl%".into()),
                    }],
                },
            );
            rs
        };
        let title = |s: &str| Change {
            new: Some(row(&[("title", KeyValue::Text(s.into()))])),
            ..insert("todos", 1)
        };
        let rs = mk(FilterOp::Like);
        assert!(rs.matches_change(&title("apple"))); // contains "ppl"
        assert!(!rs.matches_change(&title("grape"))); // does not
        assert!(!rs.matches_change(&title("APPLE"))); // case-sensitive LIKE
        let ci = mk(FilterOp::Ilike);
        assert!(ci.matches_change(&title("APPLE"))); // ILIKE is case-insensitive
    }

    #[test]
    fn multi_cond_filter_requires_all_to_match() {
        // done = false AND priority > 5 — a change must satisfy BOTH to invalidate.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter {
                conds: vec![
                    Cond {
                        field: "done".into(),
                        op: FilterOp::Eq,
                        value: KeyValue::Bool(false),
                    },
                    Cond {
                        field: "priority".into(),
                        op: FilterOp::Gt,
                        value: KeyValue::Int(5),
                    },
                ],
            },
        );
        let both = Change {
            new: Some(row(&[
                ("done", KeyValue::Bool(false)),
                ("priority", KeyValue::Int(9)),
            ])),
            ..insert("todos", 1)
        };
        let only_one = Change {
            new: Some(row(&[
                ("done", KeyValue::Bool(false)),
                ("priority", KeyValue::Int(3)),
            ])),
            ..insert("todos", 2)
        };
        assert!(rs.matches_change(&both)); // both conds hold → in result → match
        assert!(!rs.matches_change(&only_one)); // priority fails → pruned
    }

    #[test]
    fn neq_with_absent_field_matches_conservatively() {
        // A change image that doesn't carry the filtered field must conservatively
        // invalidate (never a missed update) rather than be wrongly pruned.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("todos"),
            Filter {
                conds: vec![Cond {
                    field: "done".into(),
                    op: FilterOp::Neq,
                    value: KeyValue::Bool(false),
                }],
            },
        );
        let absent = Change {
            new: Some(row(&[("title", KeyValue::Text("x".into()))])),
            ..insert("todos", 1)
        };
        assert!(rs.matches_change(&absent));
    }

    #[test]
    fn filter_matches_via_old_image_on_move() {
        // A row leaving channel A (patched A→B) must still invalidate channel A.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            eq("channelId", KeyValue::Text("A".into())),
        );
        let moved = Change {
            op: ChangeOp::Update,
            old: Some(row(&[("channelId", KeyValue::Text("A".into()))])),
            new: Some(row(&[("channelId", KeyValue::Text("B".into()))])),
            ..insert("messages", 1)
        };
        assert!(rs.matches_change(&moved));
    }

    #[test]
    fn range_filter_orders() {
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            Filter {
                conds: vec![Cond {
                    field: "_creationTime".into(),
                    op: FilterOp::Gte,
                    value: KeyValue::Int(100),
                }],
            },
        );
        let after = Change {
            new: Some(row(&[("_creationTime", KeyValue::Int(150))])),
            ..insert("messages", 1)
        };
        let before = Change {
            new: Some(row(&[("_creationTime", KeyValue::Int(50))])),
            ..insert("messages", 2)
        };
        assert!(rs.matches_change(&after));
        assert!(!rs.matches_change(&before));
    }
}
