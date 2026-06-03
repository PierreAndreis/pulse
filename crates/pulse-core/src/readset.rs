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
    Gt,
    Gte,
    Lt,
    Lte,
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
            FilterOp::Gt => matches!(v.order(&cond.value), Some(Ordering::Greater)),
            FilterOp::Gte => matches!(
                v.order(&cond.value),
                Some(Ordering::Greater | Ordering::Equal)
            ),
            FilterOp::Lt => matches!(v.order(&cond.value), Some(Ordering::Less)),
            FilterOp::Lte => matches!(v.order(&cond.value), Some(Ordering::Less | Ordering::Equal)),
        },
    }
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
    use proptest::collection::{hash_set, vec};
    use proptest::prelude::*;

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

    fn cond(field: &str, op: FilterOp, value: KeyValue) -> Filter {
        Filter {
            conds: vec![Cond {
                field: field.to_string(),
                op,
                value,
            }],
        }
    }

    // ── Example-based: matcher correctness ────────────────────────────────

    #[test]
    fn delete_invalidates_filter_via_old_image() {
        // A Delete carries only the pre-image. The only-old branch of the OR in
        // `filter_matches` must still invalidate the channel the row left.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            eq("channelId", KeyValue::Text("A".into())),
        );
        let deleted = Change {
            op: ChangeOp::Delete,
            new: None,
            old: Some(row(&[("channelId", KeyValue::Text("A".into()))])),
            ..insert("messages", 1)
        };
        assert!(rs.matches_change(&deleted));
    }

    #[test]
    fn missing_field_matches_conservatively() {
        // Filter on channelId but the new image omits it → the None-arm of
        // `eval` returns true → conservative match (never miss an invalidation).
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            eq("channelId", KeyValue::Text("A".into())),
        );
        let no_channel = Change {
            new: Some(row(&[("authorId", KeyValue::Int(7))])),
            ..insert("messages", 1)
        };
        assert!(rs.matches_change(&no_channel));
    }

    #[test]
    fn gt_is_exclusive_gte_inclusive() {
        let mut rs = ReadSet::new();
        rs.add_filter(TableId::new("t"), cond("n", FilterOp::Gte, KeyValue::Int(100)));
        let at_100 = Change {
            new: Some(row(&[("n", KeyValue::Int(100))])),
            ..insert("t", 1)
        };
        let at_99 = Change {
            new: Some(row(&[("n", KeyValue::Int(99))])),
            ..insert("t", 2)
        };
        assert!(rs.matches_change(&at_100)); // Gte includes the bound
        assert!(!rs.matches_change(&at_99));

        let mut rs_gt = ReadSet::new();
        rs_gt.add_filter(TableId::new("t"), cond("n", FilterOp::Gt, KeyValue::Int(100)));
        let at_101 = Change {
            new: Some(row(&[("n", KeyValue::Int(101))])),
            ..insert("t", 3)
        };
        assert!(!rs_gt.matches_change(&at_100)); // Gt excludes the bound
        assert!(rs_gt.matches_change(&at_101));

        // Symmetric Lt / Lte.
        let mut rs_lte = ReadSet::new();
        rs_lte.add_filter(TableId::new("t"), cond("n", FilterOp::Lte, KeyValue::Int(100)));
        assert!(rs_lte.matches_change(&at_100)); // Lte includes the bound
        assert!(!rs_lte.matches_change(&at_101));

        let mut rs_lt = ReadSet::new();
        rs_lt.add_filter(TableId::new("t"), cond("n", FilterOp::Lt, KeyValue::Int(100)));
        assert!(!rs_lt.matches_change(&at_100)); // Lt excludes the bound
        assert!(rs_lt.matches_change(&at_99));
    }

    #[test]
    fn cross_variant_order_never_matches() {
        // Ordered ops across KeyValue variants → `order` returns None → no match.
        let mut rs = ReadSet::new();
        rs.add_filter(TableId::new("t"), cond("n", FilterOp::Gte, KeyValue::Int(100)));
        let text_row = Change {
            new: Some(row(&[("n", KeyValue::Text("x".into()))])),
            ..insert("t", 1)
        };
        assert!(!rs.matches_change(&text_row));
    }

    #[test]
    #[allow(non_snake_case)]
    fn multi_cond_is_AND() {
        // [channelId = A AND authorId = 7]: a partial match must NOT invalidate.
        let mut rs = ReadSet::new();
        rs.add_filter(
            TableId::new("messages"),
            Filter {
                conds: vec![
                    Cond {
                        field: "channelId".into(),
                        op: FilterOp::Eq,
                        value: KeyValue::Text("A".into()),
                    },
                    Cond {
                        field: "authorId".into(),
                        op: FilterOp::Eq,
                        value: KeyValue::Int(7),
                    },
                ],
            },
        );
        let only_channel = Change {
            new: Some(row(&[
                ("channelId", KeyValue::Text("A".into())),
                ("authorId", KeyValue::Int(8)),
            ])),
            ..insert("messages", 1)
        };
        let both = Change {
            new: Some(row(&[
                ("channelId", KeyValue::Text("A".into())),
                ("authorId", KeyValue::Int(7)),
            ])),
            ..insert("messages", 2)
        };
        assert!(!rs.matches_change(&only_channel));
        assert!(rs.matches_change(&both));
    }

    #[test]
    fn empty_changeset_matches_nothing() {
        let mut rs = ReadSet::new();
        rs.add_table(TableId::new("messages"));
        let cs = ChangeSet::new(Lsn(1));
        assert!(cs.is_empty());
        assert!(!rs.matches(&cs));
    }

    // ── Property-based ────────────────────────────────────────────────────

    fn kv_strategy() -> impl Strategy<Value = KeyValue> {
        prop_oneof![
            any::<i64>().prop_map(KeyValue::Int),
            "[a-c]{0,3}".prop_map(KeyValue::Text),
            any::<bool>().prop_map(KeyValue::Bool),
            Just(KeyValue::Null),
        ]
    }

    fn op_strategy() -> impl Strategy<Value = FilterOp> {
        prop_oneof![
            Just(FilterOp::Eq),
            Just(FilterOp::Gt),
            Just(FilterOp::Gte),
            Just(FilterOp::Lt),
            Just(FilterOp::Lte),
        ]
    }

    fn field_strategy() -> impl Strategy<Value = String> {
        prop::sample::select(vec!["f0".to_string(), "f1".to_string(), "f2".to_string()])
    }

    fn table_strategy() -> impl Strategy<Value = TableId> {
        prop::sample::select(vec!["t0".to_string(), "t1".to_string(), "t2".to_string()])
            .prop_map(TableId)
    }

    fn cond_strategy() -> impl Strategy<Value = Cond> {
        (field_strategy(), op_strategy(), kv_strategy())
            .prop_map(|(field, op, value)| Cond { field, op, value })
    }

    fn filter_strategy() -> impl Strategy<Value = Filter> {
        vec(cond_strategy(), 0..3).prop_map(|conds| Filter { conds })
    }

    fn row_strategy() -> impl Strategy<Value = RowValues> {
        vec((field_strategy(), kv_strategy()), 0..4)
            .prop_map(|pairs| pairs.into_iter().collect())
    }

    fn opt_row_strategy() -> impl Strategy<Value = Option<RowValues>> {
        prop::option::of(row_strategy())
    }

    fn change_strategy() -> impl Strategy<Value = Change> {
        (
            table_strategy(),
            any::<i64>(),
            prop_oneof![
                Just(ChangeOp::Insert),
                Just(ChangeOp::Update),
                Just(ChangeOp::Delete)
            ],
            opt_row_strategy(),
            opt_row_strategy(),
        )
            .prop_map(|(table, k, op, new, old)| Change {
                table,
                key: PrimaryKey::single(KeyValue::Int(k)),
                op,
                new,
                old,
            })
    }

    fn readset_strategy() -> impl Strategy<Value = ReadSet> {
        (
            hash_set(table_strategy(), 0..3),
            vec((table_strategy(), vec(filter_strategy(), 0..3)), 0..3),
        )
            .prop_map(|(tables, filters)| {
                let mut rs = ReadSet::new();
                rs.tables = tables;
                for (t, fs) in filters {
                    for f in fs {
                        rs.add_filter(t.clone(), f);
                    }
                }
                rs
            })
    }

    fn changeset_strategy() -> impl Strategy<Value = ChangeSet> {
        vec(change_strategy(), 0..4).prop_map(|changes| ChangeSet {
            commit_lsn: Lsn(0),
            changes,
        })
    }

    proptest! {
        // P1: set-level matching is exactly the OR of per-change matching, and a
        // table wildcard always invalidates a change on that table.
        #[test]
        fn p1_matches_is_any_over_changes(rs in readset_strategy(), cs in changeset_strategy()) {
            let expected = cs.changes.iter().any(|c| rs.matches_change(c));
            prop_assert_eq!(rs.matches(&cs), expected);

            for c in &cs.changes {
                if rs.tables.contains(&c.table) {
                    prop_assert!(rs.matches_change(c));
                }
            }
        }

        // P3: `order` is antisymmetric within a variant, None across variants,
        // consistent with Eq, and no value satisfies both Gt(x) and Lt(x).
        #[test]
        fn p3_order_is_well_behaved(a in kv_strategy(), b in kv_strategy()) {
            match (a.order(&b), b.order(&a)) {
                (Some(ab), Some(ba)) => prop_assert_eq!(ab, ba.reverse()),
                (None, None) => {} // cross-variant (or two Nulls): symmetric None
                _ => prop_assert!(false, "order not symmetric: {:?} {:?}", a, b),
            }
            // Consistent with Eq within a variant.
            if let Some(ord) = a.order(&b) {
                prop_assert_eq!(ord == Ordering::Equal, a == b);
            }
            // No value can be both > x and < x.
            let gt = matches!(a.order(&b), Some(Ordering::Greater));
            let lt = matches!(a.order(&b), Some(Ordering::Less));
            prop_assert!(!(gt && lt));
        }
    }
}
