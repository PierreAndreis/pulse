//! CRDT document merge for `v.collab()` fields, backed by Yjs (`yrs`).
//!
//! A collab field is stored as a Yjs document's binary state (a `bytea` in
//! Postgres). All edits — local, offline-replayed, or arriving from another node
//! over the change bus — are Yjs *updates* (binary deltas). Merging is done here,
//! server-side, by `yrs`. Yjs merges are **commutative, associative, and
//! idempotent**, which is exactly what makes the hard cases safe:
//!
//! - **Offline edit to an already-changed doc** (the long-Notion-page case):
//!   replaying the offline update onto newer server state merges both edits — no
//!   clobber.
//! - **Retry** (M4 serializable conflict): re-applying the same update is a no-op.
//! - **Cross-node** (the LISTEN/NOTIFY bus): every node applies the same updates
//!   in any order and converges to identical state.
//!
//! This crate is intentionally tiny and dependency-light (just `yrs`) so it can
//! be unit-tested in isolation and reused by `pulse-sql` for the in-transaction
//! merge.

use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

#[derive(Debug, thiserror::Error)]
pub enum CollabError {
    #[error("failed to decode Yjs update: {0}")]
    Decode(String),
    #[error("failed to apply Yjs update: {0}")]
    Apply(String),
}

/// The empty document state — the starting value for a new collab field.
/// (A fresh `Doc` encoded as a full update; applying any update to it works.)
pub fn empty_state() -> Vec<u8> {
    let doc = Doc::new();
    let txn = doc.transact();
    txn.encode_state_as_update_v1(&StateVector::default())
}

/// Load a stored document state into a `yrs::Doc`. An empty slice yields a fresh
/// (empty) doc, so callers don't special-case "no row yet".
fn load(state: &[u8]) -> Result<Doc, CollabError> {
    let doc = Doc::new();
    if !state.is_empty() {
        let update = Update::decode_v1(state).map_err(|e| CollabError::Decode(e.to_string()))?;
        doc.transact_mut()
            .apply_update(update)
            .map_err(|e| CollabError::Apply(e.to_string()))?;
    }
    Ok(doc)
}

/// Encode a doc's full state as a v1 update (the form we persist in `bytea`).
fn encode(doc: &Doc) -> Vec<u8> {
    let txn = doc.transact();
    txn.encode_state_as_update_v1(&StateVector::default())
}

/// Apply a Yjs update to a stored document state, returning the merged state.
/// `state` may be empty (new doc). The result is the new persisted value.
pub fn apply_update(state: &[u8], update: &[u8]) -> Result<Vec<u8>, CollabError> {
    let doc = load(state)?;
    let update = Update::decode_v1(update).map_err(|e| CollabError::Decode(e.to_string()))?;
    doc.transact_mut()
        .apply_update(update)
        .map_err(|e| CollabError::Apply(e.to_string()))?;
    Ok(encode(&doc))
}

/// Merge two stored states (or a state and a full-state update) into one. Used
/// when the change bus delivers a peer's full state, or to combine snapshots.
pub fn merge(a: &[u8], b: &[u8]) -> Result<Vec<u8>, CollabError> {
    if a.is_empty() {
        return Ok(b.to_vec());
    }
    apply_update(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use yrs::{GetString, Text, Transact};

    /// Build a Yjs update that inserts `s` at index 0 of the root text "body",
    /// starting from `base` state. Returns the update bytes (what a client sends).
    fn insert_update(base: &[u8], text: &str, at: u32) -> Vec<u8> {
        let doc = load(base).unwrap();
        let before = {
            let txn = doc.transact();
            txn.state_vector()
        };
        {
            let body = doc.get_or_insert_text("body");
            let mut txn = doc.transact_mut();
            body.insert(&mut txn, at, text);
        }
        // The update is the delta since `before`.
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&before)
    }

    fn read_body(state: &[u8]) -> String {
        let doc = load(state).unwrap();
        let body = doc.get_or_insert_text("body");
        let txn = doc.transact();
        body.get_string(&txn)
    }

    // Tracer bullet: two independent edits to the same doc both survive a merge.
    #[test]
    fn two_updates_both_survive() {
        let base = empty_state();

        // Two clients start from the same base and each insert independently.
        let u1 = insert_update(&base, "hello", 0);
        let u2 = insert_update(&base, "world", 0);

        // Server applies both (in this order) on top of the base.
        let s1 = apply_update(&base, &u1).unwrap();
        let s2 = apply_update(&s1, &u2).unwrap();

        let body = read_body(&s2);
        assert!(body.contains("hello"), "first edit lost: {body:?}");
        assert!(body.contains("world"), "second edit lost: {body:?}");
    }

    // Convergence: applying the two updates in EITHER order yields identical
    // state (commutativity) — the property the cross-node bus relies on.
    #[test]
    fn merge_is_order_independent() {
        let base = empty_state();
        let u1 = insert_update(&base, "AAA", 0);
        let u2 = insert_update(&base, "BBB", 0);

        let forward = apply_update(&apply_update(&base, &u1).unwrap(), &u2).unwrap();
        let backward = apply_update(&apply_update(&base, &u2).unwrap(), &u1).unwrap();

        // Both contain both edits...
        assert!(read_body(&forward).contains("AAA") && read_body(&forward).contains("BBB"));
        // ...and converge to the same text (deterministic resolution).
        assert_eq!(read_body(&forward), read_body(&backward));
    }

    // Idempotency: re-applying the same update (retry / duplicate delivery) is a
    // no-op — what makes M4 retry and at-least-once bus delivery safe.
    #[test]
    fn reapplying_an_update_is_idempotent() {
        let base = empty_state();
        let u1 = insert_update(&base, "once", 0);
        let after_one = apply_update(&base, &u1).unwrap();
        let after_two = apply_update(&after_one, &u1).unwrap();
        assert_eq!(read_body(&after_one), "once");
        assert_eq!(read_body(&after_two), "once"); // not "onceonce"
    }

    // The offline-divergence case in miniature: a client edits offline starting
    // from an OLD base while the server doc moves on; replaying the offline update
    // onto the newer state keeps both.
    #[test]
    fn offline_edit_merges_into_changed_doc() {
        let base = empty_state();

        // Client goes offline with `base`, edits locally.
        let offline_update = insert_update(&base, "offline-edit", 0);

        // Meanwhile the server doc changes (someone else edits).
        let server_state = apply_update(&base, &insert_update(&base, "server-edit", 0)).unwrap();

        // Client reconnects; its offline update is replayed onto the new state.
        let merged = apply_update(&server_state, &offline_update).unwrap();
        let body = read_body(&merged);
        assert!(
            body.contains("offline-edit"),
            "offline edit clobbered: {body:?}"
        );
        assert!(
            body.contains("server-edit"),
            "server edit clobbered: {body:?}"
        );
    }

    // merge with an empty left side returns the right side verbatim (the
    // "no row yet" fast path in `merge`).
    #[test]
    fn merge_empty_left_returns_right_verbatim() {
        let b = apply_update(&empty_state(), &insert_update(&empty_state(), "rhs", 0)).unwrap();
        let merged = merge(&[], &b).unwrap();
        assert_eq!(merged, b);
    }

    // merge with an empty right side (an empty doc's full state) preserves the
    // left's contents — merging in "nothing" is a no-op on the body.
    #[test]
    fn merge_empty_right_keeps_left() {
        let a = apply_update(&empty_state(), &insert_update(&empty_state(), "keep-me", 0)).unwrap();
        let merged = merge(&a, &empty_state()).unwrap();
        assert_eq!(read_body(&merged), read_body(&a));
        assert_eq!(read_body(&merged), "keep-me");
    }

    // A malformed update surfaces a Decode error (never a panic). The update is
    // decoded after a valid (empty) state loads, so the error comes from the
    // update path in `apply_update`.
    #[test]
    fn garbage_update_is_decode_error() {
        let err = apply_update(&empty_state(), b"\xff\xff\xff").unwrap_err();
        assert!(
            matches!(err, CollabError::Decode(_)),
            "expected Decode, got {err:?}"
        );
    }

    // A malformed stored state surfaces a Decode error via `load` (the state is
    // decoded first), even when the update itself is well-formed.
    #[test]
    fn garbage_state_is_decode_error() {
        let valid_update = insert_update(&empty_state(), "ok", 0);
        let err = apply_update(b"\xff\xff", &valid_update).unwrap_err();
        assert!(
            matches!(err, CollabError::Decode(_)),
            "expected Decode, got {err:?}"
        );
    }

    // 3-way associativity: combining three concurrent edits in any grouping/order
    // converges to identical state — the stronger form of the 2-way property the
    // cross-node bus relies on.
    #[test]
    fn three_way_merge_is_associative() {
        let base = empty_state();
        let a = insert_update(&base, "AAA", 0);
        let b = insert_update(&base, "BBB", 0);
        let c = insert_update(&base, "CCC", 0);

        // ((base + a) + b) + c
        let left = apply_update(
            &apply_update(&apply_update(&base, &a).unwrap(), &b).unwrap(),
            &c,
        )
        .unwrap();
        // base + (c + (b + a)) — applied in reverse order
        let right = apply_update(
            &apply_update(&apply_update(&base, &c).unwrap(), &b).unwrap(),
            &a,
        )
        .unwrap();

        let lbody = read_body(&left);
        assert!(lbody.contains("AAA") && lbody.contains("BBB") && lbody.contains("CCC"));
        assert_eq!(read_body(&left), read_body(&right));
    }

    // A delete on one branch survives a concurrent insert on another: client X
    // deletes existing text while client Y inserts elsewhere from the same base;
    // merging both keeps the insert and honors the delete.
    #[test]
    fn delete_survives_concurrent_edit() {
        // Shared starting state with two words present.
        let base = apply_update(
            &empty_state(),
            &insert_update(&empty_state(), "DELETEME-keep", 0),
        )
        .unwrap();

        // Client X (offline) deletes the leading "DELETEME-" (8 chars at index 0).
        let delete_update = {
            let doc = load(&base).unwrap();
            let before = {
                let txn = doc.transact();
                txn.state_vector()
            };
            {
                let body = doc.get_or_insert_text("body");
                let mut txn = doc.transact_mut();
                body.remove_range(&mut txn, 0, 9);
            }
            let txn = doc.transact();
            txn.encode_state_as_update_v1(&before)
        };

        // Client Y (offline, same base) appends " inserted" at the end.
        let insert_at_end = insert_update(&base, " inserted", 13);

        // Merge both onto base, in both orders — must converge and honor delete.
        let forward = apply_update(
            &apply_update(&base, &delete_update).unwrap(),
            &insert_at_end,
        )
        .unwrap();
        let backward = apply_update(
            &apply_update(&base, &insert_at_end).unwrap(),
            &delete_update,
        )
        .unwrap();

        let body = read_body(&forward);
        assert!(!body.contains("DELETEME"), "delete lost: {body:?}");
        assert!(body.contains("keep"), "surviving text lost: {body:?}");
        assert!(
            body.contains("inserted"),
            "concurrent insert lost: {body:?}"
        );
        assert_eq!(read_body(&forward), read_body(&backward));
    }
}
