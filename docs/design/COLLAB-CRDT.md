# Collaborative CRDT fields (`v.collab()`) — architecture & status

Great-DX collaborative documents for Pulse: users bind an editor, and concurrent
/ offline edits to the **same** value **merge** instead of clobbering — even the
long-Notion-page-edited-offline case. See `COLLAB-CLIENT-DX.md` for the binding
client-API contract.

## Model: hybrid, not "everything is a CRDT"

- **Structured/relational data** stays **server-authoritative + serializable**
  (M4): rows, counters, FKs, uniqueness, balances — anything needing invariants
  or a linear history. Conflicts resolve by the engine re-running the mutation.
- **`v.collab()` fields are CRDTs** (Yjs): rich text, lists, boards, canvases —
  order-independent data where merge is the right resolution. Conflicts resolve
  by CRDT merge; no clobber, no invariant guarantees.

This is the same split collaborative apps like Linear converge on. The dividing line is in
the DX doc and the `v.collab()` jsdoc.

## Why Yjs / yrs

The de-facto rich-text CRDT, with a maintained Rust port (`yrs`). So the **merge
happens natively in the Rust engine** (`pulse-collab`, no JS on the hot path),
stored as `bytea`; clients edit via `yjs` + standard editor bindings
(`y-prosemirror`/TipTap, `y-codemirror`, etc.). Binary Yjs **updates** are the
unit that crosses every boundary; on text channels (NDJSON, SSE/JSON) they are
base64-encoded.

Pinned: `yrs` 0.26 (note: `apply_update` → `try_apply_update` in 0.26),
`yjs` 13.6, `y-prosemirror` 1.3, `@tiptap/extension-collaboration` 3.24.

## Convergence guarantee

Yjs merges are **commutative, associative, idempotent**, which makes the hard
cases safe with no special handling:
- **Offline edit onto a changed doc** → replay merges both edits.
- **M4 serializable retry** → re-applying an update is a no-op.
- **Cross-node bus** → every node applies updates in any order, converges identically.

All four properties are unit-proven in `pulse-collab` (see Slice 1).

## Data flow (offline edit → reconnect → merge → cross-node)

```
client editor (Y.Doc) --local update--> @pulse/client CollabHandle
   | offline? queue it (M5 OfflineQueue, durable)         |
   v on reconnect: flush                                   v (online)
 POST /rpc  applyCollab(update b64) ----> engine
   worker ctx.db.applyCollab --NDJSON(b64)--> pulse-sql ApplyCollab
       load bytea -> pulse_collab::apply_update (yrs merge) -> persist (in serializable tx)
       -> ChangeSet -> reactor.apply_change_set --local push + pulse-cdc bus publish-->
          other nodes apply_change_set -> SSE push (Yjs update b64) -> peer CollabHandle
             -> Y.applyUpdate -> editor updates live
```

## Layer-by-layer changes

| Layer | Change | Status |
|---|---|---|
| `pulse-collab` (new crate) | `apply_update`, `merge`, `empty_state` via `yrs`; commutativity/idempotency unit tests | **done** |
| `pulse-sql` | `DbOp::ApplyCollab`/`GetCollab` (base64 ⇄ bytea); merge via `pulse-collab` inside the tx; `access()`/`capture_reads` updated; DB-backed test | **done** |
| `@pulse/schema` | `v.collab()` validator + `CollabField` opaque type + `ValidatorDescription` `collab` kind | **done** |
| `@pulse/cli` codegen/DDL | `collab` → `CollabField` (conditional import) and nullable `bytea`; unit tests | **done** |
| `pulse-jsruntime` + protocol + `worker.ts` | `ctx.db.applyCollab(id, field, update)` / `getCollab(id, field)` emitting the NDJSON ops (engine already executes them) | **todo (Slice 3)** |
| `pulse-reactor`/server | push the Yjs **update** as the SSE delta (currently coarse re-fetch via the emitted `Change`) | **todo (Slice — refine)** |
| `@pulse/client` | `CollabHandle` (Tier 3): wraps `yjs`, initial `getCollab`, local update → queue+`applyCollab`, remote update → `Y.applyUpdate`; rides M5 offline queue | **todo (Slice 4)** |
| `@pulse/react` | `useCollab` (Tier 2) → live `Y.Doc` + `status` | **todo (Slice 5)** |
| `@pulse/react/tiptap` | `usePulseEditor` (Tier 1) one-liner | **todo (Slice 6)** |
| `examples-chat` | `notes` table (`v.collab()` body) + TipTap demo; **kanban board** (`Y.Array`) proving non-text generality | **todo (Slices 6–8)** |

## TDD slices (ordered)

1. **Engine merge tracer bullet** — two updates both survive; order-independent; idempotent; offline-onto-changed merges. ✅ `pulse-collab` (4 tests).
2. **SQL layer** — `ApplyCollab`×2 + `GetCollab` through `execute_op` into real Postgres `bytea`; both edits survive. ✅ `pulse-sql/tests/collab.rs`.
3. **Schema/codegen/DDL** — `v.collab()` → `CollabField` + nullable `bytea`. ✅ (schema + cli unit tests).
4. Worker `ctx.db.applyCollab/getCollab` over NDJSON (base64). **next**
5. `@pulse/client` `CollabHandle` + offline-queue integration.
6. `useCollab` (Tier 2) + `usePulseEditor` TipTap (Tier 1).
7. **Headline integration test** — two clients, one OFFLINE, edit the SAME doc → reconnect → **both edits survive AND both clients converge to identical state** (assert both substrings present *and* equal state; anti-false-positive: never assert mere existence of one edit).
8. **Kanban (`Y.Array`) demo** — proves the non-text path on the same primitive.

## Out of scope (v1)
Awareness/cursors/presence, undo-manager wrapper, GC tuning, per-doc multi-field
sugar. The Tier 2/3 API leaves room to add them without breaking Tier 1.

## Risks / watch-items
- **bytea over the text-cast SQL path:** collab ops bind/fetch `Vec<u8>` directly
  (not the generic text cast), so this is handled — but keep collab columns off
  the generic value path.
- **NOTIFY 8000-byte cap:** a large Yjs update could exceed the cross-node bus
  payload limit → falls back to the existing `Resync` (re-fetch full state). Fine
  for correctness; a future optimization is a fetch-by-state-vector diff.
- **yrs version pinning:** 0.26 renamed `apply_update`→`try_apply_update`; pin and
  test on upgrade.
