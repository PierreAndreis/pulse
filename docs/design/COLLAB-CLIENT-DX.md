# Collaborative fields — Client DX contract

> Binding requirement for the `v.collab()` / Yjs implementation (see
> `COLLAB-CRDT.md` for the full architecture). **Goal: users never handle
> conflicts and never touch Yjs directly.** "Best DX wins" — this doc is the bar
> the SDK must clear.

## Non-negotiables

1. **No `yjs` import in app code for the common case.** Editing a collaborative
   document is a one-liner; `Y.Doc`/`applyUpdate`/state-vectors never appear in
   user code.
2. **Typed by the contract.** A collab accessor (`pulse.notes.body`) exists
   *only* on fields declared `v.collab()`; the document `id` is `Id<"notes">`.
   Misuse is a compile error, not a runtime surprise.
3. **Offline-first by default.** Collab edits ride the existing M5 offline queue
   automatically; an offline edit to an already-changed doc **merges** on
   reconnect — no clobber, no "this page changed, reload", no lost paragraphs.
4. **Pulse is the sync provider.** No separate `y-websocket` server — collab
   reuses the engine's existing `/sync` SSE + offline queue.
5. **A `status` signal** (`"synced" | "syncing" | "offline"`) is exposed so apps
   can show sync state without reaching into internals.

## Layered API (most ergonomic first)

### Tier 1 — batteries-included editor (`@pulse/react/tiptap`, optional peer dep)
```tsx
function NotePage({ id }: { id: Id<"notes"> }) {
  const editor = usePulseEditor(pulse.notes.body, { id });
  return <EditorContent editor={editor} />;
}
```
Returns a fully-configured TipTap editor already bound to the collab field
(via `y-prosemirror`). Lives in a subpath so the TipTap dependency is opt-in and
never forced on clients using a different editor.

### Tier 2 — editor-agnostic core (`@pulse/react`)
```tsx
const { doc, status } = useCollab(pulse.notes.body, { id });
// `doc` is a live, auto-syncing Y.Doc — bind to Lexical / ProseMirror / custom.
```
This is the foundation Tier 1 is built on. Handles: initial state load,
subscribe → apply remote Yjs updates into `doc`, local `doc` updates →
queue + flush, reconnect resync.

### Tier 3 — imperative core (`@pulse/client`, framework-agnostic)
```ts
const handle = pulse.notes.body.collab({ id });
handle.doc;            // Y.Doc
handle.onUpdate(cb);   // fires on local+remote changes
handle.status();       // "synced" | "syncing" | "offline"
handle.destroy();      // unsubscribe + release
```

## Package boundaries
- `@pulse/client` — `CollabHandle` (Tier 3); depends on `yjs` (we own the dep so
  the user doesn't have to). Re-exports `Y` for power users who want shared types.
- `@pulse/react` — `useCollab` (Tier 2).
- `@pulse/react/tiptap` — `usePulseEditor` (Tier 1); peer-deps `@tiptap/*` +
  `y-prosemirror`.

## Anti-goals (for now)
Awareness/cursors/presence, an undo-manager wrapper, and multi-field-per-doc
sugar are out of scope for the first cut (documented in `COLLAB-CRDT.md`). The
Tier 2/3 API leaves room to add them without breaking Tier 1.
