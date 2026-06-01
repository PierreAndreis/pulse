# 06. oRPC-style contract + middleware, pure type inference (no codegen)

- **Status:** Accepted — consistent with `docs/ARCHITECTURE.md` §1, §3.2–3.6 ("oRPC-inspired: procedures + composable middleware, contract-first, no codegen for client types"). Two intentional deviations are noted below: (1) the spec calls the client type `RouterClient<typeof contract>` and mentions `InferInputs`/`InferOutputs`; the implementation exports `Client<C>` and singular `InferInput`/`InferOutput`/`InferKind`/`InferErrors`. (2) The spec writes `const os = implement(contract)` (reusing the name `os` for the bound builder); the implementation keeps `os` as the *standalone* middleware builder and returns an anonymous `Implemented<C>` tree from `implement`, so authoring code reads `const impl = implement(contract)`. The data-model side (`Doc<T>`) is augmentation-based and matches the spec's "schema → type codegen, distinct from client-call types."

## Context & Problem

Pulse asks the developer to write a single dependency-free *contract* and get, with no build step, end-to-end type safety on three different surfaces from that one source of truth (`docs/ARCHITECTURE.md` §3):

- **The client** imports only the contract *type* (`import type { contract }`) and must get fully-typed `.call(input)`, `.queryOptions()`, `.mutationOptions()`, etc., where input/output types come straight from the validators in the contract. The client must not pull in the engine, the handlers, or any generated file.
- **The server** implements a handler against each contract leaf and must get: the right `input` type, the right `output` return type, a kind-appropriate `ctx` (`db` reader for a query, `db` writer for a mutation, `sql` for analytical, runners for an action), and typed constructors for exactly the errors that leaf declared (plus built-ins).
- **Middleware** is reusable and composable. A middleware that, e.g., verifies a JWT and adds `{ user }` to context must (a) *require* whatever it depends on in the incoming context and (b) *widen* the downstream context type by what it adds — so a handler placed after `.use(authed)` sees `ctx.user` typed, with no manual annotation. Composition (`os.use(a).use(b)`) must accumulate both requirements and additions.

Two things force real decisions:

1. **No codegen for call types.** Some reactive frameworks generate a typed `api` object via codegen. We want the contract's `typeof` to *be* the API, inferred. That means the contract must carry enough phantom type information to recover kind/input/output/errors by `infer`, and both `@pulse/server` and `@pulse/client` must reconstruct their trees purely at the type level while their runtime is a generic walk/Proxy.
2. **A latent type cycle around `Doc<T>`.** Validators include `v.doc("messages")` whose static type is `Doc<"messages">`. If `Doc` were derived from `typeof schema` (i.e. `DataModelFromSchema<typeof schema>`), then `schema` (built from `v`, which exposes `v.doc → Doc`) would depend on `Doc`, which would depend on `schema` — a circular type. We need precise `Doc<"table">` types in handlers without that cycle.

## Decision

### 1. Contract builder `oc` — phantom-typed, immutable, plain data

A procedure is started by choosing a kind, then refined with `.input()/.output()/.errors()`. Each refinement returns a *new* `ContractProcedure` (immutable; `makeProcedure` spreads a fresh `def`). The generics are carried by a phantom field that does not exist at runtime:

```ts
interface ContractProcedure<Kind extends ProcedureKind, Input, Output, Errors extends ErrorMap> {
  readonly "~types": { kind: Kind; input: Input; output: Output; errors: Errors }; // phantom
  readonly def: ProcedureDef; // runtime: { kind, input?, output?, errors }
  input<V extends AnyValidator>(v: V): ContractProcedure<Kind, V, Output, Errors>;
  output<V extends AnyValidator>(v: V): ContractProcedure<Kind, Input, V, Errors>;
  errors<E extends ErrorMap>(e: E): ContractProcedure<Kind, Input, Output, Prettify<Errors & E>>;
}
const oc = { reactive, mutation, analytical, action }; // each = start(kind)
```

`Input`/`Output` carry the *validator* type, not the inferred TS type; inference is deferred to helpers so it happens once, at the boundary:

```ts
type InferInput<P>  = P extends ContractProcedure<any, infer I, any, any> ? (I extends AnyValidator ? Infer<I> : void) : never;
type InferOutput<P> = P extends ContractProcedure<any, any, infer O, any> ? (O extends AnyValidator ? Infer<O> : void) : never;
type InferKind<P>   = ...; type InferErrors<P> = ...;
```

A contract is just a (possibly nested) record (`ContractRouter`). It is plain data with no `@pulse/server` import, so the client can `import type` it freely (`docs/ARCHITECTURE.md` §3.2). `isProcedure(node)` distinguishes a leaf from a nested router at runtime by the presence of `def`.

### 2. Middleware — immutable builder, `next()`-extended context, full-out-context tracking

`os` is the standalone builder. The key type is `Useable<TIn, TOut>`: `TIn` is the context a middleware *requires*, `TOut` is the **full** context it yields downstream (its inputs plus its additions), not just the delta. Tracking the full out-context makes inference a single `infer` and composes cleanly under intersection.

```ts
interface ServerBuilder<TContext> extends Useable<{}, TContext> {
  $context<C extends Record<string, unknown>>(): ServerBuilder<C>;           // declare required initial ctx
  middleware<TOut>(fn: (o: MiddlewareOptions<TContext>) => Promise<{ context: TOut }>): Useable<TContext, TOut>;
  use<UIn, UOut>(u: Useable<UIn, UOut>): ServerBuilder<Prettify<TContext & UOut>>;
}
```

The widening happens through `next()`, which is overloaded: `next()` keeps the context, `next({ context: TAdd })` returns `Prettify<TContext & TAdd>`. A middleware's `TOut` is inferred from what it passes to `next`, so returning `next({ context: { user } })` makes `user` visible downstream with no annotation. `os.use(a).use(b)` concatenates the runtime `middlewares` arrays and intersects their out-contexts; the result is reusable because every builder is immutable.

At runtime middleware is fully untyped (`RuntimeMiddleware`): the typed `fn` is stored by reference (`fn as unknown as RuntimeMiddleware`) and the type machinery is erased.

### 3. `implement(contract)` — a builder tree mirroring the contract

`implement` walks the contract (`buildTree`) and returns `Implemented<C>`, a recursive type that maps each leaf to an `ImplementedProcedureBuilder<P, InitialContext<P>>` and each nested router to a record of the same. Each leaf exposes `.use()` (widen context) and `.handler()` (terminal). The initial context of a leaf is computed from its *kind*:

```ts
type InitialContext<P> = Prettify<ProcedureBaseContext & KindContext<InferKind<P>>>;
// ProcedureBaseContext = { headers; requestId }
// KindContext<"reactive"> = { db: DatabaseReader }; <"mutation"> = { db: DatabaseWriter };
// <"analytical"> = { db: DatabaseReader; sql: SqlTag }; <"action"> = ActionRunner
```

`.handler(fn)` types `fn` as `(o: { ctx: TContext; input: InferInput<P>; errors: ErrorConstructors<InferErrors<P>> }) => MaybePromise<InferOutput<P>>` and returns a `RegisteredProcedure` carrying `{ def, path, middlewares, handler }`. So the contract drives input, output, kind-context, *and* the error-constructor surface (`ErrorConstructors<E>` = declared errors as typed constructors + the six built-ins `UNAUTHORIZED`/`FORBIDDEN`/`NOT_FOUND`/`CONFLICT`/`BAD_REQUEST`/`INTERNAL`).

`executeProcedure(proc, baseContext, input)` runs the onion: a recursive `dispatch(i, ctx)` calls middleware `i`, whose `next` calls `dispatch(i+1, { ...ctx, ...added })`; at `i === middlewares.length` it runs the handler. Post-`next()` code unwinds after the handler (so a logging middleware wraps the whole call). Calling `next()` twice in one middleware throws.

### 4. Client — `createClient<typeof contract>` is a Proxy; types are inference-only

`Client<Node>` mirrors the contract to a tree of `ProcedureClient<P>`, with terminals whose signatures are derived from the contract type: `call(InferInput<P>): Promise<InferOutput<P>>`, `subscribe`, `key`/`queryKey`, `queryOptions`, `mutationOptions`, `infiniteOptions`. Inputless procedures get an optional `input` via `InputArg<P> = [InferInput<P>] extends [void] ? { input?: undefined } : { input: InferInput<P> }`.

At runtime there is no contract object at all — `createProxy` is a recursive `Proxy`: unknown property accesses extend the `path` array (cached), and the seven terminal names short-circuit to real functions (HTTP `rpcCall`, or `sync.subscribe`). Query keys are hierarchical `[[...path], { type, input }]`. The contract is *purely* a type parameter here, satisfying "type-only import, no codegen" (`docs/ARCHITECTURE.md` §3.6).

### 5. `Doc<T>` via module augmentation, not `typeof schema` — breaking the cycle

`Doc<TableName>` resolves against an augmentable interface, with a safe fallback when not augmented:

```ts
export interface PulseDataModel {} // augmented by `pulse gen`
export type Doc<T extends string> = T extends keyof PulseDataModel
  ? PulseDataModel[T]
  : Prettify<SystemFields<T> & Record<string, unknown>>;
```

`pulse gen` emits a `declare module "@pulse/schema" { interface PulseDataModel { ... } }` block containing **literal** member types per table (see `packages/examples-chat/src/_generated/dataModel.ts`), e.g. `messages: { _id: Id<"messages">; _creationTime: number; authorId: Id<"users">; ...; editedAt?: number }`. It deliberately does **not** emit `extends DataModelFromSchema<typeof schema>`, because that would route the data model back through `typeof schema` (built from `v`, whose `v.doc` references `Doc`), creating the cycle `v → doc → Doc → PulseDataModel → typeof schema → v`. Emitting concrete members breaks the cycle. (`DataModelFromSchema`/`DocFromSchema` still exist in `schema.ts` as the *derivation* the generator computes from `describe()`, but are not wired into the live `Doc` lookup.)

## Alternatives Considered

- **Codegen a typed `api` object for client calls.** Rejected for the call surface: it adds a build step, a generated file to keep in sync, and a place for drift between contract and client. Pure inference from `typeof contract` removes the artifact entirely. We *do* keep codegen for `Doc<T>` (see below) — the asymmetry is deliberate and called out in `docs/ARCHITECTURE.md` §3.1.
- **Carry inferred TS types (`Infer<V>`) in the contract generics instead of the validator type.** Rejected: storing the validator (`Input = V`) and inferring lazily in `InferInput`/`InferOutput` keeps the contract generics cheap, lets `.input()` re-thread a new validator without recomputing, and keeps `def`/types in lockstep. Eager inference would bloat every intermediate builder type.
- **Mutable builder (mutate `def`/`middlewares` in place).** Rejected: reusable base builders (`os.use(logged).use(authed)`) demand that applying `.use()` cannot disturb the shared builder. Immutability (spread a new `def`, concat a new `middlewares` array) is what makes `authedBase` safe to reuse across many leaves.
- **Track only the middleware's context *delta* (`TAdd`) in `Useable`.** Rejected: composing deltas requires repeatedly intersecting and re-`infer`-ing across `.use()` chains, which is fragile and order-sensitive. Tracking the *full* out-context (`TOut`) reduces composition to one intersection per step and one `infer` at the use site (see the `~out?: TOut` comment in `builder.ts`).
- **Derive `Doc<T>` from `DataModelFromSchema<typeof schema>`.** Rejected because of the type cycle through `v.doc`. The literal-augmentation approach is the fix and is the *reason this decision is grouped with the contract/inference work*. The cost is a generated file, accepted.
- **A real contract object at the client at runtime (walk it to build callables).** Unnecessary: the Proxy reconstructs paths from property access, so the client ships zero contract bytes and stays decoupled from validator runtime.

## Consequences

**Pros**
- One source of truth (the contract) types the client, the handlers, the kind-specific `ctx`, and the error constructors — with no generated client file and no manual DTOs.
- Middleware composition is sound and reusable: requirements (`$context`) and additions (`next({ context })`) both flow into types automatically; immutable builders make bases shareable.
- The client is fully decoupled (`import type` only, Proxy runtime), so swapping transports or adding terminals doesn't touch the contract.
- The `Doc<T>` cycle is structurally impossible to reintroduce as long as `pulse gen` emits literals.

**Cons / costs later**
- Heavy reliance on conditional/`infer` types: error messages on a mistyped handler or middleware can be opaque, and deep contract nesting or long `.use()` chains can stress the type-checker. The `Prettify` helper is sprinkled deliberately to keep hovers readable, which is itself a maintenance tax.
- Runtime is almost entirely `as`-casts at the type boundary (`fn as RuntimeMiddleware`, Proxy returns `as Client<C>`, `executeProcedure` casts `input`/`errors` to `never`). The compiler does *not* guard the runtime walk; correctness there rests on tests, not types.
- Two artifacts must stay in lockstep that the type system can't enforce: the generated `PulseDataModel` augmentation vs. the actual `defineSchema`, and (per the Status note) the doc's `RouterClient`/`InferInputs` names vs. the code's `Client`/`InferInput`. A reader following `docs/ARCHITECTURE.md` verbatim will hit naming mismatches.
- The phantom `"~types"` / `~in` / `~out` fields are load-bearing for inference but invisible at runtime; a contributor "cleaning up unused fields" could silently break inference.

## Testing Decisions

Verification is through the **public surface** of each package, not internals, with two complementary axes:

- **Runtime behavior** (vitest, existing prior art):
  - `packages/server/src/builder.test.ts` builds a real contract with `oc`, `implement`s it, attaches middleware via `os.middleware`/`os.use`, and asserts the *observable* onion order (`["before", "handler", "after"]`), context propagation (`ctx.traced === true`, composed `ctx.a`/`ctx.b`), the resolved handler result, and the recorded `proc.path`/`proc.def.kind`. A good test here drives `executeProcedure` end-to-end rather than poking at `dispatch`.
  - `packages/client/src/client.test.ts` exercises the Proxy through `createClient`: that `.call()` POSTs the right `{ path, input }` to `/rpc`, that `.key()`/`.queryKey()` produce the hierarchical keys, and that an error envelope rejects with a typed `PulseClientError`. It injects a `fetch` mock — the seam — and treats the client as `Record<string, any>` at runtime, deliberately separating runtime from type checks.
- **Type-level inference** is asserted in `packages/examples-chat` (the client test comments "full type inference is covered in examples-chat"). The good shape of an inference test is a *compile-only* assertion: build the real chat contract, `createClient<typeof contract>()`, and let `tsc` reject a wrong-typed `.call()` input or a handler returning the wrong output. This is the canonical way to verify "no codegen, pure inference" — if it compiles with the expected types it passes; there is nothing to assert at runtime.
- **The cycle fix** is verified simply by the fact that `@pulse/schema` + `examples-chat/_generated/dataModel.ts` type-check together: if `Doc` were rerouted through `typeof schema`, `tsc` would report a circular reference. A regression guard is "augment `PulseDataModel`, reference `Doc<"messages">` in a handler, and confirm fields are precise (not the open-record fallback)."

## Out of Scope / Deferred

- **`executeProcedure` wiring into the engine.** The executor exists and is unit-tested, but the doc/runtime path that feeds it real `baseContext` (instrumented `db`, `sql`, action runners) and routes by kind is covered by decisions 01/03 and the engine milestones, not here.
- **Client transport, sync, and local-first** (`rpcCall`, `SyncClient`, offline queue, optimistic overlay/rebase) — these are decision 07; this ADR only covers the *type derivation* and the Proxy skeleton.
- **`pulse gen` itself.** The CLI that emits the `PulseDataModel` augmentation is not yet built (the example file is hand-written "until the CLI lands in M7"). Schema→DDL migration and `describe()`-driven generation are deferred.
- **Naming reconciliation** between `docs/ARCHITECTURE.md` (`RouterClient`, `InferInputs`/`InferOutputs`, `const os = implement(...)`) and the implemented `Client`/`InferInput`/`InferOutput`/`InferKind`/`InferErrors` and `implement` returning an anonymous tree. Either rename in code or update the spec; left open.
- **Input validation enforcement at the boundary.** The contract carries validators and `parse`, but where/whether the engine calls `input.parse()` before a handler runs is owned by the runtime decisions, not this one.
