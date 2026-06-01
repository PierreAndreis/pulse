import {
  type AnyContractProcedure,
  type ContractRouter,
  type InferErrors,
  type InferInput,
  type InferKind,
  type InferOutput,
  type ProcedureDef,
  isProcedure,
} from "@pulse/contract";
import type { Prettify } from "@pulse/schema";
import {
  type ErrorConstructors,
  makeErrors,
} from "./errors.js";
import type { KindContext } from "./context.js";

export type MaybePromise<T> = T | Promise<T>;

/** Context available to every handler/middleware regardless of kind. */
export interface ProcedureBaseContext {
  headers: Headers;
  requestId: string;
}

// ── Middleware ───────────────────────────────────────────────────────────────

export interface MiddlewareNextResult<TContext> {
  context: TContext;
}

export interface MiddlewareNextFn<TContext> {
  (): Promise<MiddlewareNextResult<TContext>>;
  <TAdd extends Record<string, unknown>>(opts: {
    context: TAdd;
  }): Promise<MiddlewareNextResult<Prettify<TContext & TAdd>>>;
}

export interface MiddlewareOptions<TContext> {
  context: TContext;
  input: unknown;
  path: string[];
  next: MiddlewareNextFn<TContext>;
  errors: ErrorConstructors<Record<never, never>>;
}

/** Runtime (untyped) middleware shape used by the executor. */
export type RuntimeMiddleware = (opts: {
  context: Record<string, unknown>;
  input: unknown;
  path: string[];
  errors: ErrorConstructors<Record<never, never>>;
  next: (opts?: { context?: Record<string, unknown> }) => Promise<{ context: Record<string, unknown> }>;
}) => Promise<{ context: Record<string, unknown> }>;

/**
 * A composable unit applied with `.use()`. `TIn` is the context it requires;
 * `TOut` is the *full* context it yields downstream (its inputs plus additions).
 * Tracking the full out-context (rather than just the delta) makes inference a
 * single `infer` and composes cleanly under intersection.
 */
export interface Useable<TIn, TOut> {
  readonly "~in"?: (x: TIn) => void;
  readonly "~out"?: TOut;
  readonly middlewares: RuntimeMiddleware[];
}

// ── Standalone builder (`os`) ─────────────────────────────────────────────────

export interface ServerBuilder<TContext> extends Useable<Record<never, never>, TContext> {
  /** Declare the initial context this builder's middleware requires. */
  $context<C extends Record<string, unknown>>(): ServerBuilder<C>;
  /** Author a middleware. Its output context is inferred from `next()`. */
  middleware<TOut extends Record<string, unknown>>(
    fn: (opts: MiddlewareOptions<TContext>) => Promise<MiddlewareNextResult<TOut>>,
  ): Useable<TContext, TOut>;
  /** Apply a middleware/builder, widening the context by its output. */
  use<UIn, UOut>(u: Useable<UIn, UOut>): ServerBuilder<Prettify<TContext & UOut>>;
}

function makeServerBuilder<TContext>(middlewares: RuntimeMiddleware[]): ServerBuilder<TContext> {
  const builder: ServerBuilder<TContext> = {
    middlewares,
    $context<C extends Record<string, unknown>>() {
      return makeServerBuilder<C>(middlewares);
    },
    middleware<TOut extends Record<string, unknown>>(
      fn: (opts: MiddlewareOptions<TContext>) => Promise<MiddlewareNextResult<TOut>>,
    ): Useable<TContext, TOut> {
      const runtime = fn as unknown as RuntimeMiddleware;
      return { middlewares: [...middlewares, runtime] };
    },
    use<UIn, UOut>(u: Useable<UIn, UOut>) {
      return makeServerBuilder<Prettify<TContext & UOut>>([...middlewares, ...u.middlewares]);
    },
  };
  return builder;
}

/** The base builder for authoring middleware and reusable base builders. */
export const os: ServerBuilder<Record<never, never>> = makeServerBuilder([]);

// ── Implemented procedures ────────────────────────────────────────────────────

export interface HandlerOptions<P extends AnyContractProcedure, TContext> {
  ctx: TContext;
  input: InferInput<P>;
  errors: ErrorConstructors<InferErrors<P>>;
}

export interface RegisteredProcedure<P extends AnyContractProcedure = AnyContractProcedure> {
  readonly "~brand": "pulse.procedure";
  readonly def: ProcedureDef;
  readonly path: string[];
  readonly middlewares: RuntimeMiddleware[];
  readonly handler: (opts: HandlerOptions<P, Record<string, unknown>>) => unknown;
}

export interface ImplementedProcedureBuilder<P extends AnyContractProcedure, TContext> {
  use<UIn, UOut>(
    u: Useable<UIn, UOut>,
  ): ImplementedProcedureBuilder<P, Prettify<TContext & UOut>>;
  handler(
    fn: (opts: HandlerOptions<P, TContext>) => MaybePromise<InferOutput<P>>,
  ): RegisteredProcedure<P>;
}

/** Initial handler context for a procedure of a given kind. */
type InitialContext<P extends AnyContractProcedure> = Prettify<
  ProcedureBaseContext & KindContext<InferKind<P>>
>;

/** Recursively mirror a contract into a tree of procedure builders. */
export type Implemented<Node> = Node extends AnyContractProcedure
  ? ImplementedProcedureBuilder<Node, InitialContext<Node>>
  : Node extends ContractRouter
    ? { [K in keyof Node]: Implemented<Node[K]> }
    : never;

function makeProcedureBuilder(
  def: ProcedureDef,
  path: string[],
  middlewares: RuntimeMiddleware[],
): ImplementedProcedureBuilder<AnyContractProcedure, Record<string, unknown>> {
  return {
    use(u) {
      return makeProcedureBuilder(def, path, [...middlewares, ...u.middlewares]) as never;
    },
    handler(fn) {
      const registered: RegisteredProcedure = {
        "~brand": "pulse.procedure",
        def,
        path,
        middlewares,
        handler: fn as RegisteredProcedure["handler"],
      };
      return registered;
    },
  };
}

function buildTree(node: unknown, path: string[]): unknown {
  if (isProcedure(node)) {
    return makeProcedureBuilder(node.def, path, []);
  }
  const out: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(node as Record<string, unknown>)) {
    out[key] = buildTree(child, [...path, key]);
  }
  return out;
}

/** Bind a builder tree to a contract. Leaves expose `.use()`/`.handler()`. */
export function implement<C extends ContractRouter>(contract: C): Implemented<C> {
  return buildTree(contract, []) as Implemented<C>;
}

// ── Runtime executor (used by the engine in M1+) ──────────────────────────────

/** Run the middleware onion, then the handler, returning its result. */
export async function executeProcedure(
  proc: RegisteredProcedure,
  baseContext: Record<string, unknown>,
  input: unknown,
): Promise<unknown> {
  const errors = makeErrors(proc.def.errors);
  let dispatched = -1;
  let handlerOutput: unknown;

  // Onion: the handler runs at the innermost point (when the last middleware
  // calls `next()`), so each middleware's post-`next()` code unwinds *after* the
  // handler has produced its result.
  async function dispatch(
    i: number,
    ctx: Record<string, unknown>,
  ): Promise<{ context: Record<string, unknown> }> {
    if (i <= dispatched) throw new Error("next() called multiple times in one middleware");
    dispatched = i;
    if (i === proc.middlewares.length) {
      handlerOutput = await proc.handler({
        ctx,
        input: input as never,
        errors: errors as never,
      });
      return { context: ctx };
    }
    const mw = proc.middlewares[i]!;
    return mw({
      context: ctx,
      input,
      path: proc.path,
      errors,
      next: async (opts) => dispatch(i + 1, { ...ctx, ...(opts?.context ?? {}) }),
    });
  }

  await dispatch(0, baseContext);
  return handlerOutput;
}
