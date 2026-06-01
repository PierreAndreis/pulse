import type { AnyValidator, Infer, Prettify } from "@pulse/schema";

/**
 * The kind of a procedure. Determines runtime, determinism rules, and routing:
 *  - `reactive`   — read-only query, subscribable over SSE (V8 isolate)
 *  - `mutation`   — read-write, one serializable tx, invalidates subscriptions
 *  - `analytical` — heavy read-only query routed to the OLAP replica, non-reactive
 *  - `action`     — side-effecting, non-deterministic, runs in the Node worker pool
 */
export type ProcedureKind = "reactive" | "mutation" | "analytical" | "action";

/** A declared error: a name plus a validator for its `data` payload. */
export type ErrorMap = Record<string, { data: AnyValidator }>;

/** Runtime form of a contract procedure (the type lives in the generics). */
export interface ProcedureDef {
  kind: ProcedureKind;
  input?: AnyValidator;
  output?: AnyValidator;
  errors: ErrorMap;
}

export interface ContractProcedure<
  Kind extends ProcedureKind,
  Input,
  Output,
  Errors extends ErrorMap,
> {
  /** Phantom carrier so the generics are inferable. Not present at runtime. */
  readonly "~types": { kind: Kind; input: Input; output: Output; errors: Errors };
  /** Runtime definition (kind + validators + errors). */
  readonly def: ProcedureDef;
  input<V extends AnyValidator>(validator: V): ContractProcedure<Kind, V, Output, Errors>;
  output<V extends AnyValidator>(validator: V): ContractProcedure<Kind, Input, V, Errors>;
  errors<E extends ErrorMap>(
    errs: E,
  ): ContractProcedure<Kind, Input, Output, Prettify<Errors & E>>;
}

export type AnyContractProcedure = ContractProcedure<ProcedureKind, any, any, ErrorMap>;

/** A contract is a (possibly nested) record of procedures. */
export interface ContractRouter {
  [key: string]: AnyContractProcedure | ContractRouter;
}

function makeProcedure<
  Kind extends ProcedureKind,
  Input,
  Output,
  Errors extends ErrorMap,
>(def: ProcedureDef): ContractProcedure<Kind, Input, Output, Errors> {
  return {
    def,
    input(validator) {
      return makeProcedure({ ...def, input: validator });
    },
    output(validator) {
      return makeProcedure({ ...def, output: validator });
    },
    errors(errs) {
      return makeProcedure({ ...def, errors: { ...def.errors, ...errs } });
    },
  } as ContractProcedure<Kind, Input, Output, Errors>;
}

function start<Kind extends ProcedureKind>(
  kind: Kind,
): ContractProcedure<Kind, undefined, undefined, Record<never, never>> {
  return makeProcedure({ kind, errors: {} });
}

/** Contract builder. Begin every procedure by choosing its kind. */
export const oc = {
  reactive: () => start("reactive"),
  mutation: () => start("mutation"),
  analytical: () => start("analytical"),
  action: () => start("action"),
};

// ── Inference helpers (used by @pulse/server and @pulse/client) ──────────────

export type InferKind<P> = P extends ContractProcedure<infer K, any, any, ErrorMap>
  ? K
  : never;

export type InferInput<P> = P extends ContractProcedure<ProcedureKind, infer I, any, ErrorMap>
  ? I extends AnyValidator
    ? Infer<I>
    : void
  : never;

export type InferOutput<P> = P extends ContractProcedure<ProcedureKind, any, infer O, ErrorMap>
  ? O extends AnyValidator
    ? Infer<O>
    : void
  : never;

export type InferErrors<P> = P extends ContractProcedure<ProcedureKind, any, any, infer E>
  ? E
  : never;

/** True if a node is a procedure (vs a nested router). */
export function isProcedure(node: unknown): node is AnyContractProcedure {
  return (
    typeof node === "object" &&
    node !== null &&
    "def" in node &&
    typeof (node as { def?: unknown }).def === "object"
  );
}
