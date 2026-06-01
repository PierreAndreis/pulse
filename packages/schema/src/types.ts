/**
 * Core validator types for Pulse.
 *
 * A `Validator` carries three things:
 *  - a runtime `parse` for validation at the RPC boundary,
 *  - a `describe()` JSON form used by schema → DDL codegen and the wire format,
 *  - a phantom type parameter `Output` that drives static inference (`Infer`).
 *
 * The phantom is stored under a `unique symbol` key so it never collides with a
 * real field and is erased at runtime (we always construct via a cast).
 */

export type Prettify<T> = { [K in keyof T]: T[K] } & {};

declare const PHANTOM: unique symbol;

/** Whether a validator may be absent (used for object field optionality). */
export type Optionality = "required" | "optional";

export interface ParseContext {
  /** Field/array path to the value being parsed, for error messages. */
  path: (string | number)[];
}

export class ValidationError extends Error {
  constructor(
    public readonly path: (string | number)[],
    message: string,
  ) {
    const where = path.length ? ` at \`${path.join(".")}\`` : "";
    super(`${message}${where}`);
    this.name = "ValidationError";
  }
}

/** Serializable description of a validator — consumed by DDL/codegen later. */
export type ValidatorDescription =
  | { kind: "string" }
  | { kind: "number" }
  | { kind: "boolean" }
  | { kind: "null" }
  | { kind: "any" }
  | { kind: "literal"; value: string | number | boolean }
  | { kind: "id"; table: string }
  | { kind: "array"; element: ValidatorDescription }
  | { kind: "object"; fields: Record<string, ValidatorDescription>; optional: string[] }
  | { kind: "union"; members: ValidatorDescription[] }
  | { kind: "optional"; inner: ValidatorDescription }
  | { kind: "doc"; table: string }
  | { kind: "collab" };

export interface Validator<Output = unknown, Opt extends Optionality = "required"> {
  readonly kind: ValidatorDescription["kind"];
  readonly optionality: Opt;
  /** Phantom carrier for the inferred TS type — never present at runtime. */
  readonly [PHANTOM]: Output;
  parse(value: unknown, ctx?: ParseContext): Output;
  describe(): ValidatorDescription;
}

/** Any validator, used in generic bounds. */
export type AnyValidator = Validator<any, Optionality>;

/** Extract the static type a validator produces. */
export type Infer<V> = V extends Validator<infer Output, Optionality> ? Output : never;

/** Keys of a field record whose validators have the given optionality. */
type FieldKeysByOpt<F, Opt extends Optionality> = {
  [K in keyof F]: F[K] extends Validator<unknown, infer O> ? (O extends Opt ? K : never) : never;
}[keyof F];

/** Infer an object type from a record of validators, honoring optional fields. */
export type InferObjectType<F extends Record<string, AnyValidator>> = Prettify<
  { [K in FieldKeysByOpt<F, "required">]: Infer<F[K]> } & {
    [K in FieldKeysByOpt<F, "optional">]?: Infer<F[K]>;
  }
>;

/** Internal constructor — assigns the phantom via cast. */
export function makeValidator<Output, Opt extends Optionality = "required">(def: {
  kind: ValidatorDescription["kind"];
  optionality?: Opt;
  parse: (value: unknown, ctx: ParseContext) => Output;
  describe: () => ValidatorDescription;
}): Validator<Output, Opt> {
  return {
    kind: def.kind,
    optionality: (def.optionality ?? "required") as Opt,
    parse(value: unknown, ctx: ParseContext = { path: [] }): Output {
      return def.parse(value, ctx);
    },
    describe: def.describe,
  } as Validator<Output, Opt>;
}
