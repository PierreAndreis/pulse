import type { CollabField, Doc, Id } from "./model.js";
import {
  type AnyValidator,
  type Infer,
  type InferObjectType,
  type Optionality,
  type ParseContext,
  type Validator,
  makeValidator,
  ValidationError,
} from "./types.js";

function fail(ctx: ParseContext, message: string): never {
  throw new ValidationError(ctx.path, message);
}

function child(ctx: ParseContext, key: string | number): ParseContext {
  return { path: [...ctx.path, key] };
}

const string = (): Validator<string> =>
  makeValidator({
    kind: "string",
    parse: (value, ctx) => {
      if (typeof value !== "string") fail(ctx, `expected string, got ${typeof value}`);
      return value;
    },
    describe: () => ({ kind: "string" }),
  });

const number = (): Validator<number> =>
  makeValidator({
    kind: "number",
    parse: (value, ctx) => {
      if (typeof value !== "number" || Number.isNaN(value))
        fail(ctx, `expected number, got ${typeof value}`);
      return value;
    },
    describe: () => ({ kind: "number" }),
  });

const boolean = (): Validator<boolean> =>
  makeValidator({
    kind: "boolean",
    parse: (value, ctx) => {
      if (typeof value !== "boolean") fail(ctx, `expected boolean, got ${typeof value}`);
      return value;
    },
    describe: () => ({ kind: "boolean" }),
  });

const nullValidator = (): Validator<null> =>
  makeValidator({
    kind: "null",
    parse: (value, ctx) => {
      if (value !== null) fail(ctx, `expected null`);
      return null;
    },
    describe: () => ({ kind: "null" }),
  });

const any = (): Validator<any> =>
  makeValidator({
    kind: "any",
    parse: (value) => value,
    describe: () => ({ kind: "any" }),
  });

const literal = <const T extends string | number | boolean>(value: T): Validator<T> =>
  makeValidator({
    kind: "literal",
    parse: (got, ctx) => {
      if (got !== value) fail(ctx, `expected literal ${JSON.stringify(value)}`);
      return got as T;
    },
    describe: () => ({ kind: "literal", value }),
  });

const id = <const Table extends string>(table: Table): Validator<Id<Table>> =>
  makeValidator({
    kind: "id",
    parse: (value, ctx) => {
      if (typeof value !== "string") fail(ctx, `expected id<${table}> (string)`);
      return value as Id<Table>;
    },
    describe: () => ({ kind: "id", table }),
  });

const array = <V extends AnyValidator>(element: V): Validator<Infer<V>[]> =>
  makeValidator({
    kind: "array",
    parse: (value, ctx) => {
      if (!Array.isArray(value)) fail(ctx, `expected array`);
      return value.map((item, i) => element.parse(item, child(ctx, i))) as Infer<V>[];
    },
    describe: () => ({ kind: "array", element: element.describe() }),
  });

const object = <F extends Record<string, AnyValidator>>(
  fields: F,
): Validator<InferObjectType<F>> =>
  makeValidator({
    kind: "object",
    parse: (value, ctx) => {
      if (typeof value !== "object" || value === null || Array.isArray(value))
        fail(ctx, `expected object`);
      const record = value as Record<string, unknown>;
      const out: Record<string, unknown> = {};
      for (const [key, validator] of Object.entries(fields)) {
        const present = key in record;
        if (!present) {
          if (validator.optionality === "optional") continue;
          fail(child(ctx, key), `missing required field`);
        }
        out[key] = validator.parse(record[key], child(ctx, key));
      }
      return out as InferObjectType<F>;
    },
    describe: () => ({
      kind: "object",
      fields: Object.fromEntries(
        Object.entries(fields).map(([k, val]) => [k, val.describe()]),
      ),
      optional: Object.entries(fields)
        .filter(([, val]) => val.optionality === "optional")
        .map(([k]) => k),
    }),
  });

const union = <Members extends [AnyValidator, ...AnyValidator[]]>(
  ...members: Members
): Validator<Infer<Members[number]>> =>
  makeValidator({
    kind: "union",
    parse: (value, ctx) => {
      for (const member of members) {
        try {
          return member.parse(value, ctx) as Infer<Members[number]>;
        } catch {
          // try next
        }
      }
      fail(ctx, `value matched no union member`);
    },
    describe: () => ({ kind: "union", members: members.map((m) => m.describe()) }),
  });

const optional = <V extends AnyValidator>(
  inner: V,
): Validator<Infer<V> | undefined, "optional"> =>
  makeValidator<Infer<V> | undefined, "optional">({
    kind: "optional",
    optionality: "optional",
    parse: (value, ctx) => {
      if (value === undefined) return undefined;
      return inner.parse(value, ctx) as Infer<V>;
    },
    describe: () => ({ kind: "optional", inner: inner.describe() }),
  });

/**
 * Reference to a table's stored document type. Used in contract outputs, e.g.
 * `oc.output(v.array(v.doc("messages")))`. Resolves to `Doc<Table>` (precise
 * once `pulse gen` augments `PulseDataModel`, generic otherwise).
 */
const doc = <const Table extends string>(table: Table): Validator<Doc<Table>> =>
  makeValidator({
    kind: "doc",
    parse: (value, ctx) => {
      if (typeof value !== "object" || value === null) fail(ctx, `expected doc<${table}>`);
      return value as Doc<Table>;
    },
    describe: () => ({ kind: "doc", table }),
  });

/**
 * A collaborative (CRDT) document field, backed by Yjs and merged server-side.
 * Use this for data where concurrent/offline edits to the SAME value should
 * MERGE rather than clobber — rich text, lists, boards, canvases. The stored
 * value is opaque (a Yjs binary state); on the client you bind it to an editor
 * via `useCollab` / `usePulseEditor`, never touching the raw bytes. Do NOT use
 * for data needing invariants (uniqueness, FK, balances) — use normal fields,
 * which stay server-authoritative + serializable.
 */
const collab = (): Validator<CollabField> =>
  makeValidator({
    kind: "collab",
    // Over the wire a collab field's state/updates are base64 strings; the typed
    // surface is an opaque handle, so apps never see the encoding.
    parse: (value) => value as CollabField,
    describe: () => ({ kind: "collab" }),
  });

export const v = {
  string,
  number,
  boolean,
  null: nullValidator,
  any,
  literal,
  id,
  array,
  object,
  union,
  optional,
  doc,
  collab,
};

export type { Optionality };
