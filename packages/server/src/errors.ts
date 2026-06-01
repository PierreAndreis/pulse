import type { AnyValidator, Infer } from "@onveloz/pulse-schema";
import type { ErrorMap } from "@onveloz/pulse-contract";

/** A structured, typed error thrown from middleware or handlers. */
export class PulseError<Data = unknown> extends Error {
  constructor(
    public readonly code: string,
    public readonly data: Data,
    message?: string,
  ) {
    super(message ?? code);
    this.name = "PulseError";
  }
}

export interface ErrorThrowOptions {
  message?: string;
}

/** A constructor for a declared error; returns a `PulseError` you then `throw`. */
export type ErrorConstructor<Data> = [Data] extends [void | undefined]
  ? (opts?: ErrorThrowOptions) => PulseError<Data>
  : (data: Data, opts?: ErrorThrowOptions) => PulseError<Data>;

/** Built-in error codes available to every procedure without declaration. */
export interface BuiltinErrors {
  UNAUTHORIZED: (opts?: ErrorThrowOptions) => PulseError<undefined>;
  FORBIDDEN: (opts?: ErrorThrowOptions) => PulseError<undefined>;
  NOT_FOUND: (opts?: ErrorThrowOptions) => PulseError<undefined>;
  CONFLICT: (opts?: ErrorThrowOptions) => PulseError<undefined>;
  BAD_REQUEST: (opts?: ErrorThrowOptions) => PulseError<undefined>;
  INTERNAL: (opts?: ErrorThrowOptions) => PulseError<undefined>;
}

/** Map a contract `ErrorMap` to its constructor surface, plus the built-ins. */
export type ErrorConstructors<E extends ErrorMap> = BuiltinErrors & {
  [K in keyof E]: E[K]["data"] extends AnyValidator
    ? ErrorConstructor<Infer<E[K]["data"]>>
    : never;
};

const BUILTIN_CODES = [
  "UNAUTHORIZED",
  "FORBIDDEN",
  "NOT_FOUND",
  "CONFLICT",
  "BAD_REQUEST",
  "INTERNAL",
] as const;

/** Build the runtime `errors` object handed to middleware/handlers. */
export function makeErrors<E extends ErrorMap>(declared: E): ErrorConstructors<E> {
  const out: Record<string, (...args: unknown[]) => PulseError> = {};
  for (const code of BUILTIN_CODES) {
    out[code] = (opts?: unknown) =>
      new PulseError(code, undefined, (opts as ErrorThrowOptions | undefined)?.message);
  }
  for (const code of Object.keys(declared)) {
    out[code] = (data?: unknown, opts?: unknown) =>
      new PulseError(code, data, (opts as ErrorThrowOptions | undefined)?.message);
  }
  return out as ErrorConstructors<E>;
}
