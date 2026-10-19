/**
 * Background for using `json-custom-numbers`:
 *
 * https://neon.tech/blog/parsing-json-from-postgres-in-js
 */
import {
  parse as customParse,
  stringify as customStringify,
} from 'json-custom-numbers';
import * as v from './valita.ts';

function numberParser(_: unknown, v: string) {
  const n = +v;
  if (n >= Number.MIN_SAFE_INTEGER && n <= Number.MAX_SAFE_INTEGER) return n;
  if (v.includes('.') || v.includes('e') || v.includes('E')) {
    return n;
  }
  return BigInt(v);
}

// Variant of postgres.JSONValue adapted to include bigints
export type JSONValue =
  | null
  | string
  | number
  | bigint
  | boolean
  | readonly JSONValue[]
  | JSONObject;

export type JSONObject = {readonly [prop: string]: JSONValue | undefined};

export const jsonValueSchema: v.Type<JSONValue> = v.lazy(() => {
  const jsonObjectSchema = v.readonly(v.record(jsonValueSchema));

  return v.union(
    v.null(),
    v.string(),
    v.number(),
    v.bigint(),
    v.boolean(),
    v.readonly(v.array(jsonValueSchema)),
    jsonObjectSchema,
  );
});

export const jsonObjectSchema = v.readonly(v.record(jsonValueSchema));

// Integers of up to 15 digits are safe, so only a run of 16 or more digits can
// be an integer that needs the (much slower) custom number parsing.
const UNSAFE_INTEGER_DIGITS = 16;

function isDigit(c: number): boolean {
  return c >= 48 && c <= 57;
}

/**
 * Whether `str` has a run of 16 or more digits. Every such run covers one of
 * every 16 consecutive indices, so only every 16th char needs to be checked and
 * a digit there is expanded to its full run. This is several times faster than
 * `/\d{16}/`, which restarts at every digit of a shorter run.
 *
 * A run inside a JSON string also matches; that merely takes the slow path.
 */
export function hasUnsafeIntegerDigits(str: string): boolean {
  const n = str.length;
  for (let i = UNSAFE_INTEGER_DIGITS - 1; i < n; i += UNSAFE_INTEGER_DIGITS) {
    if (!isDigit(str.charCodeAt(i))) {
      continue;
    }
    let start = i;
    while (start > 0 && isDigit(str.charCodeAt(start - 1))) {
      start--;
    }
    let end = i + 1;
    while (end < n && isDigit(str.charCodeAt(end))) {
      end++;
    }
    if (end - start >= UNSAFE_INTEGER_DIGITS) {
      return true;
    }
    // The next run starts after `end`, and covers one of every 16 indices from
    // there on.
    i = end - 1;
  }
  return false;
}

/**
 * Parses JSON strings that may contain arbitrarily large integers. Integers
 * larger than {@link Number.MAX_SAFE_INTEGER} are deserialized as a `bigint`.
 */
export function parse(
  str: string,
  reviver?: (k: string, v: unknown) => unknown,
): JSONValue {
  return hasUnsafeIntegerDigits(str)
    ? customParse(str, reviver, numberParser)
    : JSON.parse(str, reviver);
}

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
function customSerializer(_: string, v: any, type: string) {
  if (type === 'bigint') return v.toString();
}

/**
 * Stringifies objects to JSON, supporting objects containing bigint values.
 * Note that the resulting JSON string may not be deserializable by
 * all environments, but it is supported by Postgres. The string should be
 * deserialized with the corresponding {@link parse} method that will represent
 * large numbers as bigints. From there it is up to the application to suitably
 * handle bigints passed to downstream logic.
 */
export function stringify(
  obj: unknown,
  replacer?:
    | (string | number)[]
    | ((key: string, value: unknown) => unknown)
    | null,
  indent?: string | number,
) {
  return customStringify(obj, replacer, indent, customSerializer);
}

export const BigIntJSON = {
  parse,
  stringify,
} as const;
