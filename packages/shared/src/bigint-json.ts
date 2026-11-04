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

/**
 * Parses JSON strings that may contain arbitrarily large integers. Integers
 * larger than {@link Number.MAX_SAFE_INTEGER} are deserialized as a `bigint`.
 */
export function parse(
  str: string,
  reviver?: (k: string, v: unknown) => unknown,
): JSONValue {
  return customParse(str, reviver, numberParser);
}

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
function customSerializer(_: string, v: any, type: string) {
  if (type === 'bigint') return v.toString();
  // An integer beyond the safe range is a rounded double if it is a number, so
  // it is written in a form that parse() keeps as a number. As digits it would
  // come back as a bigint, and not even the same value, since a double prints
  // only the digits that identify it: 2^62 is 4611686018427388000.
  if (type === 'number' && Number.isInteger(v) && !Number.isSafeInteger(v)) {
    return v.toExponential();
  }
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
