import {assertObject, throwInvalidType} from 'shared/asserts.js';
import {skipAssertJSONValue, skipFreeze, skipFrozenAsserts} from './config.js';
import type {FrozenCookie, Cookie} from './cookies.js';
import {hasOwn} from 'shared/has-own.js';

/** The values that can be represented in JSON */
export type JSONValue =
  | null
  | string
  | boolean
  | number
  | Array<JSONValue>
  | JSONObject;

/**
 * A JSON object. This is a map from strings to JSON values.
 */
export type JSONObject = {[key: string]: JSONValue};

/** Like {@link JSONValue} but deeply readonly */
export type ReadonlyJSONValue =
  | null
  | string
  | boolean
  | number
  | ReadonlyArray<ReadonlyJSONValue>
  | ReadonlyJSONObject;

/** Like {@link JSONObject} but deeply readonly */
export type ReadonlyJSONObject = {
  readonly [key: string]: ReadonlyJSONValue;
};

/**
 * Checks deep equality of two JSON value with (almost) same semantics as
 * `JSON.stringify`. The only difference is that with `JSON.stringify` the
 * ordering of the properties in an object/map/dictionary matters. In
 * {@link deepEqual} the following two values are consider equal, even though the
 * strings JSON.stringify would produce is different:
 *
 * ```js
 * assert(deepEqual(t({a: 1, b: 2}, {b: 2, a: 1}))
 * ```
 */
export function deepEqual(
  a: ReadonlyJSONValue | undefined,
  b: ReadonlyJSONValue | undefined,
): boolean {
  if (a === b) {
    return true;
  }

  if (typeof a !== typeof b) {
    return false;
  }

  switch (typeof a) {
    case 'boolean':
    case 'number':
    case 'string':
      return false;
  }

  // a cannot be undefined here because either a and b are undefined or their
  // types are different.
  // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
  a = a!;

  // 'object'
  if (Array.isArray(a)) {
    if (!Array.isArray(b)) {
      return false;
    }
    if (a.length !== b.length) {
      return false;
    }
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i])) {
        return false;
      }
    }
    return true;
  }

  if (a === null || b === null) {
    return false;
  }

  if (Array.isArray(b)) {
    return false;
  }

  // We know a and b are objects here but type inference is not smart enough.
  a = a as ReadonlyJSONObject;
  b = b as ReadonlyJSONObject;

  // We use for-in loops instead of for of Object.keys() to make sure deepEquals
  // does not allocate any objects.

  let aSize = 0;
  for (const key in a) {
    if (hasOwn(a, key)) {
      if (!deepEqual(a[key], b[key])) {
        return false;
      }
      aSize++;
    }
  }

  let bSize = 0;
  for (const key in b) {
    if (hasOwn(b, key)) {
      bSize++;
    }
  }

  return aSize === bSize;
}

const SIZE_TAG = 1;
const SIZE_INT32 = 4;
const SIZE_DOUBLE = 8;

/**
 * Gives a size of a value. The size is modelled after the size used by
 * Chromium/V8's structuredClone algorithm. It does not match exactly so the
 * size is just an approximation.
 * https://source.chromium.org/chromium/chromium/src/+/main:v8/src/objects/value-serializer.cc;l=102;drc=f0b6f7d12ea47ad7c08fb554f678c1e73801ca36;bpv=1;bpt=1
 * For example we follow JSC/Mozilla for ints and skip the varint encoding.
 *
 * Mozilla does things similarly. Main difference is that there is no varint
 * encoding and every value uses multiples of 64bits
 * https://searchfox.org/mozilla-central/source/js/src/vm/StructuredClone.cpp#94
 *
 * And JSC:
 * https://github.com/WebKit/WebKit/blob/main/Source/WebCore/bindings/js/SerializedScriptValue.cpp#L356
 * - Use 1 byte tag
 * - Numbers are either stored as Int32 or Float6
 */
export function getSizeOfValue(value: unknown): number {
  switch (typeof value) {
    case 'string':
      // Assumes all strings are one byte strings. V8 writes OneByteString and
      // TwoByteString. We could check the string but it would require iterating
      // over all the characters.
      return SIZE_TAG + SIZE_INT32 + value.length;
    case 'number':
      if (isSmi(value)) {
        if (value <= -(2 ** 30) || value >= 2 ** 30 - 1) {
          return SIZE_TAG + 5;
        }
        return SIZE_TAG + SIZE_INT32;
      }
      return SIZE_TAG + SIZE_DOUBLE;
    case 'boolean':
      return SIZE_TAG;
    case 'object':
      if (value === null) {
        return SIZE_TAG;
      }

      if (Array.isArray(value)) {
        let sum = 2 * SIZE_TAG + SIZE_INT32;
        for (const element of value) {
          sum += getSizeOfValue(element);
        }
        return sum;
      }

      {
        const val = value as ReadonlyJSONObject;
        let sum: number = SIZE_TAG;
        for (const k in val) {
          if (hasOwn(val, k)) {
            sum += getSizeOfValue(k) + getSizeOfValue(val[k]);
          }
        }
        return sum + SIZE_INT32 + SIZE_TAG;
      }
  }

  throw new Error('invalid value');
}

function isSmi(value: number): boolean {
  return value === (value | 0);
}

export function assertJSONValue(v: unknown): asserts v is JSONValue {
  if (skipAssertJSONValue) {
    return;
  }
  switch (typeof v) {
    case 'boolean':
    case 'number':
    case 'string':
      return;
    case 'object':
      if (v === null) {
        return;
      }
      if (Array.isArray(v)) {
        return assertJSONArray(v);
      }
      return assertObjectIsJSONObject(v as Record<string, unknown>);
  }
  throwInvalidType(v, 'JSON value');
}

export function assertJSONObject(v: unknown): asserts v is JSONObject {
  assertObject(v);
  assertObjectIsJSONObject(v);
}

function assertObjectIsJSONObject(
  v: Record<string, unknown>,
): asserts v is JSONObject {
  for (const k in v) {
    if (hasOwn(v, k)) {
      assertJSONValue(v[k]);
    }
  }
}

function assertJSONArray(v: unknown[]): asserts v is JSONValue[] {
  for (const item of v) {
    assertJSONValue(item);
  }
}

declare const frozenJSONTag: unique symbol;

/**
 * Used to mark a type as having been frozen.
 */
export type FrozenTag<T> = T & {readonly [frozenJSONTag]: true};

export type FrozenJSONValue =
  | null
  | string
  | boolean
  | number
  | FrozenJSONArray
  | FrozenJSONObject;

type FrozenJSONArray = FrozenTag<ReadonlyArray<FrozenJSONValue>>;

export type FrozenJSONObject = FrozenTag<{
  readonly [key: string]: FrozenJSONValue;
}>;

/**
 * We tag deep frozen objects in debug mode so that we do not have to deep
 * freeze an object more than once.
 */
const deepFrozenObjects = new WeakSet<object>();

/**
 * Recursively freezes the passed in value (mutates it) and returns it.
 *
 * This is controlled by `skipFreeze` which is true in release mode.
 */
export function deepFreeze(v: undefined): undefined;
export function deepFreeze(v: Cookie): FrozenCookie;
export function deepFreeze(v: ReadonlyJSONValue): FrozenJSONValue;
export function deepFreeze(
  v: ReadonlyJSONValue | undefined,
): FrozenJSONValue | undefined;
export function deepFreeze(
  v: ReadonlyJSONValue | undefined,
): FrozenJSONValue | undefined {
  if (skipFreeze) {
    return v as FrozenJSONValue | undefined;
  }

  if (v === undefined) {
    return undefined;
  }

  deepFreezeInternal(v, []);
  return v as FrozenJSONValue;
}

function deepFreezeInternal(v: ReadonlyJSONValue, seen: object[]): void {
  switch (typeof v) {
    case 'boolean':
    case 'number':
    case 'string':
      return;
    case 'object': {
      if (v === null) {
        return;
      }

      if (deepFrozenObjects.has(v)) {
        return;
      }
      deepFrozenObjects.add(v);

      if (seen.includes(v)) {
        throwInvalidType(v, 'Cyclic JSON object');
      }

      seen.push(v);

      Object.freeze(v);
      if (Array.isArray(v)) {
        deepFreezeArray(v, seen);
      } else {
        deepFreezeObject(v as ReadonlyJSONObject, seen);
      }
      seen.pop();
      return;
    }

    default:
      throwInvalidType(v, 'JSON value');
  }
}

function deepFreezeArray(
  v: ReadonlyArray<ReadonlyJSONValue>,
  seen: object[],
): void {
  for (const item of v) {
    deepFreezeInternal(item, seen);
  }
}

function deepFreezeObject(v: ReadonlyJSONObject, seen: object[]): void {
  for (const k in v) {
    if (hasOwn(v, k)) {
      deepFreezeInternal(v[k], seen);
    }
  }
}

export function assertFrozenJSONValue(
  v: unknown,
): asserts v is FrozenJSONValue {
  if (skipFrozenAsserts || skipAssertJSONValue) {
    return;
  }
  switch (typeof v) {
    case 'boolean':
    case 'number':
    case 'string':
      return;
    case 'object':
      if (v === null) {
        return;
      }

      if (isDeepFrozen(v, [])) {
        return;
      }
  }
  throwInvalidType(v, 'JSON value');
}

export function assertDeepFrozen<V>(v: V): asserts v is Readonly<V> {
  if (skipFrozenAsserts) {
    return;
  }

  if (!isDeepFrozen(v, [])) {
    throw new Error('Expected frozen object');
  }
}

/**
 * Recursive deep frozen check.
 *
 * It adds frozen objects to the {@link deepFrozenObjects} WeakSet so that we do
 * not have to check the same object more than once.
 */
export function isDeepFrozen(v: unknown, seen: object[]): boolean {
  switch (typeof v) {
    case 'boolean':
    case 'number':
    case 'string':
      return true;
    case 'object':
      if (v === null) {
        return true;
      }

      if (deepFrozenObjects.has(v)) {
        return true;
      }

      if (!Object.isFrozen(v)) {
        return false;
      }

      if (seen.includes(v)) {
        throwInvalidType(v, 'Cyclic JSON object');
      }

      seen.push(v);

      if (Array.isArray(v)) {
        for (const item of v) {
          if (!isDeepFrozen(item, seen)) {
            seen.pop();
            return false;
          }
        }
      } else {
        for (const k in v) {
          if (
            hasOwn(v, k) &&
            !isDeepFrozen((v as Record<string, unknown>)[k], seen)
          ) {
            seen.pop();
            return false;
          }
        }
      }

      deepFrozenObjects.add(v);
      seen.pop();
      return true;

    default:
      throwInvalidType(v, 'JSON value');
  }
}
