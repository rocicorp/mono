import {assertObject, throwInvalidType} from './asserts';
import {skipAssertJSONValue} from './config.js';
import {hasOwn} from './has-own';

/** The values that can be represented in JSON */
export type JSONValue =
  | null
  | string
  | boolean
  | number
  | Array<JSONValue>
  | JSONObject;

/**
 * A JSON object. We allow undefined values because in TypeScript there is no
 * way to express optional missing properties vs properties with the value
 * `undefined`.
 */
export type JSONObject = Partial<{[key: string]: JSONValue}>;

/** Like [[JSONValue]] but deeply readonly */
export type ReadonlyJSONValue =
  | null
  | string
  | boolean
  | number
  | ReadonlyArray<ReadonlyJSONValue>
  | ReadonlyJSONObject;

/** Like [[JSONObject]] but deeply readonly */
export type ReadonlyJSONObject = Partial<{
  readonly [key: string]: ReadonlyJSONValue;
}>;

/**
 * Checks deep equality of two JSON value with (almost) same semantics as
 * `JSON.stringify`. The only difference is that with `JSON.stringify` the
 * ordering of the properties in an object/map/dictionary matters. In
 * [[deepEqual]] the following two values are consider equal, even though the
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
  //eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
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

export function deepClone(value: ReadonlyJSONValue): JSONValue {
  const seen: Array<ReadonlyJSONObject | ReadonlyArray<ReadonlyJSONValue>> = [];
  return internalDeepClone(value, seen);
}

export function internalDeepClone(
  value: ReadonlyJSONValue,
  seen: Array<ReadonlyJSONObject | ReadonlyArray<ReadonlyJSONValue>>,
): JSONValue {
  switch (typeof value) {
    case 'boolean':
    case 'number':
    case 'string':
    case 'undefined':
      return value;
    case 'object': {
      if (value === null) {
        return null;
      }
      if (seen.includes(value)) {
        throw new Error('Cyclic object');
      }
      seen.push(value);
      if (Array.isArray(value)) {
        const rv = value.map(v => internalDeepClone(v, seen));
        seen.pop();
        return rv;
      }

      const obj: JSONObject = {};

      for (const k in value) {
        if (hasOwn(value, k)) {
          const v = (value as ReadonlyJSONObject)[k];
          if (v !== undefined) {
            obj[k] = internalDeepClone(v, seen);
          }
        }
      }
      seen.pop();
      return obj;
    }

    default:
      throw new Error(`Invalid type: ${typeof value}`);
  }
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
export function getSizeOfValue(value: ReadonlyJSONValue): number {
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
            const v = val[k];
            if (v !== undefined) {
              sum += getSizeOfValue(k) + getSizeOfValue(v);
            }
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
      const val = v[k];
      // we allow undefined values because in TypeScript there is no way to
      // express optional missing properties vs properties with the value
      // undefined.
      if (val !== undefined) {
        assertJSONValue(v[k]);
      }
    }
  }
}

function assertJSONArray(v: unknown[]): asserts v is JSONValue[] {
  for (let i = 0; i < v.length; i++) {
    const val = v[i];
    if (val !== undefined) {
      assertJSONValue(val);
    }
  }
}
