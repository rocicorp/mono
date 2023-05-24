import {hasOwn} from 'shared/has-own.js';
import type {ReadonlyJSONObject} from './json.js';

const SIZE_TAG = 1;
const SIZE_INT32 = 4;
const SIZE_SMI = 5;
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
          return SIZE_TAG + SIZE_SMI;
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
        let sum: number = 2 * SIZE_TAG + SIZE_INT32;
        for (const k in val) {
          if (hasOwn(val, k)) {
            sum += getSizeOfValue(k) + getSizeOfValue(val[k]);
          }
        }
        return sum;
      }
  }

  throw new Error('invalid value');
}

function isSmi(value: number): boolean {
  return value === (value | 0);
}

const entryFixed = 2 * SIZE_TAG + SIZE_INT32 + SIZE_TAG + SIZE_INT32;

export function getSizeOfEntry<K, V>(key: K, value: V): number {
  // Entries are stored as [key, value, sizeOfEntry]
  return entryFixed + getSizeOfValue(key) + getSizeOfValue(value);
}
