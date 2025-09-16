/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-base-to-string, @typescript-eslint/restrict-template-expressions */
import {hasOwn} from './has-own.ts';
import type {JSONValue, ReadonlyJSONValue} from './json.ts';

export function deepClone(value: ReadonlyJSONValue): JSONValue {
  const seen: Array<ReadonlyJSONValue> = [];
  return internalDeepClone(value, seen);
}

export function internalDeepClone(
  value: ReadonlyJSONValue,
  seen: Array<ReadonlyJSONValue>,
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

      const obj: JSONValue = {};

      // eslint-disable-next-line @typescript-eslint/no-for-in-array
      for (const k in value) {
        if (hasOwn(value, k)) {
          const v = (value as Record<string, ReadonlyJSONValue>)[k];
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
