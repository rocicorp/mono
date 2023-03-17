import {hasOwn} from './has-own.js';
import type {JSONValue, ReadonlyJSONValue} from './json.js';

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
