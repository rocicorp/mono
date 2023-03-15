import type {JSONType} from 'reflect-protocol';
import {hasOwn} from 'shared';

export function deepClone(value: JSONType): JSONType {
  const seen: Array<JSONType | ReadonlyArray<JSONType>> = [];
  return internalDeepClone(value, seen);
}

export function internalDeepClone(
  value: JSONType,
  seen: Array<JSONType | ReadonlyArray<JSONType>>,
): JSONType {
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

      const obj: JSONType = {};

      for (const k in value) {
        if (hasOwn(value, k)) {
          const v = (value as Record<string, JSONType>)[k];
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
