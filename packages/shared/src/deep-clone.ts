import {hasOwn} from './has-own.ts';
import type {JSONValue, ReadonlyJSONValue} from './json.ts';

export function deepClone(value: ReadonlyJSONValue): JSONValue {
  const seen: Array<ReadonlyJSONValue> = [];
  return internalDeepClone(value, seen);
}

/**
 * Like {@linkcode deepClone}, but values that are not plain objects or arrays
 * (e.g. `Date` or other class instances produced by column codecs) are treated
 * as opaque leaves and returned by reference rather than recursed into. Plain
 * JSON clones identically to {@linkcode deepClone}.
 */
export function deepCloneWithInstances<T>(value: T): T {
  return internalDeepCloneWithInstances(value, []) as T;
}

function internalDeepCloneWithInstances(
  value: unknown,
  seen: Array<unknown>,
): unknown {
  if (typeof value !== 'object' || value === null) {
    return value;
  }
  if (seen.includes(value)) {
    throw new Error('Cyclic object');
  }
  if (Array.isArray(value)) {
    seen.push(value);
    const rv = value.map(v => internalDeepCloneWithInstances(v, seen));
    seen.pop();
    return rv;
  }
  // Only recurse into plain objects. Anything with a non-Object prototype
  // (Date, Temporal, branded class instances, ...) is an opaque codec leaf.
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) {
    return value;
  }
  seen.push(value);
  const obj: Record<string, unknown> = {};
  for (const k in value) {
    if (hasOwn(value, k)) {
      const v = (value as Record<string, unknown>)[k];
      if (v !== undefined) {
        obj[k] = internalDeepCloneWithInstances(v, seen);
      }
    }
  }
  seen.pop();
  return obj;
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
