import {hasOwn} from './has-own.ts';

/**
 * Recursively removes functions, Maps, and Sets from an object for JSON serialization.
 */
export function removeFunctions<T>(value: T, seen = new Set<unknown>()): T {
  // Handle primitives and null
  if (value === null || typeof value !== 'object') {
    return value;
  }

  // Circular reference detection
  if (seen.has(value)) {
    return undefined as T;
  }
  seen.add(value);

  try {
    // Handle arrays
    if (Array.isArray(value)) {
      return value.map(item => removeFunctions(item, seen)) as T;
    }

    // Handle plain objects - filter out non-serializable types
    const result: Record<string, unknown> = {};

    for (const key in value) {
      if (hasOwn(value, key)) {
        const val = (value as Record<string, unknown>)[key];

        // Skip functions and Map/Set instances
        if (
          typeof val === 'function' ||
          val instanceof Map ||
          val instanceof Set
        ) {
          continue;
        }

        // Recursively process nested values
        result[key] = removeFunctions(val, seen);
      }
    }

    return result as T;
  } finally {
    seen.delete(value);
  }
}
