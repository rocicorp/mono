import type {
  DurableObjectGetOptions,
  DurableObjectListOptions,
  DurableObjectPutOptions,
  DurableObjectStorage,
} from '@cloudflare/workers-types';
import {compareUTF8} from 'compare-utf8';
import {assert} from 'shared/src/asserts.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as valita from 'shared/src/valita.js';

export async function getEntry<T extends ReadonlyJSONValue>(
  durable: DurableObjectStorage,
  key: string,
  schema: valita.Type<T>,
  options: DurableObjectGetOptions,
): Promise<T | undefined> {
  const value = await durable.get(key, options);
  if (value === undefined) {
    return undefined;
  }
  return valita.parse(value, schema);
}

// https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#transactional-storage-api
export const MAX_ENTRIES_TO_GET = 128;

export async function getEntries<T extends ReadonlyJSONValue>(
  durable: DurableObjectStorage,
  keys: string[],
  schema: valita.Type<T>,
  options: DurableObjectGetOptions,
): Promise<Map<string, T>> {
  assert(
    keys.length <= MAX_ENTRIES_TO_GET,
    `Cannot get more than ${MAX_ENTRIES_TO_GET} entries`,
  );
  const values = await durable.get(keys, options);
  return validateOrNormalize(values, schema);
}

export async function listEntries<T extends ReadonlyJSONValue>(
  durable: DurableObjectStorage,
  schema: valita.Type<T>,
  options: DurableObjectListOptions,
): Promise<Map<string, T>> {
  let result = await durable.list(options);

  // `durable.list()` on CF prod returns keys UTF-8 sorted.
  // When running in miniflare, this is JS/UTF-16 collation.
  if (typeof MINIFLARE !== 'undefined') {
    const entries = Array.from(result);
    entries.sort((a, b) => compareUTF8(a[0], b[0]));
    result = new Map(entries);
  }

  return validateOrNormalize(result, schema);
}

/**
 * Validates that all values in the `map` conform to the given `schema`,
 * in which case the `map` is returned as is, or creates a new Map
 * containing normalized values produced when parsing with the `schema`.
 *
 * In both cases, the iteration order of the returned Map matches that
 * of the supplied map.
 *
 * If any of the values do not conform to the `schema`, an Error is thrown.
 */
function validateOrNormalize<T>(
  map: Map<string, unknown>,
  schema: valita.Type<T>,
): Map<string, T> {
  let copyNeeded = false;
  for (const [, value] of map) {
    const parsed = valita.parse(value, schema);
    if (parsed !== value) {
      copyNeeded = true;
      break;
    }
  }
  if (!copyNeeded) {
    // Common case: schema validates and leaves objects unchanged. Return the original map.
    return map as Map<string, T>;
  }

  // A copy of the Map is needed if the schema creates new (normalized) objects.
  const normalized = new Map<string, T>();
  for (const [key, value] of map) {
    normalized.set(key, valita.parse(value, schema));
  }
  return normalized;
}

export function putEntry<T extends ReadonlyJSONValue>(
  durable: DurableObjectStorage,
  key: string,
  value: T,
  options: DurableObjectPutOptions,
): Promise<void> {
  return durable.put(key, value, options);
}

export function delEntry(
  durable: DurableObjectStorage,
  key: string,
  options: DurableObjectPutOptions,
): Promise<void> {
  return durable.delete(key, options).then(() => undefined);
}
