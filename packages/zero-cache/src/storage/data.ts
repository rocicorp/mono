import {assert} from 'shared/src/asserts.js';
import * as valita from 'shared/src/valita.js';
import type {JSONValue} from '../types/bigint-json.js';
import type {
  DurableObjectListOptions,
  FakeDurableObjectStorage,
} from '../test/fake-do.js';

export async function getEntry<T extends JSONValue>(
  durable: FakeDurableObjectStorage,
  key: string,
  schema: valita.Type<T>,
): Promise<T | undefined> {
  const value = await durable.get(key);
  if (value === undefined) {
    return undefined;
  }
  return valita.parse(value, schema);
}

// https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#transactional-storage-api
export const MAX_ENTRIES_TO_GET = 128;

export async function getEntries<T extends JSONValue>(
  durable: FakeDurableObjectStorage,
  keys: string[],
  schema: valita.Type<T>,
): Promise<Map<string, T>> {
  assert(
    keys.length <= MAX_ENTRIES_TO_GET,
    `Cannot get more than ${MAX_ENTRIES_TO_GET} entries`,
  );
  const values = await durable.get(keys);
  return validateOrNormalize(values, schema);
}

export async function listEntries<T extends JSONValue>(
  durable: FakeDurableObjectStorage,
  schema: valita.Type<T>,
  options: DurableObjectListOptions,
): Promise<Map<string, T>> {
  const result = await durable.list(options);
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

export function putEntry<T extends JSONValue>(
  durable: FakeDurableObjectStorage,
  key: string,
  value: T,
): Promise<void> {
  return durable.put(key, value);
}

export function delEntry(
  durable: FakeDurableObjectStorage,
  key: string,
): Promise<void> {
  return durable.delete(key).then(() => undefined);
}
