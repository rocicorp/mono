import {compareUTF8} from 'compare-utf8';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as valita from 'shared/src/valita.js';
import {assertMapValues as valitaAssertMapValues} from '../util/valita.js';
import {assert} from 'shared/src/asserts.js';

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
  return new Map(
    [...values].map(([key, value]) => [key, valita.parse(value, schema)]),
  );
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

  valitaAssertMapValues(result, schema);
  return result;
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
