import {compareUTF8} from 'compare-utf8';
import type {ReadonlyJSONValue} from 'replicache';
import * as valita from 'shared/valita.js';
import {assertMapValues as valitaAssertMapValues} from '../util/valita.js';

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

export async function putEntry<T extends ReadonlyJSONValue>(
  durable: DurableObjectStorage,
  key: string,
  value: T,
  options: DurableObjectPutOptions,
): Promise<void> {
  await durable.put(key, value, options);
}

export async function delEntry(
  durable: DurableObjectStorage,
  key: string,
  options: DurableObjectPutOptions,
): Promise<void> {
  await durable.delete(key, options);
}
