import type {DurableObjectStorage} from '@cloudflare/workers-types';
import {env, runInDurableObject} from 'cloudflare:test';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {expect} from 'vitest';

export function runWithDurableObjectStorage<R>(
  fn: (storage: DurableObjectStorage) => R | Promise<R>,
): Promise<R> {
  const {runnerDO} = env;
  const id = runnerDO.newUniqueId();
  const stub = runnerDO.get(id);
  return runInDurableObject(stub, (_, {storage}) => fn(storage));
}

export async function initStorage(
  storage: DurableObjectStorage,
  entries: Record<string, ReadonlyJSONValue>,
) {
  await storage.deleteAll();
  await storage.put(entries);
}

export async function expectStorage(
  storage: DurableObjectStorage,
  entries: Record<string, ReadonlyJSONValue>,
  prefix = '',
) {
  const actual = await storage.list({prefix});
  expect(actual).toEqual(new Map(Object.entries(entries)));
}
