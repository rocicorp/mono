import {expect} from 'vitest';
import type {JSONObject} from '../types/bigint-json.js';
import {FakeDurableObjectStorage} from './fake-do.js';

export function runWithDurableObjectStorage<R>(
  fn: (storage: FakeDurableObjectStorage) => R | Promise<R>,
): R | Promise<R> {
  return fn(new FakeDurableObjectStorage());
}

export async function initStorage(
  storage: FakeDurableObjectStorage,
  entries: Record<string, JSONObject>,
) {
  await storage.deleteAll();
  await storage.put(entries);
}

export async function expectStorage(
  storage: FakeDurableObjectStorage,
  entries: Record<string, JSONObject>,
  prefix = '',
) {
  const actual = await storage.list({prefix});
  expect(actual).toEqual(new Map(Object.entries(entries)));
}
