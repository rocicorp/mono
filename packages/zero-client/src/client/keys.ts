import {h64WithReverse} from '../../../shared/src/h64-with-reverse.js';
import type {
  PrimaryKey,
  PrimaryKeyValueRecord,
} from '../../../zero-protocol/src/primary-key.js';

export const CLIENTS_KEY_PREFIX = 'c/';
export const DESIRED_QUERIES_KEY_PREFIX = 'd/';
export const GOT_QUERIES_KEY_PREFIX = 'g/';
export const ENTITIES_KEY_PREFIX = 'e/';

export function toClientsKey(clientID: string): string {
  return CLIENTS_KEY_PREFIX + clientID;
}

export function toDesiredQueriesKey(clientID: string, hash: string): string {
  return DESIRED_QUERIES_KEY_PREFIX + clientID + '/' + hash;
}

export function desiredQueriesPrefixForClient(clientID: string): string {
  return DESIRED_QUERIES_KEY_PREFIX + clientID + '/';
}

export function toGotQueriesKey(hash: string): string {
  return GOT_QUERIES_KEY_PREFIX + hash;
}

/**
 * This returns a new array if the array is not already sorted.
 */
function maybeSort<T>(arr: readonly T[]): readonly T[] {
  for (let i = 1; i < arr.length; i++) {
    if (arr[i] < arr[i - 1]) {
      return [...arr].sort();
    }
  }
  return arr;
}

export function toPrimaryKeyString(
  tableName: string,
  primaryKey: PrimaryKey,
  id: PrimaryKeyValueRecord,
): string {
  if (primaryKey.length === 1) {
    return ENTITIES_KEY_PREFIX + tableName + '/' + id[primaryKey[0]];
  }

  // TODO: Assert that PrimaryKey is always sorted at a higher level.
  const sorted = maybeSort(primaryKey);

  const arr = sorted.map(k => id[k]);
  const str = JSON.stringify(arr);

  const idSegment = h64WithReverse(str);
  return ENTITIES_KEY_PREFIX + tableName + '/' + idSegment;
}
