import {h64WithReverse} from '../../../shared/src/h64-with-reverse.js';
import type {PrimaryKeyValueRecord} from '../../../zero-protocol/src/primary-key.js';
import type {NormalizedPrimaryKey} from '../../../zql/src/zql/query/normalize-table-schema.js';

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

export function toPrimaryKeyString(
  tableName: string,
  primaryKey: NormalizedPrimaryKey,
  id: PrimaryKeyValueRecord,
): string {
  if (primaryKey.length === 1) {
    return ENTITIES_KEY_PREFIX + tableName + '/' + id[primaryKey[0]];
  }

  const values = primaryKey.map(k => id[k]);
  const str = JSON.stringify(values);

  const idSegment = h64WithReverse(str);
  return ENTITIES_KEY_PREFIX + tableName + '/' + idSegment;
}
