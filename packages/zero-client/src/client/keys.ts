import {h64WithReverse} from '../../../shared/src/h64-with-reverse.js';
import * as v from '../../../shared/src/valita.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import {primaryKeyValueSchema} from '../../../zero-protocol/src/primary-key.js';
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
  value: Row,
): string {
  // TODO: The type system should have enforced that the value has valid primary keys.
  // We should maybe tag the row with the primary key so we can enforce this.
  if (primaryKey.length === 1) {
    return (
      ENTITIES_KEY_PREFIX +
      tableName +
      '/' +
      v.parse(value[primaryKey[0]], primaryKeyValueSchema)
    );
  }

  const values = primaryKey.map(k => v.parse(value[k], primaryKeyValueSchema));
  const str = JSON.stringify(values);

  const idSegment = h64WithReverse(str);
  return ENTITIES_KEY_PREFIX + tableName + '/' + idSegment;
}
