import {h128} from '../../../shared/src/hash.ts';
import * as v from '../../../shared/src/valita.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zql/src/ivm/data.ts';
import {primaryKeyValueSchema} from '../../../zero-protocol/src/primary-key.ts';

export const DESIRED_QUERIES_KEY_PREFIX = 'd/';
export const GOT_QUERIES_KEY_PREFIX = 'g/';
export const ENTITIES_KEY_PREFIX = 'e/';

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
  primaryKey: CompoundKey,
  value: Row,
): string {
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

  const idSegment = h128(str);
  return ENTITIES_KEY_PREFIX + tableName + '/' + idSegment;
}

export function sourceNameFromKey(key: string): string {
  const slash = key.indexOf('/', ENTITIES_KEY_PREFIX.length);
  return key.slice(ENTITIES_KEY_PREFIX.length, slash);
}
