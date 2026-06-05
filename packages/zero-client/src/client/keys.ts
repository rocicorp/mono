import {h128} from '../../../shared/src/hash.ts';
import * as v from '../../../shared/src/valita.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {MutationID} from '../../../zero-protocol/src/mutation-id.ts';
import {primaryKeyValueSchema} from '../../../zero-protocol/src/primary-key.ts';
import {AGGREGATE_PAYLOAD_COLUMNS} from '../../../zql/src/ivm/aggregate.ts';

// The replicated row-version column the server stamps on synced rows; not part
// of an aggregate's key. (Mirrors zero-cache's ZERO_VERSION_COLUMN_NAME, which
// is server-side; inlined here to avoid a server dependency.)
export const ROW_VERSION_COLUMN = '_0_version';

export const DESIRED_QUERIES_KEY_PREFIX = 'd/';
export const GOT_QUERIES_KEY_PREFIX = 'g/';
export const ENTITIES_KEY_PREFIX = 'e/';
export const MUTATIONS_KEY_PREFIX = 'm/';

export function toDesiredQueriesKey(clientID: string, hash: string): string {
  return DESIRED_QUERIES_KEY_PREFIX + clientID + '/' + hash;
}

export function desiredQueriesPrefixForClient(clientID: string): string {
  return DESIRED_QUERIES_KEY_PREFIX + clientID + '/';
}

export function toGotQueriesKey(hash: string): string {
  return GOT_QUERIES_KEY_PREFIX + hash;
}

export function toMutationResponseKey(mid: MutationID): string {
  return MUTATIONS_KEY_PREFIX + mid.clientID + '/' + mid.id;
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

/**
 * The Replicache key for a synthetic aggregate row. The key columns are every
 * column except the result `value` and the row-version, sorted so a `put` (full
 * row), a `del`/`update` (row key only), and an optimistic delta from a mutation
 * all produce the same key. Works for both top-level (`{'': 0, …}`) and
 * relationship (`{…childFields, …}`) rows.
 */
export function aggregateRowKey(tableName: string, row: Row): string {
  const keyColumns = Object.keys(row)
    .filter(k => !AGGREGATE_PAYLOAD_COLUMNS.has(k) && k !== ROW_VERSION_COLUMN)
    .sort();
  return toPrimaryKeyString(
    tableName,
    keyColumns as unknown as CompoundKey,
    row,
  );
}
