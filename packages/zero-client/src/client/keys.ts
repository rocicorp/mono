import {h128} from '../../../shared/src/hash.ts';
import * as v from '../../../shared/src/valita.ts';
import {AGGREGATE_TABLE_PREFIX} from '../../../zero-protocol/src/aggregate-table.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {MutationID} from '../../../zero-protocol/src/mutation-id.ts';
import {primaryKeyValueSchema} from '../../../zero-protocol/src/primary-key.ts';
import {AGGREGATE_PAYLOAD_COLUMNS} from '../../../zql/src/ivm/aggregate.ts';

// The replicated row-version column the server stamps on synced rows; not part
// of an aggregate's key. (Mirrors zero-cache's ZERO_VERSION_COLUMN_NAME, which
// is server-side; inlined here to avoid a server dependency.)
export const ROW_VERSION_COLUMN = '_0_version';

const DESIRED_QUERIES_KEY_PREFIX = 'd/';
export const GOT_QUERIES_KEY_PREFIX = 'g/';
export const ENTITIES_KEY_PREFIX = 'e/';
/**
 * Synced aggregate results live outside `e/`. A client from before them loads
 * every `e/` row into a table of its schema and fails to open a database that
 * has rows of an `aggregate:` table there, while it skips keys outside `e/`.
 * Keeping them apart lets both versions share a database (STORAGE_VERSION).
 */
export const AGGREGATES_KEY_PREFIX = 'a/';
/** The prefixes of the rows the IVM sources hold, in key order. */
export const ROW_KEY_PREFIXES = [
  AGGREGATES_KEY_PREFIX,
  ENTITIES_KEY_PREFIX,
] as const;
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
  return rowKey(ENTITIES_KEY_PREFIX, tableName, primaryKey, value);
}

function rowKey(
  prefix: string,
  tableName: string,
  primaryKey: CompoundKey,
  value: Row,
): string {
  if (primaryKey.length === 1) {
    return (
      prefix +
      tableName +
      '/' +
      v.parse(value[primaryKey[0]], primaryKeyValueSchema)
    );
  }

  const values = primaryKey.map(k => v.parse(value[k], primaryKeyValueSchema));
  const str = JSON.stringify(values);

  const idSegment = u128ToDecimal(h128(str));
  return prefix + tableName + '/' + idSegment;
}

const E19 = 10n ** 19n;

/**
 * `String(x)` for an unsigned 128-bit `x`: the decimal form entity keys have
 * always used for compound primary keys, so it must not change.
 *
 * Chromium 148 (V8 14.8) takes ~4us to stringify a bigint wider than 64 bits
 * in base 10, against ~100ns in node 24, Firefox and WebKit; bigints that fit
 * in 64 bits stay fast everywhere. Splitting `x` into base-10^19 pieces, each
 * under 2^64, keeps every conversion on that fast path: ~35x faster in
 * Chromium and within ~10% elsewhere.
 */
function u128ToDecimal(x: bigint): string {
  if (x < E19) {
    return String(x);
  }
  const t = x / E19;
  const low = String(x % E19).padStart(19, '0');
  if (t < E19) {
    return String(t) + low;
  }
  return String(t / E19) + String(t % E19).padStart(19, '0') + low;
}

/**
 * The source (table) name of an `e/` or `a/` row key. An `a/` key leaves out
 * the `aggregate:` every aggregate table name starts with.
 */
export function sourceNameFromKey(key: string): string {
  // Both prefixes are two characters long.
  const slash = key.indexOf('/', ENTITIES_KEY_PREFIX.length);
  const name = key.slice(ENTITIES_KEY_PREFIX.length, slash);
  return key.startsWith(AGGREGATES_KEY_PREFIX)
    ? AGGREGATE_TABLE_PREFIX + name
    : name;
}

/**
 * The Replicache key for a synthetic aggregate row: `a/`, the table name
 * without its `aggregate:`, and the row's key. The key columns
 * are every column except the payload (`_0_value`, plus `_0_sum`/`_0_count`)
 * and the row-version, sorted so a `put` (full
 * row), a `del`/`update` (row key only), and an optimistic delta from a mutation
 * all produce the same key. Works for both top-level (`{_0_key: 0, …}`) and
 * relationship (`{…childFields, …}`) rows.
 */
export function aggregateRowKey(tableName: string, row: Row): string {
  const keyColumns = Object.keys(row)
    .filter(k => !AGGREGATE_PAYLOAD_COLUMNS.has(k) && k !== ROW_VERSION_COLUMN)
    .sort();
  return rowKey(
    AGGREGATES_KEY_PREFIX,
    tableName.slice(AGGREGATE_TABLE_PREFIX.length),
    keyColumns as unknown as CompoundKey,
    row,
  );
}
