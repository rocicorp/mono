import {h128} from '../../../shared/src/hash.ts';
import * as v from '../../../shared/src/valita.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {MutationID} from '../../../zero-protocol/src/mutation-id.ts';
import {primaryKeyValueSchema} from '../../../zero-protocol/src/primary-key.ts';

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

  const idSegment = u128ToDecimal(h128(str));
  return ENTITIES_KEY_PREFIX + tableName + '/' + idSegment;
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

export function sourceNameFromKey(key: string): string {
  const slash = key.indexOf('/', ENTITIES_KEY_PREFIX.length);
  return key.slice(ENTITIES_KEY_PREFIX.length, slash);
}
