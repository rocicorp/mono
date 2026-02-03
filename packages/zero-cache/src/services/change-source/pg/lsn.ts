import {assert} from '../../../../../shared/src/asserts.ts';
import {
  majorVersionToString,
  stateVersionFromString,
} from '../../../types/state-version.ts';
import type {Change} from '../protocol/current/data.ts';

/**
 * Parsing and conversion utilities for the pg_lsn Type, which represents
 * the "Log Sequence Number" used as a monotonic progress marker for logical
 * replication from a PostgresSQL database.
 *
 * The LSN is a 64-bit integer represented in logical replication as two
 * hexadecimal numbers (up to 8 digits each) separated by a slash. This is
 * converted to a LexiVersion and used as DB-agnostic version in change log,
 * invalidation index, and row version in the tables of the sync replica.
 */
export type LSN = string;

export type RecordType = Change['tag'];

export function toBigInt(lsn: LSN): bigint {
  const parts = lsn.split('/');
  assert(parts.length === 2, `Malformed LSN: "${lsn}"`);
  const high = BigInt(`0x${parts[0]}`);
  const low = BigInt(`0x${parts[1]}`);
  const val = (high << 32n) + low;
  return val;
}

export function toStateVersionString(lsn: LSN): string {
  return majorVersionToString(toBigInt(lsn));
}

/** The LSN is tracked by the `major` component of the StateVersion. */
export function fromStateVersionString(ver: string): LSN {
  const {major} = stateVersionFromString(ver);
  return fromBigInt(major);
}

export function fromBigInt(val: bigint): LSN {
  const high = val >> 32n;
  const low = val & 0xffffffffn;
  return `${high.toString(16).toUpperCase()}/${low.toString(16).toUpperCase()}`;
}
