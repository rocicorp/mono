import {assert} from 'shared/src/asserts.js';
import {
  versionFromLexi,
  versionToLexi,
  type LexiVersion,
} from 'zero-cache/src/types/lexi-version.js';
import {Change} from '../schema/change.js';

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

export function toLexiVersion(
  lsn: LSN,
  type: RecordType = 'commit',
): LexiVersion {
  const parts = lsn.split('/');
  assert(parts.length === 2, `Malformed LSN: "${lsn}"`);
  const high = BigInt(`0x${parts[0]}`);
  const low = BigInt(`0x${parts[1]}`);
  // Shift by 2 bits and add offset().
  const val = (high << 34n) + (low << 2n) + offset(type);
  return versionToLexi(val);
}

export function fromLexiVersion(lexi: LexiVersion): LSN {
  const val = versionFromLexi(lexi);
  const high = val >> 34n;
  const low = (val >> 2n) & 0xffffffffn;
  return `${high.toString(16).toUpperCase()}/${low.toString(16).toUpperCase()}`;
}

/**
 * Postgres defines the "Log sequence number (LSN)" as a value that
 * "increases monotonically with each new WAL record":
 *
 * https://www.postgresql.org/docs/current/glossary.html#:~:text=Log%20sequence%20number%20(LSN)
 *
 * and a WAL record being a "low-level description of an individual data change".
 *
 * Unfortunately, 'begin' and 'commit' transaction markers are not technically
 * "data changes" (i.e. they are not DML statements), and thus do not always
 * have their own LSN.
 *
 * In fact, a 'begin' message always has the same LSN as its transaction's first
 * DML statement. Moreover, executing commands in quick succession can result
 * in a 'commit', the next 'begin', and the subsequent data change all sharing
 * the same LSN:
 *
 *
 * ```json
 * "8F/38B017C0": {
 *   "tag": "insert",
 *   "relation": {
 *   ...
 * }
 * "8F/38B01E18": {
 *   "tag": "commit",
 *   "flags": 0,
 *   "commitLsn": "0000008F/38B01DE8",
 *   "commitEndLsn": "0000008F/38B01E18",
 *   "commitTime": "BigInt(1726014579672237)"
 * },
 * "8F/38B01E18": {
 *   "tag": "begin",
 *   "commitLsn": "0000008F/38B01E98",
 *   "commitTime": "BigInt(1726014580746075)",
 *   "xid": 494599
 * },
 * "8F/38B01E18": {
 *   "tag": "insert",
 *   "relation": {
 *   ...
 * }
 * "8F/38B01EC8": {
 *   "tag": "commit",
 *   "flags": 0,
 *   "commitLsn": "0000008F/38B01E98",
 *   "commitEndLsn": "0000008F/38B01EC8",
 *   "commitTime": "BigInt(1726014580746075)"
 * },
 * ```
 *
 * This renders the LSN unsuitable as a watermark on its own. Even attaching
 * the position of the message within the transaction (with 'begin' starting at 0)
 * does not work since the 'commit' from the previous transaction would be sorted
 * after the 'begin' from the next one if they shared the same LSN.
 *
 * The scheme to convert an LSN to a monotonic value is based on the
 * characteristic that like-LSN records always follow a specific order:
 * - `commit`
 * - `begin`
 * - DML statement (i.e. `insert`, `update`, `delete`, `truncate`)
 *
 * A monotonic watermark is thus computed from the LSN by:
 * - Shifting the LSN by 2 bits.
 * - Adding 1 for `begin` records.
 * - Adding 2 for DML records.
 *
 * This ensures that the resulting watermarks are strictly monotonic and
 * sorted in stream order.
 */

function offset(type: RecordType): bigint {
  switch (type) {
    case 'commit':
      return 0n;
    case 'begin':
      return 1n;
    default:
      return 2n;
  }
}
