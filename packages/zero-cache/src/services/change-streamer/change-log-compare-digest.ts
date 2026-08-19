/**
 * Pure building blocks for comparing catchup output between the Postgres and
 * SQLite change logs: which transactions to sample, and how to reduce a
 * catchup range to a single comparable digest.
 *
 * Nothing here touches either store. The comparator service supplies the
 * batches.
 */

import {createHash} from 'node:crypto';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {ShardID} from '../../types/shards.ts';
import {extractChangeSubstring} from './change-log-codec.ts';
import {ChangeLogTransactionHasher} from './change-log-transaction-hash.ts';
import type {WatermarkedChange} from './change-streamer.ts';

/** Selects a stable sample by shard and watermark. */
export function isSampledForCompare(
  shard: ShardID,
  watermark: string,
  percent: number,
): boolean {
  if (percent >= 100) {
    return true;
  }
  if (percent <= 0) {
    return false;
  }
  const digest = createHash('sha256')
    .update(`${shard.appID}/${shard.shardNum}:${watermark}`)
    .digest();
  return digest.readUInt32BE(0) % 100 < percent;
}

/**
 * Normalizes JSON text before hashing.
 *
 * SQLite preserves the source text. Postgres can change its formatting.
 * Invalid JSON remains unchanged.
 */
export function normalizeChangeJSON(change: string): string {
  try {
    return BigIntJSON.stringify(BigIntJSON.parse(change));
  } catch {
    return change;
  }
}

export type CatchupRangeDigest = {
  readonly digest: string;
  readonly rows: number;
  /** Normalized bytes hashed. This is the payload cost of the range. */
  readonly bytes: number;
  /** A row or byte limit was reached before the closing commit. */
  readonly limitReached: boolean;
};

/**
 * Builds one ordered digest for a catchup range.
 *
 * The row position makes missing, extra, changed, and reordered rows change the digest.
 * The digest omits `precommit` because a commit already includes its watermark.
 *
 * The read stops at `maxRows` or at `maxBytes`, whichever comes first.
 */
export async function digestCatchupRange(
  batches: AsyncIterable<readonly WatermarkedChange[]>,
  throughWatermark: string,
  maxRows: number,
  maxBytes: number,
): Promise<CatchupRangeDigest> {
  const hasher = new ChangeLogTransactionHasher();
  let rows = 0;
  let bytes = 0;
  let servedClosingCommit = false;

  for await (const batch of batches) {
    for (const [watermark, tag, json] of batch) {
      if (rows === maxRows || bytes >= maxBytes) {
        return {digest: hasher.digest(), rows, bytes, limitReached: true};
      }
      // Measure the stored text before parsing it. A row wider than the
      // whole budget never fits, and normalizing it first would pay the
      // cost that this limit exists to avoid.
      if (json.length > maxBytes) {
        return {digest: hasher.digest(), rows, bytes, limitReached: true};
      }
      const change = normalizeChangeJSON(extractChangeSubstring(json, tag));
      hasher.add({
        watermark,
        pos: rows,
        tag,
        change,
        precommit: null,
      });
      rows++;
      // Count the normalized length so both stores measure the same logical
      // payload. Stored encodings differ between them. Normalized ones do not.
      bytes += change.length;
      if (tag === 'commit' && watermark === throughWatermark) {
        servedClosingCommit = true;
      }
    }
  }
  return {
    digest: hasher.digest(),
    rows,
    bytes,
    limitReached:
      (rows === maxRows || bytes >= maxBytes) && !servedClosingCommit,
  };
}
