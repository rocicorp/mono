/**
 * Pure building blocks for comparing catchup output between the Postgres and
 * SQLite change logs: which transactions to sample, and how to reduce a
 * catchup range to a single comparable digest.
 *
 * Nothing here touches either store. The comparator service supplies the
 * batches.
 */

import {createHash} from 'node:crypto';
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

export type CatchupRangeDigest = {
  readonly digest: string;
  readonly rows: number;
  /** Change text hashed. This is the payload cost of the range. */
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
 * The change text is hashed as stored. Both logs write the same
 * `extractChangeSubstring` output, and the Postgres column is `json`, which
 * keeps the input text exactly, so the two sides are byte-comparable without
 * reparsing. Comparing the stored text also catches an encoding difference
 * that a semantic comparison would hide.
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
      // Check the whole stream message first. A row wider than the entire
      // budget never fits, so it is rejected before any per-row work.
      if (json.length > maxBytes) {
        return {digest: hasher.digest(), rows, bytes, limitReached: true};
      }
      const change = extractChangeSubstring(json, tag);
      hasher.add({
        watermark,
        pos: rows,
        tag,
        change,
        precommit: null,
      });
      rows++;
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
