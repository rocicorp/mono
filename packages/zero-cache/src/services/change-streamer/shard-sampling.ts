import {h32} from '../../../../shared/src/hash.ts';
import type {ShardID} from '../../types/shards.ts';

/**
 * Stable percentage sampling keyed by shard and an arbitrary identity: a
 * subscriber's task for read routing, a watermark for catchup comparison.
 *
 * The same key always lands in the same bucket, so raising `percent` only ever
 * adds to the sampled set, and every process in a shard agrees on the set
 * without coordinating.
 */
export function isSampledForShard(
  shard: ShardID,
  key: string,
  percent: number,
): boolean {
  if (percent >= 100) {
    return true;
  }
  if (percent <= 0) {
    return false;
  }
  return h32(`${shard.appID}/${shard.shardNum}:${key}`) % 100 < percent;
}
