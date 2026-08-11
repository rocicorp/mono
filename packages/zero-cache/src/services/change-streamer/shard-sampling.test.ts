import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import type {ShardID} from '../../types/shards.ts';
import {isSampledForShard} from './shard-sampling.ts';

describe('change-streamer/shard-sampling', () => {
  test('sampling is stable, bounded, and monotone in the percentage', () => {
    fc.assert(
      fc.property(
        fc.record({
          appID: fc.string({minLength: 1, maxLength: 8}),
          shardNum: fc.nat({max: 1000}),
          key: fc.hexaString({minLength: 2, maxLength: 10}),
          p1: fc.integer({min: 0, max: 100}),
          p2: fc.integer({min: 0, max: 100}),
        }),
        ({appID, shardNum, key, p1, p2}) => {
          const shard: ShardID = {appID, shardNum};
          const lo = Math.min(p1, p2);
          const hi = Math.max(p1, p2);
          // A retry selects the same keys.
          expect(isSampledForShard(shard, key, hi)).toBe(
            isSampledForShard(shard, key, hi),
          );
          // A larger percentage keeps the keys in a smaller sample.
          if (isSampledForShard(shard, key, lo)) {
            expect(isSampledForShard(shard, key, hi)).toBe(true);
          }
          expect(isSampledForShard(shard, key, 0)).toBe(false);
          expect(isSampledForShard(shard, key, 100)).toBe(true);
        },
      ),
    );
  });

  test('the shard is part of the key, so shards sample independently', () => {
    const keys = Array.from({length: 200}, (_, i) => `task-${i}`);
    const a = keys.filter(key =>
      isSampledForShard({appID: 'zero', shardNum: 0}, key, 25),
    );
    const b = keys.filter(key =>
      isSampledForShard({appID: 'zero', shardNum: 1}, key, 25),
    );
    expect(a).not.toEqual(b);
  });
});
