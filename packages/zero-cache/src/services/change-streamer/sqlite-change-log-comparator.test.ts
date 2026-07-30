/**
 * Property tests for the comparator's pure functions. The store-facing
 * behavior — real Postgres against a real SQLite log — lives in
 * `sqlite-change-log-comparator.pg.test.ts`; nothing here needs a database.
 */

import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import type {ShardID} from '../../types/shards.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer.ts';
import {
  digestCatchupTransactions,
  isSampledForCompare,
  normalizeChangeJSON,
} from './sqlite-change-log-comparator.ts';

describe('change-streamer/sqlite-change-log-comparator/properties', () => {
  type TxKind = 'complete' | 'orphan' | 'rollback';

  const streamScenario = fc.record({
    txs: fc.array(
      fc.record({
        width: fc.integer({min: 0, max: 5}),
        kind: fc.constantFrom<TxKind>('complete', 'orphan', 'rollback'),
      }),
      {maxLength: 8},
    ),
    // Rows dropped from the front of the stream, so it can begin
    // mid-transaction — possibly beheading more than one transaction.
    truncateHead: fc.nat({max: 3}),
    // Two chunkings of the same rows, cycled over the stream.
    chunkSizesA: fc.array(fc.integer({min: 1, max: 7}), {
      minLength: 1,
      maxLength: 4,
    }),
    chunkSizesB: fc.array(fc.integer({min: 1, max: 7}), {
      minLength: 1,
      maxLength: 4,
    }),
  });

  function buildRows(
    txs: {width: number; kind: TxKind}[],
  ): WatermarkedChange[] {
    const rows: WatermarkedChange[] = [];
    txs.forEach(({width, kind}, i) => {
      const w = `w${String(i + 1).padStart(2, '0')}`;
      rows.push([
        w,
        'begin',
        `["begin",{"tag":"begin"},{"commitWatermark":"${w}"}]`,
      ]);
      for (let k = 0; k < width; k++) {
        rows.push([
          w,
          'insert' as ChangeTag,
          `["data",{"tag":"insert","row":${k}}]`,
        ]);
      }
      if (kind === 'complete') {
        rows.push([
          w,
          'commit',
          `["commit",{"tag":"commit"},{"watermark":"${w}"}]`,
        ]);
      } else if (kind === 'rollback') {
        rows.push([w, 'rollback', `["rollback",{"tag":"rollback"}]`]);
      } // orphan: no terminal row — a torn or corrupt remnant.
    });
    return rows;
  }

  async function* chunked(
    rows: WatermarkedChange[],
    sizes: number[],
  ): AsyncIterable<WatermarkedChange[]> {
    let i = 0;
    let s = 0;
    while (i < rows.length) {
      const size = sizes[s++ % sizes.length];
      yield rows.slice(i, i + size);
      i += size;
    }
  }

  test('digests are a pure function of the rows, not of their batching', async () => {
    await fc.assert(
      fc.asyncProperty(
        streamScenario,
        async ({txs, truncateHead, chunkSizesA, chunkSizesB}) => {
          const rows = buildRows(txs).slice(truncateHead);

          const a = await digestCatchupTransactions(chunked(rows, chunkSizesA));
          const b = await digestCatchupTransactions(chunked(rows, chunkSizesB));
          // Batch boundaries carry no meaning: any two chunkings of the same
          // served rows digest identically.
          expect(a).toEqual(b);

          // The entries are exactly the groupwise rules the comparator
          // depends on: committed groups digest as complete iff their begin
          // survived; rolled-back groups vanish; everything else is orphan
          // output, always incomplete. Row counts are what was served.
          const groups = new Map<string, WatermarkedChange[]>();
          for (const row of rows) {
            const group = groups.get(row[0]) ?? [];
            group.push(row);
            groups.set(row[0], group);
          }
          for (const [w, group] of groups) {
            const tags = new Set(group.map(([, tag]) => tag));
            const entry = a.get(w);
            if (tags.has('rollback')) {
              expect(entry).toBeUndefined();
            } else {
              expect(entry).toMatchObject({
                watermark: w,
                rows: group.length,
                complete: tags.has('commit') && tags.has('begin'),
              });
            }
          }
          expect(a.size).toBe(
            [...groups.values()].filter(
              group => !group.some(([, tag]) => tag === 'rollback'),
            ).length,
          );
        },
      ),
      {numRuns: 100},
    );
  });

  test('sampling is stable, bounded, and monotone in the percentage', () => {
    fc.assert(
      fc.property(
        fc.record({
          appID: fc.string({minLength: 1, maxLength: 8}),
          shardNum: fc.nat({max: 1000}),
          watermark: fc.hexaString({minLength: 2, maxLength: 10}),
          p1: fc.integer({min: 0, max: 100}),
          p2: fc.integer({min: 0, max: 100}),
        }),
        ({appID, shardNum, watermark, p1, p2}) => {
          const shard: ShardID = {appID, shardNum};
          const lo = Math.min(p1, p2);
          const hi = Math.max(p1, p2);
          // Stable: a retried comparison selects the same transactions.
          expect(isSampledForCompare(shard, watermark, hi)).toBe(
            isSampledForCompare(shard, watermark, hi),
          );
          // Monotone: raising the percentage only ever adds transactions,
          // so a canary ramp-up keeps everything it was already comparing.
          if (isSampledForCompare(shard, watermark, lo)) {
            expect(isSampledForCompare(shard, watermark, hi)).toBe(true);
          }
          expect(isSampledForCompare(shard, watermark, 0)).toBe(false);
          expect(isSampledForCompare(shard, watermark, 100)).toBe(true);
        },
      ),
    );
  });

  test('normalization is idempotent', () => {
    fc.assert(
      fc.property(fc.oneof(fc.json(), fc.string()), value => {
        const once = normalizeChangeJSON(value);
        expect(normalizeChangeJSON(once)).toBe(once);
      }),
    );
  });
});
