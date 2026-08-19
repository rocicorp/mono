/** Tests pure comparator functions. Integration tests cover both stores. */

import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import type {ShardID} from '../../types/shards.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer.ts';
import {
  digestCatchupRange,
  isSampledForCompare,
  normalizeChangeJSON,
} from './sqlite-change-log-comparator.ts';

describe('change-streamer/sqlite-change-log-comparator/properties', () => {
  type TxKind = 'complete' | 'orphan' | 'rollback';

  /** Isolates the row budget in tests that do not exercise the byte budget. */
  const NO_BYTE_LIMIT = Number.MAX_SAFE_INTEGER;

  const streamScenario = fc.record({
    txs: fc.array(
      fc.record({
        width: fc.integer({min: 0, max: 5}),
        kind: fc.constantFrom<TxKind>('complete', 'orphan', 'rollback'),
      }),
      {minLength: 1, maxLength: 8},
    ),
    // Remove leading rows to start inside a transaction.
    truncateHead: fc.nat({max: 3}),
    // Use two batch layouts for the same rows.
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
      } // Orphan transactions have no terminal row.
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

  const digest = (rows: WatermarkedChange[], sizes: number[] = [3]) =>
    digestCatchupRange(
      chunked(rows, sizes),
      rows.at(-1)?.[0] ?? '',
      rows.length + 1,
      NO_BYTE_LIMIT,
    );

  test('the digest is a pure function of the served rows, not their batching', async () => {
    await fc.assert(
      fc.asyncProperty(
        streamScenario,
        async ({txs, truncateHead, chunkSizesA, chunkSizesB}) => {
          const rows = buildRows(txs).slice(truncateHead);

          // Batch boundaries do not change the digest or row count.
          expect(await digest(rows, chunkSizesA)).toEqual(
            await digest(rows, chunkSizesB),
          );
          expect((await digest(rows, chunkSizesA)).rows).toBe(rows.length);
        },
      ),
      {numRuns: 100},
    );
  });

  test('the digest changes under any single-row corruption', async () => {
    type Corruption = 'drop' | 'mutate' | 'duplicate' | 'move-to-end';

    await fc.assert(
      fc.asyncProperty(
        fc.record({
          txs: streamScenario.map(({txs}) => txs),
          index: fc.nat(),
          kind: fc.constantFrom<Corruption>(
            'drop',
            'mutate',
            'duplicate',
            'move-to-end',
          ),
        }),
        async ({txs, index, kind}) => {
          const rows = buildRows(txs);
          const i = index % rows.length;
          // Moving the only or last row does not change its order.
          fc.pre(kind !== 'move-to-end' || i < rows.length - 1);

          const corrupted = [...rows];
          switch (kind) {
            case 'drop':
              corrupted.splice(i, 1);
              break;
            case 'mutate': {
              const [w, tag, json] = rows[i];
              corrupted[i] = [w, tag, `${json.slice(0, -1)},"extra":1]`];
              break;
            }
            case 'duplicate':
              corrupted.splice(i, 0, rows[i]);
              break;
            case 'move-to-end':
              corrupted.push(...corrupted.splice(i, 1));
              break;
          }

          // Each corruption changes the ordered range digest.
          expect((await digest(corrupted)).digest).not.toBe(
            (await digest(rows)).digest,
          );
        },
      ),
      {numRuns: 200},
    );
  });

  test('the digest ignores JSON formatting, as the two stores differ in it', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({txs: streamScenario.map(({txs}) => txs)}),
        async ({txs}) => {
          const rows = buildRows(txs);
          // Both stores can return the same JSON with different formatting.
          const reformatted = rows.map(
            ([w, tag, json]): WatermarkedChange => [
              w,
              tag,
              json.replaceAll(':', ': ').replaceAll(',', ', '),
            ],
          );
          expect((await digest(reformatted)).digest).toBe(
            (await digest(rows)).digest,
          );
        },
      ),
      {numRuns: 50},
    );
  });

  test('the row cap reports whether the range closed', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          txs: streamScenario.map(({txs}) => txs),
          maxRows: fc.integer({min: 1, max: 60}),
        }),
        async ({txs, maxRows}) => {
          const rows = buildRows(txs);
          const through = rows.at(-1)?.[0] ?? '';
          const result = await digestCatchupRange(
            chunked(rows, [4]),
            through,
            maxRows,
            NO_BYTE_LIMIT,
          );

          const served = Math.min(maxRows, rows.length);
          expect(result.rows).toBe(served);
          // The limit applies only when it omits the closing commit.
          const closed = rows
            .slice(0, served)
            .some(([w, tag]) => tag === 'commit' && w === through);
          expect(result.limitReached).toBe(served === maxRows && !closed);
        },
      ),
      {numRuns: 100},
    );
  });

  test('the byte budget bounds the read', async () => {
    const rows = buildRows([{width: 5, kind: 'complete'}]);
    const through = rows.at(-1)?.[0] ?? '';
    const read = (maxBytes: number) =>
      digestCatchupRange(
        chunked(rows, [4]),
        through,
        rows.length + 1,
        maxBytes,
      );

    // A budget above the cost of the range serves all of it.
    const full = await read(NO_BYTE_LIMIT);
    expect(full.rows).toBe(rows.length);
    expect(full.bytes).toBeGreaterThan(0);
    expect(full.limitReached).toBe(false);

    // A budget below that cost stops before the closing commit.
    const cut = await read(Math.floor(full.bytes / 2));
    expect(cut.limitReached).toBe(true);
    expect(cut.rows).toBeLessThan(full.rows);
    expect(cut.bytes).toBeLessThan(full.bytes);

    // A row wider than the whole budget stops the read before it is parsed.
    const none = await read(1);
    expect(none.rows).toBe(0);
    expect(none.bytes).toBe(0);
    expect(none.limitReached).toBe(true);
  });

  test('the byte count measures normalized payloads, not stored text', async () => {
    // The same logical rows, one store padding its JSON with whitespace.
    const canonical = buildRows([{width: 3, kind: 'complete'}]);
    const padded = canonical.map(
      ([w, tag, json]) =>
        [w, tag, json.replaceAll(',', ' ,\n  ')] as WatermarkedChange,
    );
    const through = canonical.at(-1)?.[0] ?? '';

    const a = await digestCatchupRange(
      chunked(canonical, [4]),
      through,
      canonical.length + 1,
      NO_BYTE_LIMIT,
    );
    const b = await digestCatchupRange(
      chunked(padded, [4]),
      through,
      padded.length + 1,
      NO_BYTE_LIMIT,
    );

    // Both stores spend the same budget, so a limit falls in the same place
    // for both and neither side is sampled less than the other.
    expect(b.bytes).toBe(a.bytes);
    expect(b.digest).toBe(a.digest);
    expect(b.rows).toBe(a.rows);
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
          // A retry selects the same transactions.
          expect(isSampledForCompare(shard, watermark, hi)).toBe(
            isSampledForCompare(shard, watermark, hi),
          );
          // A larger percentage keeps the transactions in a smaller sample.
          if (isSampledForCompare(shard, watermark, lo)) {
            expect(isSampledForCompare(shard, watermark, hi)).toBe(true);
          }
          expect(isSampledForCompare(shard, watermark, 0)).toBe(false);
          expect(isSampledForCompare(shard, watermark, 100)).toBe(true);
        },
      ),
    );
  });

  test('normalization collapses formatting and preserves content', () => {
    expect(normalizeChangeJSON('{ "tag" : "insert",\n  "v": 1 }')).toBe(
      normalizeChangeJSON('{"tag":"insert","v":1}'),
    );
    // Values above Number.MAX_SAFE_INTEGER retain their precision.
    expect(normalizeChangeJSON('{"v":9007199254740993}')).toBe(
      '{"v":9007199254740993}',
    );
    // Invalid JSON remains unchanged.
    expect(normalizeChangeJSON('not-json')).toBe('not-json');

    fc.assert(
      fc.property(fc.oneof(fc.json(), fc.string()), value => {
        const once = normalizeChangeJSON(value);
        expect(normalizeChangeJSON(once)).toBe(once);
      }),
    );
  });
});
