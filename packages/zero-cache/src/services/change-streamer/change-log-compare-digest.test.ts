/** Tests the pure compare primitives. Integration tests cover both stores. */

import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import {digestCatchupRange} from './change-log-compare-digest.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer.ts';

describe('change-streamer/change-log-compare-digest', () => {
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

  test('the digest tracks the stored text, so an encoding difference shows', async () => {
    // Both logs store the same `extractChangeSubstring` output, and the
    // Postgres column is `json`, which keeps the input text exactly. A
    // formatting difference is therefore a real difference, not a rendering
    // artifact, and hashing the stored text keeps it visible.
    const rows = buildRows([{width: 3, kind: 'complete'}]);
    const reformatted = rows.map(
      ([w, tag, json]): WatermarkedChange => [
        w,
        tag,
        json.replaceAll(':', ': '),
      ],
    );
    expect(reformatted).not.toEqual(rows);
    expect((await digest(reformatted)).digest).not.toBe(
      (await digest(rows)).digest,
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

    // A row wider than the whole budget stops the read before it is hashed.
    const none = await read(1);
    expect(none.rows).toBe(0);
    expect(none.bytes).toBe(0);
    expect(none.limitReached).toBe(true);
  });

  test('the byte count measures the stored change text', async () => {
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

    // The budget is spent on what the store actually returned, so a wider
    // encoding costs more of it.
    expect(b.rows).toBe(a.rows);
    expect(b.bytes).toBeGreaterThan(a.bytes);
  });

  test('a change is hashed exactly as stored, including large integers', async () => {
    // Hashing the stored text cannot lose precision the way a JSON round trip
    // through `number` would.
    const big = '9007199254740993';
    const rows: WatermarkedChange[] = [
      ['w01', 'begin', `["begin",{"tag":"begin"},{"commitWatermark":"w01"}]`],
      ['w01', 'insert' as ChangeTag, `["data",{"tag":"insert","v":${big}}]`],
      ['w01', 'commit', `["commit",{"tag":"commit"},{"watermark":"w01"}]`],
    ];
    const nudged = rows.map(
      ([w, tag, json]): WatermarkedChange => [
        w,
        tag,
        json.replace(big, '9007199254740992'),
      ],
    );
    expect((await digest(nudged)).digest).not.toBe((await digest(rows)).digest);
  });
});
