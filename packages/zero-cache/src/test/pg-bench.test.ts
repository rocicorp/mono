import {describe, expect, test} from 'vitest';
import {
  benchmarkProductionPayload,
  initialSyncBenchmarkPayloadBytes,
  makeBenchmarkFixtureRows,
} from './pg-bench.ts';

describe('initial-sync benchmark fixtures', () => {
  test('generates deterministic varied mixed rows', () => {
    const first = makeBenchmarkFixtureRows(1, 20);
    const second = makeBenchmarkFixtureRows(1, 20);
    expect(first).toEqual(second);
    expect(new Set(first.map(({table}) => table))).toEqual(
      new Set(['bench_rows', 'bench_lookup', 'bench_wide', 'bench_composite']),
    );
  });

  test('generates deterministic row-specific large payloads', () => {
    const payload1 = benchmarkProductionPayload('wide-text', 1, 683_000);
    const payload2 = benchmarkProductionPayload('wide-text', 2, 683_000);
    expect(Buffer.byteLength(payload1)).toBe(683_000);
    expect(Buffer.byteLength(payload2)).toBe(683_000);
    expect(payload1).not.toBe(payload2);
    expect(payload1).toMatch(/^wide-text:1:/);
    expect(payload2).toMatch(/^wide-text:2:/);
  });

  test('reports exact generated payload bytes', () => {
    expect(
      initialSyncBenchmarkPayloadBytes({
        fixture: 'large-payload',
        rows: 2_000,
        payloadBytes: 275_000,
      }),
    ).toBe(550_000_000);
  });
});
