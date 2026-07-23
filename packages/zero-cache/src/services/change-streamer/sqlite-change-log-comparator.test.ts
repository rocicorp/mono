import {LogContext} from '@rocicorp/logger';
import {describe, expect, test, vi} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {reconstructWatermarkedChange} from './change-log-codec.ts';
import {
  type ChangeLogComparisonBounds,
  type ChangeLogComparisonReader,
  type ChangeLogComparisonRow,
  type ChangeLogComparisonSource,
  type ChangeLogRangeInspection,
  isSQLiteChangeLogSampled,
  SQLiteChangeLogComparator,
  type SQLiteChangeLogMismatchReason,
} from './sqlite-change-log-comparator.ts';

const SHARD = {appID: 'zero', shardNum: 1};
const REPLICA_VERSION = '02';
const WATERMARK = '06';

describe('change-streamer/sqlite-change-log-comparator', () => {
  test('matches every canonical message representation across many batches', async () => {
    const rows = transactionRows([
      ['begin', {tag: 'begin', json: 'b'}],
      ['insert', {tag: 'insert', value: 'nul:\u0000'}],
      ['update', {tag: 'update', value: 9_007_199_254_740_993n}],
      ['delete', {tag: 'delete'}],
      ['truncate', {tag: 'truncate', relations: []}],
      ['create-table', {tag: 'create-table', spec: {name: 'foo'}}],
      ['backfill-completed', {tag: 'backfill-completed', columns: ['a']}],
      ['commit', {tag: 'commit', json: 'c'}],
    ]);
    using fixture = comparatorFixture(rows, rows, {batchSize: 2});

    await expect(
      fixture.comparator.compareWatermark(WATERMARK),
    ).resolves.toEqual(
      expect.objectContaining({
        outcome: 'match',
        reason: 'match',
        targetWatermark: WATERMARK,
        comparedRows: rows.length,
        retry: false,
      }),
    );
    expect(fixture.pg.readBatches).toBeGreaterThan(1);
    expect(fixture.sqlite.readBatches).toBeGreaterThan(1);
  });

  test('treats temporary head lag as inconclusive and compares only a common committed range', async () => {
    const rows = transactionRows([
      ['begin', {tag: 'begin'}],
      ['commit', {tag: 'commit'}],
    ]);
    using fixture = comparatorFixture(rows, rows);
    fixture.pg.boundsValue = bounds({headWatermark: '08'});
    fixture.sqlite.boundsValue = bounds({
      headWatermark: WATERMARK,
      schemaVersion: 14,
    });

    await expect(fixture.comparator.compareWatermark('08')).resolves.toEqual(
      expect.objectContaining({
        outcome: 'inconclusive',
        reason: 'head-skew',
        pgHead: '08',
        sqliteHead: WATERMARK,
        retry: true,
      }),
    );
    await expect(
      fixture.comparator.compareWatermark(WATERMARK),
    ).resolves.toEqual(expect.objectContaining({outcome: 'match'}));
  });

  test.each<{
    reason: SQLiteChangeLogMismatchReason;
    mutate: (
      pg: ChangeLogComparisonRow[],
      sqlite: ChangeLogComparisonRow[],
    ) => void;
  }>([
    {
      reason: 'missing-pg-row',
      mutate: pg => pg.splice(1, 1),
    },
    {
      reason: 'missing-sqlite-row',
      mutate: (_pg, sqlite) => sqlite.splice(1, 1),
    },
    {
      reason: 'tag-mismatch',
      mutate: (_pg, sqlite) => {
        sqlite[1] = {...sqlite[1], tag: 'delete'};
      },
    },
    {
      reason: 'byte-mismatch',
      mutate: (_pg, sqlite) => {
        sqlite[1] = {...sqlite[1], json: sqlite[1].json + ' '};
      },
    },
    {
      reason: 'bound-mismatch',
      mutate: (_pg, sqlite) => sqlite.splice(0, 1),
    },
  ])('classifies $reason', async ({reason, mutate}) => {
    const pg = transactionRows([
      ['begin', {tag: 'begin'}],
      ['insert', {tag: 'insert', value: 'secret'}],
      ['commit', {tag: 'commit'}],
    ]);
    const sqlite = structuredClone(pg);
    mutate(pg, sqlite);
    using fixture = comparatorFixture(pg, sqlite);

    await expect(
      fixture.comparator.compareWatermark(WATERMARK),
    ).resolves.toEqual(
      expect.objectContaining({
        outcome: 'divergence',
        reason,
        retry: false,
      }),
    );
  });

  test('turns a purge race into an inconclusive result', async () => {
    const rows = transactionRows([
      ['begin', {tag: 'begin'}],
      ['insert', {tag: 'insert'}],
      ['update', {tag: 'update'}],
      ['commit', {tag: 'commit'}],
    ]);
    using fixture = comparatorFixture(rows, structuredClone(rows), {
      batchSize: 1,
    });
    fixture.sqlite.afterBatch = batch => {
      if (batch === 1) {
        fixture.sqlite.rows = [];
        fixture.sqlite.boundsValue = bounds({
          minWatermark: '08',
          maxWatermark: '08',
          headWatermark: '08',
          schemaVersion: 14,
        });
      }
    };

    await expect(
      fixture.comparator.compareWatermark(WATERMARK),
    ).resolves.toEqual(
      expect.objectContaining({
        outcome: 'inconclusive',
        reason: 'bounds-changed',
        retry: false,
      }),
    );
  });

  test('checks schema, replica version, warm-up, and reader eligibility', async () => {
    const rows = transactionRows([
      ['begin', {tag: 'begin'}],
      ['commit', {tag: 'commit'}],
    ]);

    using oldSchema = comparatorFixture(rows, rows);
    oldSchema.sqlite.boundsValue = bounds({schemaVersion: 13});
    await expect(
      oldSchema.comparator.compareWatermark(WATERMARK),
    ).resolves.toEqual(
      expect.objectContaining({
        outcome: 'ineligible',
        reason: 'schema-version',
      }),
    );

    using wrongReplica = comparatorFixture(rows, rows);
    wrongReplica.sqlite.boundsValue = bounds({
      replicaVersion: 'different',
      schemaVersion: 14,
    });
    await expect(
      wrongReplica.comparator.compareWatermark(WATERMARK),
    ).resolves.toEqual(
      expect.objectContaining({
        outcome: 'ineligible',
        reason: 'replica-version',
      }),
    );

    using warming = comparatorFixture(rows, rows, {
      now: () => 100,
      warmupStartedAtMs: 50,
      retentionMs: 100,
    });
    await expect(
      warming.comparator.compareWatermark(WATERMARK),
    ).resolves.toEqual(
      expect.objectContaining({
        outcome: 'ineligible',
        reason: 'warming-up',
        retry: true,
      }),
    );

    using readerError = comparatorFixture(rows, rows);
    readerError.sqlite.readError = new Error('no payload here');
    await expect(
      readerError.comparator.compareWatermark(WATERMARK),
    ).resolves.toEqual(
      expect.objectContaining({
        outcome: 'inconclusive',
        reason: 'reader-error',
        errorName: 'Error',
        retry: true,
      }),
    );
  });

  test('sampling is stable by replica, shard, and watermark', () => {
    expect(isSQLiteChangeLogSampled(SHARD, REPLICA_VERSION, WATERMARK, 0)).toBe(
      false,
    );
    expect(
      isSQLiteChangeLogSampled(SHARD, REPLICA_VERSION, WATERMARK, 100),
    ).toBe(true);

    const decisions = Array.from({length: 1000}, (_, i) =>
      isSQLiteChangeLogSampled(SHARD, REPLICA_VERSION, String(i), 50),
    );
    expect(decisions.filter(Boolean).length).toBeGreaterThan(400);
    expect(decisions.filter(Boolean).length).toBeLessThan(600);
    for (let i = 0; i < decisions.length; i++) {
      expect(
        isSQLiteChangeLogSampled(SHARD, REPLICA_VERSION, String(i), 50),
      ).toBe(decisions[i]);
    }
  });

  test('scheduler retries the same range and coalesces newer sampled ranges', async () => {
    vi.useFakeTimers();
    const results: {targetWatermark: string; reason: string}[] = [];
    const rows06 = transactionRows([
      ['begin', {tag: 'begin'}],
      ['commit', {tag: 'commit'}],
    ]);
    const rows0a = rows06.map(row => ({...row, watermark: '0a'}));
    const pg = new MemoryComparisonSource(
      [...rows06, ...rows0a],
      bounds({headWatermark: '0a', maxWatermark: '0a'}),
    );
    const sqlite = new MemoryComparisonSource(
      [...rows06, ...rows0a],
      bounds({
        headWatermark: '04',
        maxWatermark: '0a',
        schemaVersion: 14,
      }),
    );
    const comparator = new SQLiteChangeLogComparator(
      new LogContext('error'),
      pg,
      sqlite,
      {
        replicaVersion: REPLICA_VERSION,
        shard: SHARD,
        retentionMs: 1,
        batchSize: 10,
        samplePercent: 100,
        warmupStartedAtMs: 0,
        retryDelayMs: 10,
        now: () => 1000,
        onResult: ({targetWatermark, reason}) =>
          results.push({targetWatermark, reason}),
      },
    );
    try {
      comparator.schedule(WATERMARK);
      await vi.advanceTimersByTimeAsync(0);
      expect(results).toEqual([
        {targetWatermark: WATERMARK, reason: 'head-skew'},
      ]);

      comparator.schedule('08');
      comparator.schedule('0a');
      sqlite.boundsValue = bounds({
        headWatermark: '0a',
        maxWatermark: '0a',
        schemaVersion: 14,
      });
      await vi.advanceTimersByTimeAsync(10);
      await vi.advanceTimersByTimeAsync(1);

      expect(results).toEqual([
        {targetWatermark: WATERMARK, reason: 'head-skew'},
        {targetWatermark: WATERMARK, reason: 'match'},
        {targetWatermark: '0a', reason: 'match'},
      ]);
    } finally {
      await comparator.close();
      vi.useRealTimers();
    }
  });

  test('bounded diagnostics do not log row payloads', async () => {
    const secret = 'customer-secret-that-must-not-be-logged';
    const pg = transactionRows([
      ['begin', {tag: 'begin'}],
      ['insert', {tag: 'insert', value: secret}],
      ['commit', {tag: 'commit'}],
    ]);
    const sqlite = structuredClone(pg);
    sqlite[1] = {...sqlite[1], json: sqlite[1].json + ' '};
    const sink = new TestLogSink();
    using fixture = comparatorFixture(pg, sqlite, {
      lc: new LogContext('debug', undefined, sink),
    });

    await fixture.comparator.compareWatermark(WATERMARK);

    const logs = JSON.stringify(sink.messages);
    expect(logs).not.toContain(secret);
    expect(logs).not.toContain(pg[1].json);
    expect(logs).toContain('jsonBytes');
    expect(logs).toContain('byte-mismatch');
  });
});

type TaggedChange = readonly [tag: string, change: Record<string, unknown>];

function transactionRows(changes: readonly TaggedChange[]) {
  return changes.map(([tag, change], pos): ChangeLogComparisonRow => {
    const changeJSON = stringify(change);
    return {
      watermark: WATERMARK,
      pos,
      tag,
      json: reconstructWatermarkedChange({
        watermark: WATERMARK,
        tag,
        change: changeJSON,
      })[2],
    };
  });
}

function stringify(value: unknown): string {
  return JSON.stringify(value, (_key, nested) =>
    typeof nested === 'bigint' ? `${nested}n` : nested,
  );
}

function bounds(
  overrides: Partial<ChangeLogComparisonBounds> = {},
): ChangeLogComparisonBounds {
  return {
    replicaVersion: REPLICA_VERSION,
    headWatermark: '08',
    minWatermark: '04',
    maxWatermark: '08',
    ...overrides,
  };
}

type FixtureOptions = {
  readonly batchSize?: number | undefined;
  readonly retentionMs?: number | undefined;
  readonly warmupStartedAtMs?: number | undefined;
  readonly now?: (() => number) | undefined;
  readonly lc?: LogContext | undefined;
};

function comparatorFixture(
  pgRows: readonly ChangeLogComparisonRow[],
  sqliteRows: readonly ChangeLogComparisonRow[],
  opts: FixtureOptions = {},
) {
  const pg = new MemoryComparisonSource([...structuredClone(pgRows)], bounds());
  const sqlite = new MemoryComparisonSource(
    [...structuredClone(sqliteRows)],
    bounds({schemaVersion: 14}),
  );
  const comparator = new SQLiteChangeLogComparator(
    opts.lc ?? new LogContext('error'),
    pg,
    sqlite,
    {
      replicaVersion: REPLICA_VERSION,
      shard: SHARD,
      retentionMs: opts.retentionMs ?? 100,
      batchSize: opts.batchSize ?? 100,
      samplePercent: 100,
      warmupStartedAtMs: opts.warmupStartedAtMs ?? 0,
      now: opts.now ?? (() => 1000),
    },
  );
  return {
    comparator,
    pg,
    sqlite,
    [Symbol.dispose]() {
      void comparator.close();
    },
  };
}

class MemoryComparisonSource
  implements ChangeLogComparisonSource, ChangeLogComparisonReader
{
  rows: ChangeLogComparisonRow[];
  boundsValue: ChangeLogComparisonBounds;
  afterBatch: ((batch: number) => void) | undefined;
  readError: Error | undefined;
  readBatches = 0;

  constructor(
    rows: ChangeLogComparisonRow[],
    boundsValue: ChangeLogComparisonBounds,
  ) {
    this.rows = rows;
    this.boundsValue = boundsValue;
  }

  withRead<T>(
    read: (reader: ChangeLogComparisonReader) => Promise<T>,
  ): Promise<T> {
    return read(this);
  }

  bounds(): Promise<ChangeLogComparisonBounds> {
    return Promise.resolve({...this.boundsValue});
  }

  inspect(watermark: string): Promise<ChangeLogRangeInspection> {
    const bounds = {...this.boundsValue};
    if (
      bounds.minWatermark === null ||
      watermark < bounds.minWatermark ||
      watermark > bounds.headWatermark
    ) {
      return Promise.resolve({bounds, status: 'outside-retention'});
    }
    const rows = this.rows.filter(row => row.watermark === watermark);
    const first = rows.at(0);
    const last = rows.at(-1);
    return Promise.resolve({
      bounds,
      status:
        first?.pos === 0 && first.tag === 'begin' && last?.tag === 'commit'
          ? 'complete'
          : 'incomplete',
    });
  }

  inspectCurrent(watermark: string): Promise<ChangeLogRangeInspection> {
    return this.inspect(watermark);
  }

  async *read(
    watermark: string,
    batchSize: number,
  ): AsyncIterable<readonly ChangeLogComparisonRow[]> {
    if (this.readError) {
      throw this.readError;
    }
    let lastPos = -1;
    while (true) {
      const batch = this.rows
        .filter(row => row.watermark === watermark && row.pos > lastPos)
        .sort((a, b) => a.pos - b.pos)
        .slice(0, batchSize);
      if (batch.length === 0) {
        return;
      }
      this.readBatches++;
      lastPos = batch.at(-1)?.pos ?? lastPos;
      yield batch;
      this.afterBatch?.(this.readBatches);
    }
  }

  close(): void {}
}
