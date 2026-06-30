import type postgres from 'postgres';
import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {
  buildCopyTasks,
  DEFAULT_MAX_CHUNKS_PER_TABLE,
  getInitialDownloadState,
  makeDownloadStatements,
  sampledHeapBlocks,
  sortCopyTasksForInitialCopy,
  sortDownloadsForInitialCopy,
  type DownloadState,
} from './initial-sync.ts';
import {createReplicationSlot} from './replication-slots.ts';

function spec(
  publications: Record<string, {rowFilter: string | null}> = {
    pub1: {rowFilter: null},
  },
): PublishedTableSpec {
  return {
    schema: 'public',
    name: 't',
    publications,
  } as unknown as PublishedTableSpec;
}

describe('makeDownloadStatements', () => {
  test('default path has no TABLESAMPLE or LIMIT', () => {
    const stmts = makeDownloadStatements(spec(), ['a', 'b']);
    expect(stmts.select).not.toMatch(/TABLESAMPLE/i);
    expect(stmts.select).not.toMatch(/\bLIMIT\b/i);
    expect(stmts.getTotalRows).not.toMatch(/TABLESAMPLE/i);
    expect(stmts.getTotalRows).not.toMatch(/FROM \(/i);
    expect(stmts.getTotalBytes).not.toMatch(/TABLESAMPLE/i);
    expect(stmts.getTotalBytes).not.toMatch(/FROM \(/i);
    expect(stmts.select).toBe(`SELECT "a","b" FROM "public"."t" `);
  });

  test('sampleRate === 1 does not inject TABLESAMPLE', () => {
    const stmts = makeDownloadStatements(spec(), ['a'], 1);
    expect(stmts.select).not.toMatch(/TABLESAMPLE/i);
    expect(stmts.select).not.toMatch(/\bLIMIT\b/i);
  });

  test('sampleRate undefined does not inject TABLESAMPLE', () => {
    const stmts = makeDownloadStatements(spec(), ['a'], undefined);
    expect(stmts.select).not.toMatch(/TABLESAMPLE/i);
  });

  test('sampleRate < 1 injects TABLESAMPLE BERNOULLI', () => {
    const stmts = makeDownloadStatements(spec(), ['a'], 0.25);
    expect(stmts.select).toMatch(/ TABLESAMPLE BERNOULLI\(25\) /);
    expect(stmts.getTotalRows).toMatch(/ TABLESAMPLE BERNOULLI\(25\) /);
    expect(stmts.getTotalBytes).toMatch(/ TABLESAMPLE BERNOULLI\(25\) /);
    // No LIMIT without maxRowsPerTable.
    expect(stmts.select).not.toMatch(/\bLIMIT\b/i);
  });

  test('maxRowsPerTable injects LIMIT and wraps counts in subquery', () => {
    const stmts = makeDownloadStatements(spec(), ['a', 'b'], undefined, 50);
    expect(stmts.select).toMatch(/ LIMIT 50$/);
    expect(stmts.getTotalRows).toMatch(
      /SELECT COUNT\(\*\)::bigint AS "totalRows" FROM \(SELECT 1 AS _ FROM .* LIMIT 50\) s/,
    );
    expect(stmts.getTotalBytes).toMatch(
      /SELECT COALESCE\(SUM\(b\), 0\)::bigint AS "totalBytes" FROM \(SELECT \(.+\) AS b FROM .* LIMIT 50\) s/,
    );
  });

  test('sample + limit compose', () => {
    const stmts = makeDownloadStatements(spec(), ['a'], 0.5, 10);
    expect(stmts.select).toMatch(
      /SELECT "a" FROM "public"\."t" TABLESAMPLE BERNOULLI\(50\) \s*LIMIT 10$/,
    );
    expect(stmts.getTotalRows).toMatch(/TABLESAMPLE BERNOULLI\(50\)/);
    expect(stmts.getTotalRows).toMatch(/LIMIT 10\) s$/);
  });

  test('row filters still appear in WHERE clause alongside sampling', () => {
    const stmts = makeDownloadStatements(
      spec({p: {rowFilter: 'a > 10'}}),
      ['a'],
      0.5,
    );
    expect(stmts.select).toMatch(
      /FROM "public"\."t" TABLESAMPLE BERNOULLI\(50\) WHERE a > 10/,
    );
  });

  test('extra predicate combines safely with row filters', () => {
    const stmts = makeDownloadStatements(
      spec({
        p1: {rowFilter: 'a > 10'},
        p2: {rowFilter: 'b < 20'},
      }),
      ['a'],
      undefined,
      undefined,
      undefined,
      `ctid >= '(0,0)'::tid AND ctid < '(10,0)'::tid`,
    );

    expect(stmts.select).toContain(
      `WHERE (a > 10 OR b < 20) AND (ctid >= '(0,0)'::tid AND ctid < '(10,0)'::tid)`,
    );
  });
});

function download(
  table: string,
  totalBytes: number | undefined,
  heapPages: number,
  copyBytesEstimate?: number | undefined,
): DownloadState {
  return {
    spec: {schema: 'public', name: table, publications: {p: {rowFilter: null}}},
    status: {table, columns: [], totalRows: 0, totalBytes, rows: 0},
    copyBytesEstimate,
    heapPages,
  } as unknown as DownloadState;
}

describe('buildCopyTasks', () => {
  test('chunking disabled creates one task per table', () => {
    const tasks = buildCopyTasks(
      createSilentLogContext(),
      [download('a', 100, 10), download('b', 200, 20)],
      0,
      DEFAULT_MAX_CHUNKS_PER_TABLE,
      false,
    );

    expect(tasks.map(t => t.chunk)).toEqual([undefined, undefined]);
    expect(tasks.map(t => t.estimatedBytes)).toEqual([100, 200]);
  });

  test('table below target creates one task', () => {
    const tasks = buildCopyTasks(
      createSilentLogContext(),
      [download('a', 99, 10)],
      100,
      DEFAULT_MAX_CHUNKS_PER_TABLE,
      false,
    );

    expect(tasks).toHaveLength(1);
    expect(tasks[0].chunk).toBeUndefined();
  });

  test('table above target creates CTID ranges without gaps or overlaps', () => {
    const tasks = buildCopyTasks(
      createSilentLogContext(),
      [download('a', 10_000, 10)],
      2_500,
      DEFAULT_MAX_CHUNKS_PER_TABLE,
      false,
    );

    expect(tasks.map(t => t.chunk)).toEqual([
      {index: 1, total: 4, startBlock: 0, endBlock: 2},
      {index: 2, total: 4, startBlock: 2, endBlock: 5},
      {index: 3, total: 4, startBlock: 5, endBlock: 7},
      {index: 4, total: 4, startBlock: 7, endBlock: 10},
    ]);
  });

  test('copy byte estimate drives chunking when physical size is below target', () => {
    const tasks = buildCopyTasks(
      createSilentLogContext(),
      [download('toast_heavy', 100, 10, 10_000)],
      2_500,
      DEFAULT_MAX_CHUNKS_PER_TABLE,
      false,
    );

    expect(tasks).toHaveLength(4);
    expect(tasks.map(t => t.estimatedBytes)).toEqual([
      2_000, 3_000, 2_000, 3_000,
    ]);
    expect(tasks.map(t => t.chunk)).toEqual([
      {index: 1, total: 4, startBlock: 0, endBlock: 2},
      {index: 2, total: 4, startBlock: 2, endBlock: 5},
      {index: 3, total: 4, startBlock: 5, endBlock: 7},
      {index: 4, total: 4, startBlock: 7, endBlock: 10},
    ]);
  });

  test('missing copy byte estimate falls back to total bytes', () => {
    const tasks = buildCopyTasks(
      createSilentLogContext(),
      [download('a', 10_000, 10)],
      2_500,
      DEFAULT_MAX_CHUNKS_PER_TABLE,
      false,
    );

    expect(tasks).toHaveLength(4);
    expect(tasks.map(t => t.estimatedBytes)).toEqual([
      2_000, 3_000, 2_000, 3_000,
    ]);
  });

  test('chunk count caps at max chunks and heap pages', () => {
    const byMax = buildCopyTasks(
      createSilentLogContext(),
      [download('a', 1_000, 1_000)],
      1,
      DEFAULT_MAX_CHUNKS_PER_TABLE,
      false,
    );
    expect(byMax).toHaveLength(DEFAULT_MAX_CHUNKS_PER_TABLE);

    const byHeapPages = buildCopyTasks(
      createSilentLogContext(),
      [download('a', 1_000, 3)],
      1,
      DEFAULT_MAX_CHUNKS_PER_TABLE,
      false,
    );
    expect(byHeapPages).toHaveLength(3);
  });

  test('custom max chunks per table is respected', () => {
    const tasks = buildCopyTasks(
      createSilentLogContext(),
      [download('a', 1_000, 1_000)],
      1,
      7,
      false,
    );

    expect(tasks).toHaveLength(7);
  });

  test('missing estimates fall back to one task', () => {
    const missingBytes = buildCopyTasks(
      createSilentLogContext(),
      [download('a', undefined, 10)],
      1,
      DEFAULT_MAX_CHUNKS_PER_TABLE,
      false,
    );
    expect(missingBytes).toHaveLength(1);
    expect(missingBytes[0].chunk).toBeUndefined();

    const missingHeapPages = buildCopyTasks(
      createSilentLogContext(),
      [download('a', 1_000, 0)],
      1,
      DEFAULT_MAX_CHUNKS_PER_TABLE,
      false,
    );
    expect(missingHeapPages).toHaveLength(1);
    expect(missingHeapPages[0].chunk).toBeUndefined();
  });

  test('copy tasks sort by estimated bytes descending', () => {
    const small = {estimatedBytes: 10};
    const large = {estimatedBytes: 1000};
    const medium = {estimatedBytes: 100};
    const tasks = [small, large, medium];

    expect(sortCopyTasksForInitialCopy(tasks)).toEqual([large, medium, small]);
    expect(tasks).toEqual([small, large, medium]);
  });
});

describe('sortDownloadsForInitialCopy', () => {
  test('orders table copies by estimated bytes descending', () => {
    const small = {status: {table: 'small', totalBytes: 10}};
    const large = {status: {table: 'large', totalBytes: 1000}};
    const medium = {
      copyBytesEstimate: 500,
      status: {table: 'medium', totalBytes: 100},
    };
    const unknown = {status: {table: 'unknown', totalBytes: undefined}};
    const downloads = [small, unknown, large, medium];

    expect(sortDownloadsForInitialCopy(downloads)).toEqual([
      large,
      medium,
      small,
      unknown,
    ]);
    expect(downloads).toEqual([small, unknown, large, medium]);
  });
});

describe('sampledHeapBlocks', () => {
  test('samples every page for small heaps', () => {
    expect(sampledHeapBlocks(4)).toEqual([0, 1, 2, 3]);
  });

  test('samples 16 pages spread across large heaps', () => {
    const blocks = sampledHeapBlocks(1024);

    expect(blocks).toHaveLength(16);
    expect(blocks[0]).toBe(32);
    expect(blocks.at(-1)).toBe(992);
    expect(new Set(blocks).size).toBe(blocks.length);
  });
});

describe('getInitialDownloadState', () => {
  function tableSpec(): PublishedTableSpec {
    return {
      schema: 'public',
      name: 't',
      columns: {a: {dataType: 'int4'}, b: {dataType: 'text'}},
      publications: {pub1: {rowFilter: null}},
    } as unknown as PublishedTableSpec;
  }

  test('skipTotals=true returns zeros without touching the DB', async () => {
    let called = false;
    const sql = Object.assign(
      () => {
        called = true;
        throw new Error('sql should not be called when skipTotals=true');
      },
      {
        unsafe() {
          called = true;
          throw new Error('sql should not be called when skipTotals=true');
        },
      },
    ) as unknown as PostgresDB;

    const state = await getInitialDownloadState(
      createSilentLogContext(),
      sql,
      tableSpec(),
      true,
    );
    expect(called).toBe(false);
    expect(state.status).toEqual({
      table: 't',
      columns: ['a', 'b'],
      rows: 0,
      totalRows: 0,
      totalBytes: 0,
    });
    expect(state.copyBytesEstimate).toBe(0);
    expect(state.heapPages).toBe(0);
  });

  test('skipTotals=false uses pg_class and sampled logical copy estimate', async () => {
    // The tagged template sql`...` is called as a function with
    // (strings, ...values) when used as a template tag.
    const sql = Object.assign(
      (strings: TemplateStringsArray, ..._values: unknown[]) => {
        const query = strings.join('$1');
        if (query.includes('pg_class')) {
          return Promise.resolve([
            {totalRows: 42, totalBytes: 8192, heapPages: 1},
          ]);
        }
        return Promise.resolve([]);
      },
      {
        unsafe(query: string) {
          expect(query).toContain(`ctid >= '(0,0)'::tid`);
          expect(query).toContain(`octet_length("a"::text)`);
          expect(query).toContain(`octet_length("b"::text)`);
          return Promise.resolve([{sampleBytes: 1_000}]);
        },
      },
    ) as unknown as PostgresDB;

    const state = await getInitialDownloadState(
      createSilentLogContext(),
      sql,
      tableSpec(),
      false,
    );
    expect(state.status.totalRows).toBe(42);
    expect(state.status.totalBytes).toBe(8192);
    expect(state.copyBytesEstimate).toBe(8192);
    expect(state.heapPages).toBe(1);
  });
});

describe('createReplicationSlot', () => {
  /** Builds a mock session whose `unsafe` calls are handled by `handler`. */
  function mockSession(handler: (stmt: string) => Promise<unknown>) {
    return {
      unsafe: vi.fn(handler),
      end: vi.fn(() => Promise.resolve()),
    } as unknown as postgres.Sql;
  }

  test('returns the slot on success', async () => {
    const slot = {
      slot_name: 'test_slot',
      consistent_point: '0/1',
      snapshot_name: 'snap',
      output_plugin: 'pgoutput',
    };
    const session = mockSession(stmt => {
      if (stmt.startsWith('SET lock_timeout')) {
        return Promise.resolve([]);
      }
      // CREATE_REPLICATION_SLOT
      return Promise.resolve([slot]);
    });

    const result = await createReplicationSlot(
      createSilentLogContext(),
      session,
      {slotName: 'test_slot'},
    );
    expect(result).toEqual(slot);
  });

  test('sets lock_timeout before creating the slot', async () => {
    const calls: string[] = [];
    const session = mockSession(stmt => {
      calls.push(stmt);
      if (stmt.startsWith('SET lock_timeout')) {
        return Promise.resolve([]);
      }
      return Promise.resolve([
        {
          slot_name: 's',
          consistent_point: '0/1',
          snapshot_name: 'snap',
          output_plugin: 'pgoutput',
        },
      ]);
    });

    await createReplicationSlot(createSilentLogContext(), session, {
      slotName: 's',
    });
    expect(calls[0]).toMatch(/^SET lock_timeout = \d+$/);
    expect(calls[1]).toMatch(/CREATE_REPLICATION_SLOT/);
  });

  test('propagates server-side errors (e.g. lock_not_available)', async () => {
    const pgError = new Error('canceling statement due to lock timeout');
    (pgError as unknown as {code: string}).code = '55P03';

    const session = mockSession(stmt => {
      if (stmt.startsWith('SET lock_timeout')) {
        return Promise.resolve([]);
      }
      return Promise.reject(pgError);
    });

    await expect(
      createReplicationSlot(createSilentLogContext(), session, {
        slotName: 'test_slot',
      }),
    ).rejects.toBe(pgError);
  });

  test('falls back to client-side timeout when session hangs', async () => {
    vi.useFakeTimers();
    try {
      const session = mockSession(stmt => {
        if (stmt.startsWith('SET lock_timeout')) {
          return Promise.resolve([]);
        }
        // Simulate a hang: never resolve (e.g. network partition where
        // the server aborted but the client never receives the error).
        return new Promise(() => {});
      });

      // Capture the rejection eagerly to avoid an unhandled rejection
      // between the time advanceTimersByTimeAsync triggers it and the
      // time we assert on it.
      let caught: unknown;
      const result = createReplicationSlot(createSilentLogContext(), session, {
        slotName: 'hang_slot',
      }).catch(e => {
        caught = e;
      });

      // Advance past the 30s client-side timeout.
      await vi.advanceTimersByTimeAsync(31_000);
      await result;

      expect(caught).toBeInstanceOf(Error);
      expect(String(caught)).toMatch(
        /Timed out after \d+ ms creating replication slot hang_slot/,
      );

      // session.end() is called in the background to tear down the
      // orphaned connection.
      expect(
        (session.end as ReturnType<typeof vi.fn>).mock.calls.length,
      ).toBeGreaterThanOrEqual(1);

      // Drain any remaining timers/microtasks before restoring real timers.
      await vi.runAllTimersAsync();
    } finally {
      vi.useRealTimers();
    }
  });
});
