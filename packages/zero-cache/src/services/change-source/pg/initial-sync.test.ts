import type postgres from 'postgres';
import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import type {DownloadStatus} from '../../../../../zero-events/src/status.ts';
import type {
  PublishedIndexSpec,
  PublishedTableSpec,
} from '../../../db/specs.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {
  getInitialDownloadState,
  makeDownloadStatements,
  planInitialSyncIndexes,
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
  });

  test('skipTotals=false uses pg_class estimates', async () => {
    // The tagged template sql`...` is called as a function with
    // (strings, ...values) when used as a template tag.
    const sql = Object.assign(
      (strings: TemplateStringsArray, ..._values: unknown[]) => {
        const query = strings.join('$1');
        if (query.includes('pg_class')) {
          return Promise.resolve([{totalRows: 42, totalBytes: 8192}]);
        }
        return Promise.resolve([]);
      },
      {
        unsafe() {
          throw new Error('unsafe should not be called');
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
  });
});

describe('planInitialSyncIndexes', () => {
  function indexSpec(
    schema: string,
    tableName: string,
    name: string,
    isPrimaryKey = false,
  ): PublishedIndexSpec {
    return {
      schema,
      tableName,
      name,
      unique: isPrimaryKey,
      columns: {id: 'ASC'},
      isPrimaryKey,
    };
  }

  function downloadStatus(
    table: string,
    totalRows: number,
    totalBytes: number | undefined,
  ): DownloadStatus {
    return {table, columns: ['id'], rows: 0, totalRows, totalBytes};
  }

  test('leaves all indexes after the copy when disabled', () => {
    const secondary = indexSpec('public', 'items', 'items_key_idx');
    const primary = indexSpec('public', 'items', 'items_pkey', true);
    const indexes = [secondary, primary];

    const plan = planInitialSyncIndexes(
      indexes,
      [downloadStatus('items', 1, 4096)],
      0,
      true,
    );

    expect(plan.enabled).toBe(false);
    expect(plan.preCopyIndexes).toEqual([]);
    expect(plan.postCopyIndexes).toEqual(indexes);
    expect(plan.tableStrategies).toEqual([]);
    expect(plan.postCopySecondaryIndexes).toBe(1);
    expect(plan.postCopyPrimaryIndexes).toBe(1);
  });

  test.each([
    {name: 'empty table', rows: 0, bytes: 8192, expected: false},
    {name: 'missing bytes', rows: 1, bytes: undefined, expected: false},
    {name: 'zero bytes', rows: 1, bytes: 0, expected: false},
    {name: 'below threshold', rows: 1, bytes: 2047, expected: false},
    {name: 'at threshold', rows: 1, bytes: 2048, expected: true},
    {name: 'above threshold', rows: 1, bytes: 2049, expected: true},
    {
      name: 'large finite estimate',
      rows: 1_000_000,
      bytes: 2_048_000_000,
      expected: true,
    },
    {name: 'stale high row estimate', rows: 10, bytes: 2049, expected: false},
    {name: 'stale low row estimate', rows: 1, bytes: 20_480, expected: true},
  ])('$name', ({rows, bytes, expected}) => {
    const secondary = indexSpec('public', 'items', 'items_key_idx');
    const plan = planInitialSyncIndexes(
      [secondary],
      [downloadStatus('items', rows, bytes)],
      2048,
      true,
    );

    expect(plan.preCopyIndexes).toEqual(expected ? [secondary] : []);
    expect(plan.postCopyIndexes).toEqual(expected ? [] : [secondary]);
    expect(plan.tableStrategies[0]?.strategy).toBe(
      expected ? 'eager' : 'deferred',
    );
  });

  test('partitions indexes by table while preserving order', () => {
    const wideSecondaryA = indexSpec('wide', 'items', 'items_key_idx');
    const narrowSecondary = indexSpec('narrow', 'items', 'items_key_idx');
    const widePrimary = indexSpec('wide', 'items', 'items_pkey', true);
    const emptySecondary = indexSpec('empty', 'items', 'items_key_idx');
    const wideSecondaryB = indexSpec('wide', 'items', 'items_replica_idx');
    const plan = planInitialSyncIndexes(
      [
        wideSecondaryA,
        narrowSecondary,
        widePrimary,
        emptySecondary,
        wideSecondaryB,
      ],
      [
        downloadStatus('wide.items', 2, 4096),
        downloadStatus('narrow.items', 2, 4095),
        downloadStatus('empty.items', 0, 8192),
      ],
      2048,
      true,
    );

    expect(plan.preCopyIndexes).toEqual([wideSecondaryA, wideSecondaryB]);
    expect(plan.postCopyIndexes).toEqual([
      narrowSecondary,
      widePrimary,
      emptySecondary,
    ]);
    expect(plan.tableStrategies).toEqual([
      {
        table: 'wide.items',
        estimateSource: 'pg_class',
        totalRows: 2,
        totalBytes: 4096,
        estimatedAverageRowBytes: 2048,
        strategy: 'eager',
      },
      {
        table: 'narrow.items',
        estimateSource: 'pg_class',
        totalRows: 2,
        totalBytes: 4095,
        estimatedAverageRowBytes: 2047.5,
        strategy: 'deferred',
      },
      {
        table: 'empty.items',
        estimateSource: 'pg_class',
        totalRows: 0,
        totalBytes: 8192,
        estimatedAverageRowBytes: null,
        strategy: 'deferred',
      },
    ]);
    expect(plan.postCopySecondaryIndexes).toBe(2);
    expect(plan.postCopyPrimaryIndexes).toBe(1);
  });

  test('defers indexes when table estimates are unavailable', () => {
    const secondary = indexSpec('public', 'items', 'items_key_idx');
    const plan = planInitialSyncIndexes(
      [secondary],
      [downloadStatus('items', 1, 4096)],
      2048,
      false,
    );

    expect(plan.preCopyIndexes).toEqual([]);
    expect(plan.postCopyIndexes).toEqual([secondary]);
    expect(plan.tableStrategies).toEqual([
      {
        table: 'items',
        estimateSource: 'unavailable',
        totalRows: null,
        totalBytes: null,
        estimatedAverageRowBytes: null,
        strategy: 'deferred',
      },
    ]);
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
