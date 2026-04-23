import type postgres from 'postgres';
import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {
  createReplicationSlot,
  getInitialDownloadState,
  makeDownloadStatements,
} from './initial-sync.ts';

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
    const sql = {
      unsafe() {
        called = true;
        throw new Error('sql should not be called when skipTotals=true');
      },
    } as unknown as PostgresDB;

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

  test('skipTotals=false runs the expensive queries', async () => {
    const received: string[] = [];
    const sql = {
      unsafe(stmt: string) {
        received.push(stmt);
        const row = stmt.includes('totalRows')
          ? [{totalRows: 42n}]
          : [{totalBytes: 1024n}];
        return {execute: () => Promise.resolve(row)};
      },
    } as unknown as PostgresDB;

    const state = await getInitialDownloadState(
      createSilentLogContext(),
      sql,
      tableSpec(),
      false,
    );
    expect(received).toHaveLength(2);
    expect(received.some(s => s.includes('COUNT(*)'))).toBe(true);
    expect(received.some(s => s.includes('pg_column_size'))).toBe(true);
    expect(state.status.totalRows).toBe(42);
    expect(state.status.totalBytes).toBe(1024);
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
      'test_slot',
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

    await createReplicationSlot(createSilentLogContext(), session, 's');
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
      createReplicationSlot(createSilentLogContext(), session, 'test_slot'),
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
      const result = createReplicationSlot(
        createSilentLogContext(),
        session,
        'hang_slot',
      ).catch(e => {
        caught = e;
      });

      // Advance past the 5s client-side timeout.
      await vi.advanceTimersByTimeAsync(6_000);
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
