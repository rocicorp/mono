import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {
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
