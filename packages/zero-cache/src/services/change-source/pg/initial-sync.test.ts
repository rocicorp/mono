import {describe, expect, test} from 'vitest';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import {makeDownloadStatements} from './initial-sync.ts';

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
