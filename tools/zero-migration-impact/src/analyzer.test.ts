import {describe, expect, test} from 'vitest';
import {mapPostgresToLiteDefault} from '../../../packages/zero-cache/src/db/pg-to-lite.ts';
import {
  dataTypeToZqlValueType,
  pgToZqlTypeMap,
} from '../../../packages/zero-cache/src/types/pg-data-type.ts';
import {analyzeSql} from './analyzer.ts';

describe('zero migration impact analyzer', () => {
  test('current supported type rules match zero-cache', () => {
    for (const [pgType, zqlType] of Object.entries(pgToZqlTypeMap)) {
      const zeroType = dataTypeToZqlValueType(pgType, false, false);
      expect(zeroType).toBe(zqlType);

      const result = analyzeSql(
        `CREATE TABLE test_table (id text PRIMARY KEY, value ${pgType});`,
      );
      expect(
        result.findings.filter(
          finding => finding.id === 'unsupported-column-type',
        ),
      ).toEqual([]);
    }

    const arrayResult = analyzeSql(
      `CREATE TABLE test_table (id text PRIMARY KEY, value text[]);`,
    );
    expect(dataTypeToZqlValueType('text', false, true)).toBe('json');
    expect(
      arrayResult.findings.filter(
        finding => finding.id === 'unsupported-column-type',
      ),
    ).toEqual([]);
  });

  test('current default rules match zero-cache', () => {
    for (const defaultExpression of [
      '0',
      'true',
      "'safe'::text",
      'ARRAY[]::text[]',
      "'{}'::text[]",
      'now()',
    ]) {
      let zeroNeedsBackfill = false;
      try {
        mapPostgresToLiteDefault('test_table', 'value', defaultExpression);
      } catch {
        zeroNeedsBackfill = true;
      }

      const result = analyzeSql(
        `ALTER TABLE test_table ADD COLUMN value text DEFAULT ${defaultExpression};`,
      );
      expect(
        result.findings.some(finding => finding.id === 'add-column-backfill'),
      ).toBe(zeroNeedsBackfill);
    }
  });

  test('flags backfill for volatile add-column defaults', () => {
    const result = analyzeSql(
      `ALTER TABLE issues ADD COLUMN created_at timestamptz DEFAULT now();`,
      'migration.sql',
    );

    expect(result.summary).toMatchObject({
      backfill: 'yes',
      schemaVersionNotSupported: 'possible',
      replicationLag: 'high',
      safety: 'review',
    });
    expect(result.findings.map(finding => finding.id)).toContain(
      'add-column-backfill',
    );
  });

  test('flags unsupported defaults as unsafe before auto-backfill', () => {
    const result = analyzeSql(
      `ALTER TABLE issues ADD COLUMN created_at timestamptz DEFAULT now();`,
      'migration.sql',
      {zeroVersion: '0.25.0'},
    );

    expect(result.summary).toMatchObject({
      backfill: 'no',
      replicationLag: 'low',
      safety: 'unsafe',
    });
    expect(result.findings.map(finding => finding.id)).toEqual([
      'add-column-default-unsupported-by-zero-version',
    ]);
  });

  test('flags any add-column default as unsafe before default support', () => {
    const result = analyzeSql(
      `ALTER TABLE groups ADD COLUMN "inviteLinkEnabled" boolean NOT NULL DEFAULT true;`,
      undefined,
      {zeroVersion: '0.0.202410010000'},
    );

    expect(result.summary).toMatchObject({
      backfill: 'no',
      safety: 'unsafe',
    });
    expect(result.findings.map(finding => finding.id)).toEqual([
      'add-column-default-unsupported-by-zero-version',
    ]);
  });

  test('treats simple additive columns as safe', () => {
    const result = analyzeSql(`
      ALTER TABLE issues ADD COLUMN sort_order integer DEFAULT 0;
      ALTER TABLE issues ADD COLUMN title text;
    `);

    expect(result.summary).toMatchObject({
      backfill: 'no',
      schemaVersionNotSupported: 'no',
      replicationLag: 'low',
      safety: 'safe',
    });
    expect(result.findings).toEqual([]);
  });

  test('flags destructive schema changes as client compatibility risks', () => {
    const result = analyzeSql(`
      ALTER TABLE issues DROP COLUMN owner_id;
      DROP TABLE comments;
    `);

    expect(result.summary.schemaVersionNotSupported).toBe('possible');
    expect(result.findings.map(finding => finding.id)).toEqual([
      'drop-column',
      'drop-table',
    ]);
  });

  test('does not treat table constraints as columns', () => {
    const result = analyzeSql(`
      ALTER TABLE "comment" ADD CONSTRAINT "comment_issueID_fkey" FOREIGN KEY ("issueID") REFERENCES "issue"("id");
      ALTER TABLE "comment" DROP CONSTRAINT "comment_issueID_fkey";
      ALTER TABLE "issue" DROP CONSTRAINT "issue_pkey";
    `);

    expect(result.findings.map(finding => finding.id)).toEqual([
      'drop-key-constraint',
    ]);
  });

  test('flags publication adds as backfills', () => {
    const result = analyzeSql(
      `ALTER PUBLICATION zero_data ADD TABLE public.audit_log;`,
    );

    expect(result.summary.backfill).toBe('yes');
    expect(result.summary.replicationLag).toBe('high');
    expect(result.findings[0]?.id).toBe('publication-add-table');
  });

  test('flags new tables without syncable keys', () => {
    const result = analyzeSql(`
      CREATE TABLE audit_log (
        id bytea PRIMARY KEY,
        payload jsonb,
        note text
      );
    `);

    expect(result.findings.map(finding => finding.id)).toEqual([
      'unsupported-column-type',
      'unsupported-primary-key-type',
    ]);
  });

  test('handles semicolons inside strings and comments', () => {
    const result = analyzeSql(`
      -- ignored ; semicolon
      ALTER TABLE issues ADD COLUMN note text DEFAULT 'safe; value';
      ALTER TABLE issues ADD COLUMN touched_at timestamptz DEFAULT now();
    `);

    expect(result.statementsAnalyzed).toBe(2);
    expect(result.findings.map(finding => finding.id)).toEqual([
      'add-column-backfill',
    ]);
  });

  test('flags unbounded dml', () => {
    const result = analyzeSql(`UPDATE issues SET status = 'closed';`);

    expect(result.summary.replicationLag).toBe('high');
    expect(result.findings[0]?.id).toBe('unbounded-dml');
  });
});
