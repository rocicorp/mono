import {describe, expect, test} from 'vitest';
import {analyzeSql} from './analyzer.ts';

describe('zero migration impact analyzer', () => {
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
