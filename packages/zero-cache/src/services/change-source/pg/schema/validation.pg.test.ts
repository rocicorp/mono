import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../../../shared/src/logging-test-utils.ts';
import {initDB, type PgTest, test} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {getPublicationInfo} from './published.ts';
import {UnsupportedTableSchemaError, validate} from './validation.ts';

describe('change-source/pg', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('zero_schema_validation_test');

    return () => testDBs.drop(db);
  });

  type InvalidTableCase = {
    error: string;
    setupUpstreamQuery: string;
  };

  const invalidUpstreamCases: InvalidTableCase[] = [
    {
      error: 'uses reserved column name "_0_version"',
      setupUpstreamQuery: `
        CREATE TABLE issues(
          "issueID" INTEGER PRIMARY KEY, 
          "orgID" INTEGER, 
          _0_version INTEGER);
      `,
    },
    {
      error: 'Table "table/with/slashes" has invalid characters',
      setupUpstreamQuery: `
        CREATE TABLE "table/with/slashes" ("issueID" INTEGER PRIMARY KEY, "orgID" INTEGER);
      `,
    },
    {
      error: 'Table "table.with.dots" has invalid characters',
      setupUpstreamQuery: `
        CREATE TABLE "table.with.dots" ("issueID" INTEGER PRIMARY KEY, "orgID" INTEGER);
      `,
    },
    {
      error:
        'Column "column/with/slashes" in table "issues" has invalid characters',
      setupUpstreamQuery: `
        CREATE TABLE issues ("issueID" INTEGER PRIMARY KEY, "column/with/slashes" INTEGER);
      `,
    },
    {
      error:
        'UnsupportedTableSchemaError: Table "issues" is missing its REPLICA IDENTITY INDEX',
      setupUpstreamQuery: `
        CREATE TABLE issues ("issueID" INTEGER NOT NULL, "foo" INTEGER);
        CREATE UNIQUE INDEX issues_idx ON issues ("issueID");
        ALTER TABLE issues REPLICA IDENTITY USING INDEX issues_idx;
        DROP INDEX issues_idx;
      `,
    },
    {
      error:
        'UnsupportedTableSchemaError: Table "issues" with REPLICA IDENTITY NOTHING cannot be replicated',
      setupUpstreamQuery: `
        CREATE TABLE issues ("issueID" INTEGER NOT NULL, "foo" INTEGER);
        ALTER TABLE issues REPLICA IDENTITY NOTHING;
      `,
    },
  ];

  for (const c of invalidUpstreamCases) {
    test(`Invalid upstream: ${c.error}`, async () => {
      await initDB(
        db,
        `CREATE PUBLICATION zero_all FOR ALL TABLES; ` + c.setupUpstreamQuery,
      );

      const pubs = await getPublicationInfo(db, ['zero_all']);
      expect(pubs.tables.length).toBe(1);
      let result;
      try {
        validate(lc, pubs.tables[0]);
      } catch (e) {
        result = e;
      }
      expect(result).toBeInstanceOf(UnsupportedTableSchemaError);
      expect(String(result)).toContain(c.error);
    });
  }

  type RowFilterCase = {
    name: string;
    setupUpstreamQuery: string;
    uncovered: string[];
  };

  const rowFilterCases: RowFilterCase[] = [
    {
      name: 'filter on the primary key',
      setupUpstreamQuery: `
        CREATE PUBLICATION zero_data FOR TABLE issues WHERE ("issueID" > 10);
      `,
      uncovered: [],
    },
    {
      name: 'filter on a column outside the default replica identity',
      setupUpstreamQuery: `
        CREATE PUBLICATION zero_data FOR TABLE issues WHERE ("orgID" = 1);
      `,
      uncovered: ['orgID'],
    },
    {
      name: 'filter covered by REPLICA IDENTITY USING INDEX',
      setupUpstreamQuery: `
        CREATE UNIQUE INDEX issues_key ON issues ("issueID", "orgID");
        ALTER TABLE issues REPLICA IDENTITY USING INDEX issues_key;
        CREATE PUBLICATION zero_data FOR TABLE issues WHERE ("orgID" = 1);
      `,
      uncovered: [],
    },
    {
      name: 'filter covered by REPLICA IDENTITY FULL',
      setupUpstreamQuery: `
        ALTER TABLE issues REPLICA IDENTITY FULL;
        CREATE PUBLICATION zero_data FOR TABLE issues WHERE ("orgID" = 1);
      `,
      uncovered: [],
    },
    {
      name: 'functions, casts, literals and collations are not columns',
      setupUpstreamQuery: `
        ALTER TABLE issues
          ADD COLUMN hashtext INTEGER,
          ADD COLUMN varying INTEGER,
          ADD COLUMN text INTEGER,
          ADD COLUMN "C" INTEGER;
        CREATE PUBLICATION zero_data FOR TABLE issues WHERE (
          (hashtext(title) & 3) = 2
          AND title::character varying(10) <> 'hashtext'
          AND title COLLATE "C" <> 'x "orgID" x'
          AND title <> E'text\\\\'
        );
      `,
      uncovered: ['title'],
    },
    {
      name: 'each publication is checked',
      setupUpstreamQuery: `
        CREATE PUBLICATION zero_data FOR TABLE issues WHERE ("issueID" > 10);
        CREATE PUBLICATION zero_two FOR TABLE issues WHERE (title IS NOT NULL);
      `,
      uncovered: ['title'],
    },
  ];

  for (const c of rowFilterCases) {
    test(`Row filter: ${c.name}`, async () => {
      await initDB(
        db,
        `CREATE TABLE issues ("issueID" INTEGER PRIMARY KEY, "orgID" INTEGER NOT NULL, title TEXT);` +
          c.setupUpstreamQuery,
      );

      const sink = new TestLogSink();
      const pubs = await getPublicationInfo(db, ['zero_data', 'zero_two']);
      validate(new LogContext('warn', {}, sink), pubs.tables[0]);

      const warnings = sink.messages
        .filter(([level]) => level === 'warn')
        .map(([, , args]) => String(args[0]));
      if (c.uncovered.length === 0) {
        expect(warnings).toEqual([]);
      } else {
        expect(warnings).toHaveLength(1);
        expect(warnings[0]).toContain(
          `references ${c.uncovered.map(col => `"${col}"`).join(', ')}, which`,
        );
      }
    });
  }
});
