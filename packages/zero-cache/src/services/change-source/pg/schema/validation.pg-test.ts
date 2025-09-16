/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.ts';
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
        validate(lc, pubs.tables[0], pubs.indexes);
      } catch (e) {
        result = e;
      }
      expect(result).toBeInstanceOf(UnsupportedTableSchemaError);
      expect(String(result)).toContain(c.error);
    });
  }
});
