import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.ts';
import {initDB, testDBs} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {getPublicationInfo} from './published.ts';
import {UnsupportedTableSchemaError, validate} from './validation.ts';

describe('change-source/pg', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;

  beforeEach(async () => {
    db = await testDBs.create('zero_schema_validation_test');
  });

  afterEach(async () => {
    await testDBs.drop(db);
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
});
