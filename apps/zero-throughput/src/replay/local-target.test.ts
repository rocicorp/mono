import {expect, test} from 'vitest';
import {databaseURL, schemaDDL} from './local-target.ts';

test('schemaDDL creates schemas, tables, *_id indexes and a publication', () => {
  expect(
    schemaDDL({
      tables: {
        'catalog.work_titles': {
          columns: {
            title_id: {type: 'string'},
            work_id: {type: 'string'},
            is_primary: {type: 'boolean'},
            position: {type: 'number'},
            extra: {type: 'json'},
          },
          primaryKey: ['title_id'],
        },
        'languages': {
          columns: {bcp47_code: {type: 'string'}},
          primaryKey: ['bcp47_code'],
        },
      },
    }),
  ).toEqual([
    'CREATE SCHEMA IF NOT EXISTS "catalog"',
    'CREATE TABLE "catalog"."work_titles" (\n' +
      '  "title_id" text NOT NULL,\n' +
      '  "work_id" text,\n' +
      '  "is_primary" boolean,\n' +
      '  "position" double precision,\n' +
      '  "extra" jsonb,\n' +
      '  PRIMARY KEY ("title_id")\n' +
      ')',
    'CREATE INDEX ON "catalog"."work_titles" ("work_id")',
    'CREATE SCHEMA IF NOT EXISTS "public"',
    'CREATE TABLE "languages" (\n' +
      '  "bcp47_code" text NOT NULL,\n' +
      '  PRIMARY KEY ("bcp47_code")\n' +
      ')',
    'CREATE PUBLICATION replay_tables FOR TABLES IN SCHEMA "catalog", "public"',
  ]);
});

test('databaseURL swaps the database', () => {
  expect(
    databaseURL('postgresql://user:password@127.0.0.1:6436/postgres', 'replay'),
  ).toBe('postgresql://user:password@127.0.0.1:6436/replay');
});
