import {describe, expect, test} from 'vitest';
import {newQuery} from './query-impl.ts';
import {queryWithContext} from './query-internals.ts';
import {type AnyQuery} from './query.ts';
import {schema} from './test/test-schemas.ts';
import {completeOrdering} from './complete-ordering.ts';
import type {TableSchema} from '../../../zero-types/src/schema.ts';

function ast(q: AnyQuery) {
  return queryWithContext(q, undefined).ast;
}

const tables: Record<string, TableSchema> = schema.tables;

const getPrimaryKey = (tableName: string) => {
  return tables[tableName].primaryKey;
};

describe('completeOrdering', () => {
  test('basic', () => {
    const issueQuery = newQuery(schema, 'issue');
    expect(ast(issueQuery)).toMatchInlineSnapshot(`
      {
        "table": "issue",
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
      {
        "orderBy": [
          [
            "id",
            "asc",
          ],
        ],
        "table": "issue",
      }
    `);
  });
});
