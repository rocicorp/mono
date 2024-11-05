import {expect, test} from 'vitest';
import type {TableSchema} from '../../../zero-schema/src/table-schema.js';
import {normalizeSchema} from './normalized-schema.js';
import type {Schema} from '../../../zero-schema/src/mod.js';

// Use JSON to preserve the order of properties since
// the testing framework doesn't care about the order.
function normalizeSchemaJSON(schema: Schema): string {
  return JSON.stringify(normalizeSchema(schema), null, 2);
}

test('Invalid table name', () => {
  expect(() =>
    normalizeSchema({
      tables: {
        foo: {
          tableName: 'bar',
          primaryKey: ['id'],
          columns: {
            id: {type: 'string'},
            a: {type: 'number'},
          },
          relationships: {},
        },
      },
      version: 1,
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Table name mismatch: "bar" !== "foo"]`,
  );
});

test('sort the table names', () => {
  expect(
    normalizeSchemaJSON({
      tables: {
        foo: {
          tableName: 'foo',
          primaryKey: ['id'],
          columns: {
            id: {type: 'string'},
          },
          relationships: {},
        },
        bar: {
          tableName: 'bar',
          primaryKey: ['id'],
          columns: {
            id: {type: 'string'},
          },
          relationships: {},
        },
      },
      version: 1,
    }),
  ).toMatchInlineSnapshot(
    `
    "{
      "version": 1,
      "tables": {
        "bar": {
          "tableName": "bar",
          "primaryKey": [
            "id"
          ],
          "columns": {
            "id": {
              "type": "string",
              "optional": false
            }
          },
          "relationships": {}
        },
        "foo": {
          "tableName": "foo",
          "primaryKey": [
            "id"
          ],
          "columns": {
            "id": {
              "type": "string",
              "optional": false
            }
          },
          "relationships": {}
        }
      }
    }"
  `,
  );
});

test('order version and table properties', () => {
  expect(
    normalizeSchemaJSON({
      tables: {
        foo: {
          tableName: 'foo',
          primaryKey: ['id'],
          columns: {
            id: {type: 'string'},
          },
          relationships: {},
        },
      },
      version: 1,
    }),
  ).toMatchInlineSnapshot(
    `
    "{
      "version": 1,
      "tables": {
        "foo": {
          "tableName": "foo",
          "primaryKey": [
            "id"
          ],
          "columns": {
            "id": {
              "type": "string",
              "optional": false
            }
          },
          "relationships": {}
        }
      }
    }"
  `,
  );
});

test('Mutually resolving relationships should be supported', () => {
  const fooTableSchema: TableSchema = {
    tableName: 'foo',
    primaryKey: ['id'],
    columns: {
      id: {type: 'string'},
    },
    relationships: {
      bar: {
        dest: {
          field: 'field',
          schema: () => barTableSchema,
        },
        source: 'bar-source',
      },
    },
  };

  const barTableSchema: TableSchema = {
    tableName: 'bar',
    primaryKey: ['id'],
    columns: {
      id: {type: 'string'},
    },
    relationships: {
      foo: {
        dest: {
          field: 'field',
          schema: () => fooTableSchema,
        },
        source: 'foo-source',
      },
    },
  };

  const normalizedFooSchema = normalizeSchema({
    tables: {foo: fooTableSchema, bar: barTableSchema},
    version: 1,
  });

  expect(normalizedFooSchema.tables.foo.relationships.bar.dest.schema).toBe(
    normalizedFooSchema.tables.bar,
  );
  expect(normalizedFooSchema.tables.bar.relationships.foo.dest.schema).toBe(
    normalizedFooSchema.tables.foo,
  );
});
