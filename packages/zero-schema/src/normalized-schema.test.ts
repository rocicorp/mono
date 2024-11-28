import {expect, test} from 'vitest';
import {assert} from '../../shared/src/asserts.js';
import {normalizeSchema} from './normalized-schema.js';
import type {Schema} from './schema.js';
import {isFieldRelationship} from './table-schema.js';

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
  const fooTableSchema = {
    tableName: 'foo',
    primaryKey: ['id'],
    columns: {
      id: {type: 'string'},
    },
    relationships: {
      bar: {
        sourceField: ['bar-source'],
        destField: ['field'],
        destSchema: () => barTableSchema,
      },
    },
  } as const;

  const barTableSchema = {
    tableName: 'bar',
    primaryKey: ['id'],
    columns: {
      id: {type: 'string'},
    },
    relationships: {
      foo: {
        sourceField: ['foo-source'],
        destField: ['field'],
        destSchema: () => fooTableSchema,
      },
    },
  } as const;

  const normalizedFooSchema = normalizeSchema({
    tables: {foo: fooTableSchema, bar: barTableSchema},
    version: 1,
  });

  assert(isFieldRelationship(normalizedFooSchema.tables.foo.relationships.bar));
  expect(normalizedFooSchema.tables.foo.relationships.bar.destSchema).toBe(
    normalizedFooSchema.tables.bar,
  );
  assert(isFieldRelationship(normalizedFooSchema.tables.bar.relationships.foo));
  expect(normalizedFooSchema.tables.bar.relationships.foo.destSchema).toBe(
    normalizedFooSchema.tables.foo,
  );
});
