import {expect, test} from 'vitest';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import {
  normalizePrimaryKey,
  normalizeTableSchema,
} from './normalize-table-schema.js';
import type {TableSchema} from './schema.js';

// Use JSON to preserve the order of properties since
// the testing framework doesn't care about the order.
function normalizeTableSchemaJSON(tableSchema: TableSchema): string {
  return JSON.stringify(normalizeTableSchema(tableSchema), null, 2);
}

test('normalizePrimaryKey', () => {
  let pk: PrimaryKey = ['a'];
  expect(normalizePrimaryKey(pk)).toBe(pk);
  pk = ['a', 'b'];
  expect(normalizePrimaryKey(pk)).toBe(pk);

  pk = ['b', 'a'];
  expect(normalizePrimaryKey(pk)).toEqual(['a', 'b']);

  pk = ['b', 'a', 'c'];
  expect(normalizePrimaryKey(pk)).toEqual(['a', 'b', 'c']);

  pk = ['a', 'b', 'a'];
  expect(() => normalizePrimaryKey(pk)).toThrow(
    new Error('Primary key must not contain duplicates'),
  );
});

test('sort the column names', () => {
  expect(
    normalizeTableSchemaJSON({
      tableName: 'foo',
      primaryKey: ['id'],
      columns: {
        id: {type: 'string'},
        a: {type: 'number'},
      },
      relationships: {},
    }),
  ).toMatchInlineSnapshot(`
    "{
      "tableName": "foo",
      "primaryKey": [
        "id"
      ],
      "columns": {
        "a": {
          "type": "number",
          "optional": false
        },
        "id": {
          "type": "string",
          "optional": false
        }
      },
      "relationships": {}
    }"
  `);
});

test('Invalid primary key type', () => {
  expect(() =>
    normalizeTableSchema({
      tableName: 'foo',
      primaryKey: ['id'],
      columns: {
        id: {type: 'null'},
        a: {type: 'number'},
      },
      relationships: {},
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Primary key column "id" must be a string, number, or boolean. Got null]`,
  );
});

test('Invalid primary key column not found', () => {
  expect(() =>
    normalizeTableSchema({
      tableName: 'foo',
      primaryKey: ['id'],
      columns: {
        a: {type: 'number'},
      },
      relationships: {},
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Primary key column "id" not found]`,
  );
});

test('Invalid primary key column optional', () => {
  expect(() =>
    normalizeTableSchema({
      tableName: 'foo',
      primaryKey: ['id'],
      columns: {
        id: {type: 'string', optional: true},
        a: {type: 'number'},
      },
      relationships: {},
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Primary key column "id" cannot be optional]`,
  );
});

test('add optional to schema value', () => {
  expect(
    normalizeTableSchemaJSON({
      tableName: 'foo',
      primaryKey: ['id'],
      columns: {
        id: {type: 'string'},
        a: {type: 'number', optional: true},
      },
      relationships: {},
    }),
  ).toMatchInlineSnapshot(
    `
    "{
      "tableName": "foo",
      "primaryKey": [
        "id"
      ],
      "columns": {
        "a": {
          "type": "number",
          "optional": true
        },
        "id": {
          "type": "string",
          "optional": false
        }
      },
      "relationships": {}
    }"
  `,
  );
});

test('relationships should be sorted', () => {
  const barSchema: TableSchema = {
    tableName: 'bar',
    primaryKey: ['id'],
    columns: {
      id: {type: 'string'},
    },
    relationships: {},
  };
  const fooSchema: TableSchema = {
    tableName: 'foo',
    primaryKey: ['id'],
    columns: {
      id: {type: 'string'},
    },
    relationships: {
      b: {
        dest: {
          field: 'field',
          schema: barSchema,
        },
        source: 'bar-source',
      },
      a: {
        dest: {
          field: 'field',
          schema: () => barSchema,
        },
        source: 'bar-source',
      },
    },
  };

  expect(normalizeTableSchemaJSON(fooSchema)).toMatchInlineSnapshot(
    `
    "{
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
      "relationships": {
        "a": {
          "source": "bar-source",
          "dest": {
            "field": "field",
            "schema": {
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
            }
          }
        },
        "b": {
          "source": "bar-source",
          "dest": {
            "field": "field",
            "schema": {
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
            }
          }
        }
      }
    }"
  `,
  );
});

test('Cyclic relationship should be supported', () => {
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

          schema: () => fooTableSchema,
        },
        source: 'bar-source',
      },
    },
  };

  const normalizedFooTableSchema = normalizeTableSchema(fooTableSchema);

  expect(normalizedFooTableSchema.relationships.bar.dest.schema).toBe(
    normalizedFooTableSchema,
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

  const normalizedFooTableSchema = normalizeTableSchema(fooTableSchema);

  expect(
    normalizedFooTableSchema.relationships.bar.dest.schema.relationships.foo
      .dest.schema,
  ).toBe(normalizedFooTableSchema);
});
