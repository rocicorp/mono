import {expect, test} from 'vitest';
import {createSchema} from './schema.js';

test('Unexpected tableName should throw', () => {
  const schema = {
    version: 1,
    tables: {
      foo: {
        tableName: 'foo',
        primaryKey: 'id',
        columns: {
          id: {type: 'number'},
        },
      },
      bar: {
        tableName: 'bars',
        primaryKey: 'id',
        columns: {
          id: {type: 'number'},
        },
      },
    },
  } as const;
  expect(() => createSchema(schema)).toThrow(
    'createSchema tableName mismatch, expected bar === bars',
  );
});
