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

test('Missing table in direct relationship should throw', () => {
  const bar = {
    tableName: 'bar',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
  } as const;

  const foo = {
    tableName: 'foo',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      bar_id: {type: 'number'},
    },
    relationships: {
      bar: {
        sourceField: 'bar_id',
        destSchema: () => bar,
        destField: 'id',
      },
    },
  } as const;

  const schema = {
    version: 1,
    tables: {
      foo: foo,
    },
  } as const;

  expect(() => createSchema(schema)).toThrow(
    'createSchema relationship missing, foo relationship bar not present in schema.tables',
  );
});

test('Missing table in junction relationship should throw', () => {
  const baz = {
    tableName: 'baz',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
    },
  } as const;

  const bar = {
    tableName: 'bar',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      baz_id: {type: 'number'},
    },
    relationships: {
      baz: {
        sourceField: 'baz_id',
        destSchema: () => baz,
        destField: 'id',
      },
    },
  } as const;

  const foo = {
    tableName: 'foo',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      bar_id: {type: 'number'},
    },
    relationships: {
      baz: [
        {
          sourceField: 'bar_id',
          destSchema: () => bar,
          destField: 'id',
        },
        {
          sourceField: 'baz_id',
          destSchema: () => baz,
          destField: 'id',
        },
      ],
    },
  } as const;

  const schema = {
    version: 1,
    tables: {
      foo: foo,
      bar: bar,
    },
  } as const;

  expect(() => createSchema(schema)).toThrow(
    'createSchema relationship missing, foo relationship baz not present in schema.tables',
  );
});
