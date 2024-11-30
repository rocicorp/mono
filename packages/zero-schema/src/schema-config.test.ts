import {expect, test} from 'vitest';
import type {Schema} from './schema.js';
import {normalizeSchema} from './normalized-schema.js';
import {
  replacePointersWithSchemaNames,
  replaceSchemaNamesWithPointers,
} from './schema-config.js';

test('replace pointers, replace strings', () => {
  const foo = {
    tableName: 'foo',
    columns: {
      id: {type: 'string'},
      bar: {type: 'string'},
    },
    primaryKey: ['id'],
    relationships: {
      self: {
        sourceField: ['id'],
        destField: ['id'],
        destSchema: () => foo,
      },
      bar: {
        sourceField: ['bar'],
        destField: ['id'],
        destSchema: () => bar,
      },
    },
  } as const;

  const bar = {
    tableName: 'bar',
    columns: {
      id: {type: 'string'},
      foo: {type: 'string'},
    },
    primaryKey: ['id'],
    relationships: {
      self: {
        sourceField: ['id'],
        destField: ['id'],
        destSchema: () => bar,
      },
      foo: {
        sourceField: ['foo'],
        destField: ['id'],
        destSchema: () => foo,
      },
    },
  } as const;

  const schema = {
    version: 1,
    tables: {
      foo,
      bar,
    },
  } satisfies Schema;

  const normalized = normalizeSchema(schema);

  const replaced = replacePointersWithSchemaNames(normalized);
  const normal = replaceSchemaNamesWithPointers(replaced);

  expect(normal).toEqual(normalized);
  expect(replacePointersWithSchemaNames(normal)).toEqual(replaced);
});
