import {expect, test} from 'vitest';
import {createSchema, type Schema} from './schema.js';
import {normalizeSchema} from './normalized-schema.js';
import {
  parseSchema,
  replacePointersWithSchemaNames,
  replaceSchemaNamesWithPointers,
  stringifySchema,
} from './schema-config.js';
import {definePermissions} from './permissions.js';

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

test('round trip', async () => {
  const circular = {
    tableName: 'circular',
    columns: {
      id: {type: 'string'},
    },
    primaryKey: ['id'],
    relationships: {
      self: {
        sourceField: 'id',
        destField: 'id',
        destSchema: () => circular,
      },
    },
  } as const;
  const baseSchema = createSchema({
    tables: {
      circular,
    },
    version: 1,
  });
  const schema = normalizeSchema(baseSchema);

  const schemaAndPermissions = {
    schema,
    permissions: definePermissions<{sub: string}, typeof baseSchema>(
      baseSchema,
      () => ({
        circular: {
          row: {
            select: [(_, eb) => eb.exists('self')],
          },
        },
      }),
    ),
  };
  const roundTripped = parseSchema(
    await stringifySchema(schemaAndPermissions),
    'test',
  );
  expect(roundTripped).toEqual({
    schema: schemaAndPermissions.schema,
    permissions: await schemaAndPermissions.permissions,
  });
});
