import {expect, test} from 'vitest';
import {relationships} from './builder/relationship-builder.ts';
import {createSchema} from './builder/schema-builder.ts';
import {string, table} from './builder/table-builder.ts';
import {definePermissions} from './permissions.ts';
import {parseSchema, stringifySchema} from './schema-config.ts';

test('round trip', async () => {
  const circular = table('circular')
    .columns({
      id: string(),
    })
    .primaryKey('id');

  const circularRelationships = relationships(circular, connect => ({
    self: connect.many({
      sourceField: ['id'],
      destField: ['id'],
      destSchema: circular,
    }),
  }));
  const schema = createSchema({
    tables: [circular],
    relationships: [circularRelationships],
  });

  const schemaAndPermissions = {
    schema,
    permissions: definePermissions<{sub: string}, typeof schema>(
      schema,
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
