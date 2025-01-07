import {expect, test} from 'vitest';
import {createSchema} from './builder/schema-builder.js';
import {parseSchema, stringifySchema} from './schema-config.js';
import {definePermissions} from './permissions.js';
import {string, table} from './builder/table-builder.js';
import {relationships} from './builder/relationship-builder.js';

test('round trip', async () => {
  const circular = table('circular')
    .columns({
      id: string(),
    })
    .primaryKey('id');

  const circularRelationships = relationships(circular, connect => ({
    self: connect({
      sourceField: ['id'],
      destField: ['id'],
      destSchema: circular,
    }),
  }));
  const schema = createSchema(1, {circular}, {circularRelationships});

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
