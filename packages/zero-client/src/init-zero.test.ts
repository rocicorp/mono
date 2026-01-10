import {describe, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import type {Transaction} from '../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import {initZero} from './init-zero.ts';

const issueTable = table('issue')
  .columns({
    id: string(),
    title: string(),
    creatorID: string(),
  })
  .primaryKey('id');

const userTable = table('user')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [issueTable, userTable],
});

const zql = createBuilder(schema);

type AuthData = {userID: string; role: 'admin' | 'user'};

describe('initZero', () => {
  test('returns typed utilities', () => {
    const result = initZero<typeof schema, AuthData | undefined>();

    expectTypeOf<
      ReturnType<typeof result.defineMutator>['~']['$context']
    >().toEqualTypeOf<AuthData | undefined>();
    expectTypeOf<
      ReturnType<typeof result.defineQuery>['~']['$context']
    >().toEqualTypeOf<AuthData | undefined>();

    const m = result.defineMutators({
      issue: {
        // oxlint-disable-next-line require-await
        create: result.defineMutator(async ({tx}) => {
          expectTypeOf(tx).toEqualTypeOf<Transaction<typeof schema, unknown>>();
        }),
      },
    });
    expectTypeOf<(typeof m)['~']['$schema']>().toEqualTypeOf<typeof schema>();

    const q = result.defineQueries({
      issue: result.defineQuery(({ctx}) => {
        expectTypeOf(ctx).toEqualTypeOf<AuthData | undefined>();
        return zql.issue;
      }),
    });
    expectTypeOf<(typeof q)['~']['$schema']>().toEqualTypeOf<typeof schema>();
  });
});
