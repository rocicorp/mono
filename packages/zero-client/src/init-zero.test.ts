import {describe, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import type {Transaction} from '../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import {initZero} from './init-zero.ts';

const issue = table('issue')
  .columns({
    id: string(),
    title: string(),
    creatorID: string(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [issue],
});

const builder = createBuilder(schema);

type AuthData = {userID: string; role: 'admin' | 'user'};
type WrappedTransaction = {readonly tag: 'wrapped'};

describe('initZero', () => {
  test('returns typed utilities', () => {
    const result = initZero<
      typeof schema,
      AuthData | undefined,
      WrappedTransaction
    >();

    expectTypeOf<
      ReturnType<typeof result.defineMutator>['~']['$context']
    >().toEqualTypeOf<AuthData | undefined>();
    expectTypeOf<
      ReturnType<typeof result.defineQuery>['~']['$context']
    >().toEqualTypeOf<AuthData | undefined>();

    const mutators = result.defineMutators({
      issue: {
        // oxlint-disable-next-line require-await
        create: result.defineMutator(async ({tx}) => {
          expectTypeOf(tx).toEqualTypeOf<
            Transaction<typeof schema, WrappedTransaction>
          >();
        }),
      },
    });
    expectTypeOf<(typeof mutators)['~']['$schema']>().toEqualTypeOf<
      typeof schema
    >();

    const queries = result.defineQueries({
      issue: result.defineQuery(({ctx}) => {
        expectTypeOf(ctx).toEqualTypeOf<AuthData | undefined>();
        return builder.issue;
      }),
    });
    expectTypeOf<(typeof queries)['~']['$schema']>().toEqualTypeOf<
      typeof schema
    >();
  });
});
