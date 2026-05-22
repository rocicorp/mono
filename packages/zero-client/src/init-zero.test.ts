import {describe, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import type {
  ClientTransaction,
  ServerTransaction,
  Transaction,
} from '../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import type {Row} from '../../zql/src/query/query.ts';
import type {ZeroOptions} from './client/options.ts';
import type {Zero as ZeroClient} from './client/zero.ts';
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
type IssueRow = {
  readonly id: string;
  readonly title: string;
  readonly creatorID: string;
};

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
        create: result.defineMutator<{id: string}>(async ({tx}) => {
          expectTypeOf(tx).toEqualTypeOf<
            Transaction<typeof schema, WrappedTransaction>
          >();
          await Promise.resolve();
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
      issueByID: result.defineQuery(({args}: {args: {id: string}}) =>
        builder.issue.where('id', args.id).one(),
      ),
    });
    expectTypeOf<(typeof queries)['~']['$schema']>().toEqualTypeOf<
      typeof schema
    >();

    const queryRequest = queries.issueByID({id: '1'});

    expectTypeOf<Row<typeof schema>['issue']>().toEqualTypeOf<IssueRow>();
    expectTypeOf<Row<typeof issue.schema>>().toEqualTypeOf<IssueRow>();
    expectTypeOf<
      (typeof result)['~']['$row']['issue']
    >().toEqualTypeOf<IssueRow>();
    expectTypeOf<Row<typeof queries.issueByID>>().toEqualTypeOf<
      IssueRow | undefined
    >();
    expectTypeOf<Row<typeof queryRequest>>().toEqualTypeOf<
      IssueRow | undefined
    >();

    expectTypeOf<
      Transaction<typeof schema, WrappedTransaction>
    >().toEqualTypeOf<
      | ServerTransaction<typeof schema, WrappedTransaction>
      | ClientTransaction<typeof schema>
    >();
    expectTypeOf<(typeof result)['~']['$transaction']>().toEqualTypeOf<
      Transaction<typeof schema, WrappedTransaction>
    >();
    expectTypeOf<(typeof result)['~']['$clientTransaction']>().toEqualTypeOf<
      ClientTransaction<typeof schema>
    >();
    expectTypeOf<(typeof result)['~']['$serverTransaction']>().toEqualTypeOf<
      ServerTransaction<typeof schema, WrappedTransaction>
    >();
    expectTypeOf<(typeof result)['~']['$zero']>().toEqualTypeOf<
      ZeroClient<typeof schema, undefined, AuthData | undefined>
    >();

    const resultWithSchema = initZero<
      typeof schema,
      AuthData | undefined,
      WrappedTransaction
    >({schema});
    expectTypeOf<
      ConstructorParameters<typeof resultWithSchema.Zero>[0]
    >().toEqualTypeOf<
      Omit<
        ZeroOptions<typeof schema, undefined, AuthData | undefined>,
        'schema'
      >
    >();
    expectTypeOf<typeof resultWithSchema.schema>().toEqualTypeOf<
      typeof schema
    >();
    expectTypeOf<
      (typeof resultWithSchema)['~']['$transaction']
    >().toEqualTypeOf<Transaction<typeof schema, WrappedTransaction>>();
  });
});
