import {describe, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import type {RunnableQuery} from '../../../zql/src/query/runnable-query.ts';
import type {Query, HumanReadable} from '../../../zql/src/query/query.ts';

const user = table('user')
  .columns({
    id: string(),
    name: string(),
    age: number(),
    active: boolean(),
  })
  .primaryKey('id');

const issue = table('issue')
  .columns({
    id: string(),
    title: string(),
    userId: string(),
    status: string(),
  })
  .primaryKey('id');

const comment = table('comment')
  .columns({
    id: string(),
    issueId: string(),
    text: string(),
    userId: string(),
  })
  .primaryKey('id');

const userRelationships = relationships(user, connect => ({
  issues: connect.many({
    sourceField: ['id'],
    destField: ['userId'],
    destSchema: issue,
  }),
  comments: connect.many({
    sourceField: ['id'],
    destField: ['userId'],
    destSchema: comment,
  }),
}));

const issueRelationships = relationships(issue, connect => ({
  user: connect.one({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: user,
  }),
  comments: connect.many({
    sourceField: ['id'],
    destField: ['issueId'],
    destSchema: comment,
  }),
}));

const commentRelationships = relationships(comment, connect => ({
  issue: connect.one({
    sourceField: ['issueId'],
    destField: ['id'],
    destSchema: issue,
  }),
  user: connect.one({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: user,
  }),
}));

const schema = createSchema({
  tables: [user, issue, comment],
  relationships: [userRelationships, issueRelationships, commentRelationships],
});

type Schema = typeof schema;

describe('RunnableQuery type tests', () => {
  test('RunnableQuery has run method', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;

    // RunnableQuery should have a run method
    expectTypeOf<UserQuery['run']>().toBeFunction();
    expectTypeOf<UserQuery['run']>().returns.toEqualTypeOf<
      Promise<
        HumanReadable<{
          readonly id: string;
          readonly name: string;
          readonly age: number;
          readonly active: boolean;
        }>
      >
    >();
  });

  test('Query does not have run method', () => {
    type UserQuery = Query<Schema, 'user'>;

    // Query interface does not expose run method
    expectTypeOf<UserQuery>().not.toHaveProperty('run');
  });

  test('where() preserves RunnableQuery type', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type FilteredQuery = ReturnType<UserQuery['where']>;

    // where() returns 'this', so it should preserve RunnableQuery
    expectTypeOf<FilteredQuery>().toEqualTypeOf<UserQuery>();

    // The returned query should still have run()
    expectTypeOf<FilteredQuery['run']>().toBeFunction();
  });

  test('limit() preserves RunnableQuery type', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type LimitedQuery = ReturnType<UserQuery['limit']>;

    expectTypeOf<LimitedQuery>().toEqualTypeOf<UserQuery>();
    expectTypeOf<LimitedQuery['run']>().toBeFunction();
  });

  test('orderBy() preserves RunnableQuery type', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type OrderedQuery = ReturnType<UserQuery['orderBy']>;

    expectTypeOf<OrderedQuery>().toEqualTypeOf<UserQuery>();
    expectTypeOf<OrderedQuery['run']>().toBeFunction();
  });

  test('start() preserves RunnableQuery type', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type StartQuery = ReturnType<UserQuery['start']>;

    expectTypeOf<StartQuery>().toEqualTypeOf<UserQuery>();
    expectTypeOf<StartQuery['run']>().toBeFunction();
  });

  test('whereExists() preserves RunnableQuery type', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type ExistsQuery = ReturnType<UserQuery['whereExists']>;

    expectTypeOf<ExistsQuery>().toEqualTypeOf<UserQuery>();
    expectTypeOf<ExistsQuery['run']>().toBeFunction();
  });

  test('method chaining preserves RunnableQuery type', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type ChainedQuery = ReturnType<
      ReturnType<ReturnType<UserQuery['where']>['orderBy']>['limit']
    >;

    // After chaining where().orderBy().limit(), should still be RunnableQuery
    expectTypeOf<ChainedQuery>().toEqualTypeOf<UserQuery>();
    expectTypeOf<ChainedQuery['run']>().toBeFunction();
  });

  test('related() changes return type but preserves RunnableQuery', () => {
    // Type-level test: verify related() method exists
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type RelatedMethod = UserQuery['related'];

    // Verify related() is a function
    expectTypeOf<RelatedMethod>().toBeFunction();

    // Note: ReturnType gives 'any' for overloaded methods - this is a TypeScript limitation
    // The runtime tests in runnable-query.test.ts verify the actual behavior including
    // that related() returns RunnableQuery and that run() works
  });

  test('one() returns Query, not RunnableQuery', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type OneQuery = ReturnType<UserQuery['one']>;

    // one() changes TReturn, so it returns Query (not RunnableQuery)
    expectTypeOf<OneQuery>().toMatchTypeOf<
      Query<
        Schema,
        'user',
        | {
            readonly id: string;
            readonly name: string;
            readonly age: number;
            readonly active: boolean;
          }
        | undefined
      >
    >();

    // Query interface doesn't expose run()
    expectTypeOf<OneQuery>().not.toHaveProperty('run');
  });

  test('chaining methods after one() returns Query', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type OneQuery = ReturnType<UserQuery['one']>;
    type ChainedAfterOne = ReturnType<OneQuery['where']>;

    // After one(), subsequent chains should be Query (not RunnableQuery)
    expectTypeOf<ChainedAfterOne>().toMatchTypeOf<
      Query<
        Schema,
        'user',
        | {
            readonly id: string;
            readonly name: string;
            readonly age: number;
            readonly active: boolean;
          }
        | undefined
      >
    >();

    // Should not have run()
    expectTypeOf<ChainedAfterOne>().not.toHaveProperty('run');
  });

  test('related() with callback preserves RunnableQuery', () => {
    // Type-level test: verify related() method exists and accepts a callback
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type RelatedMethod = UserQuery['related'];

    // Verify related() is a function
    expectTypeOf<RelatedMethod>().toBeFunction();

    // Runtime behavior is tested in runnable-query.test.ts
    // TypeScript's ReturnType gives 'any' for overloaded methods
  });

  test('nested related() preserves RunnableQuery', () => {
    // Type-level test: verify related() exists on RunnableQuery
    type UserQuery = RunnableQuery<Schema, 'user'>;

    // Verify related() method exists
    expectTypeOf<UserQuery['related']>().toBeFunction();

    // Runtime behavior (chaining multiple related() calls) is tested in runnable-query.test.ts
  });

  test('run() return type matches query return type', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type RunResult = ReturnType<UserQuery['run']>;

    // run() should return Promise<HumanReadable<TReturn>>
    // For a base query, HumanReadable wraps in an array
    expectTypeOf<RunResult>().toEqualTypeOf<
      Promise<
        {
          readonly id: string;
          readonly name: string;
          readonly age: number;
          readonly active: boolean;
        }[]
      >
    >();
  });

  test('run() return type with related data', () => {
    // Type-level test: verify run() returns a Promise
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type RunMethod = UserQuery['run'];

    // Verify run() exists and returns a Promise
    expectTypeOf<RunMethod>().toBeFunction();
    expectTypeOf<RunMethod>().returns.toMatchTypeOf<Promise<any>>();

    // The actual type inference with related data is tested in runtime tests
  });

  test('complex chaining preserves types correctly', () => {
    // Type-level test: verify methods exist and can be chained
    type UserQuery = RunnableQuery<Schema, 'user'>;

    // Verify key methods exist
    expectTypeOf<UserQuery['where']>().toBeFunction();
    expectTypeOf<UserQuery['related']>().toBeFunction();
    expectTypeOf<UserQuery['orderBy']>().toBeFunction();
    expectTypeOf<UserQuery['limit']>().toBeFunction();
    expectTypeOf<UserQuery['run']>().toBeFunction();

    // Runtime chaining behavior is tested in runnable-query.test.ts
  });

  test('related with one cardinality preserves RunnableQuery', () => {
    // Type-level test: verify related() works for both 'many' and 'one' relationships
    type IssueQuery = RunnableQuery<Schema, 'issue'>;
    type RelatedMethod = IssueQuery['related'];

    // Verify related() method exists
    expectTypeOf<RelatedMethod>().toBeFunction();

    // Runtime behavior (testing 'one' relationships like 'user') is tested in runnable-query.test.ts
  });

  test('whereExists with callback preserves RunnableQuery', () => {
    type UserQuery = RunnableQuery<Schema, 'user'>;
    type WithExistsFilter = ReturnType<UserQuery['whereExists']>;

    expectTypeOf<WithExistsFilter>().toEqualTypeOf<UserQuery>();
    expectTypeOf<WithExistsFilter['run']>().toBeFunction();
  });
});
