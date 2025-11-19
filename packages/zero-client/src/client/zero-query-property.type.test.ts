import {expectTypeOf, test} from 'vitest';
import {z} from 'zod';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {defineQueryWithContextType} from '../../../zql/src/query/define-query.ts';
import {createBuilder} from '../../../zql/src/query/named.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import type {Zero} from './zero.ts';

test('Zero.query type with custom queries', () => {
  // Define a simple schema
  const schema = createSchema({
    tables: [
      table('user')
        .columns({
          id: string(),
          name: string(),
          age: number(),
        })
        .primaryKey('id'),
    ],
  });

  type Schema = typeof schema;
  const builder = createBuilder(schema);

  // Define some queries with a context type
  type AuthContext = {sub: string} | undefined;
  const defineAuthQuery = defineQueryWithContextType<AuthContext>();

  const queries = {
    allUsers: defineAuthQuery(z.undefined(), () => builder.user),
    userById: defineAuthQuery(z.string(), ({args}) =>
      builder.user.where('id', args).one(),
    ),
    currentUser: defineAuthQuery(z.undefined(), () => builder.user),
  };

  // Test without validators - should infer types from destructuring
  const queriesNoValidator = {
    allUsers2: defineAuthQuery(() => builder.user),
    userById2: defineAuthQuery(({args}: {args: string}) =>
      builder.user.where('id', args).one(),
    ),
  };

  type ZeroWithQueriesNoValidator = Zero<
    Schema,
    undefined,
    AuthContext,
    typeof queriesNoValidator
  >;
  type QueryPropNoValidator = ZeroWithQueriesNoValidator['query'];

  // Test that userById2 infers the correct type
  type UserById2Query = QueryPropNoValidator['userById2'];
  expectTypeOf<UserById2Query>().toEqualTypeOf<
    (
      args: string,
    ) => Query<
      Schema,
      'user',
      {id: string; name: string; age: number} | undefined
    >
  >();

  // Test that allUsers2 works
  type AllUsers2Query = QueryPropNoValidator['allUsers2'];
  expectTypeOf<AllUsers2Query>().toBeCallableWith();

  // Create Zero type with queries
  type ZeroWithQueries = Zero<Schema, undefined, AuthContext, typeof queries>;

  // Test that the query property has the expected structure
  type QueryProp = ZeroWithQueries['query'];

  // Should have entity queries (from schema)
  expectTypeOf<QueryProp>().toHaveProperty('user');

  // Should have custom queries (from queries object)
  expectTypeOf<QueryProp>().toHaveProperty('allUsers');
  expectTypeOf<QueryProp>().toHaveProperty('userById');
  expectTypeOf<QueryProp>().toHaveProperty('currentUser');

  // Test that custom queries are callable functions with proper return types
  type AllUsersQuery = QueryProp['allUsers'];
  type UserByIdQuery = QueryProp['userById'];

  // allUsers should be callable with no args (no parameter at all when args is undefined)
  expectTypeOf<AllUsersQuery>().toBeCallableWith();
  expectTypeOf<AllUsersQuery>().toEqualTypeOf<
    () => Query<Schema, 'user', {id: string; name: string; age: number}[]>
  >();

  // userById should require a string argument (args is string, not undefined)
  expectTypeOf<UserByIdQuery>().toEqualTypeOf<
    (
      args: string,
    ) => Query<
      Schema,
      'user',
      {id: string; name: string; age: number} | undefined
    >
  >();
});
