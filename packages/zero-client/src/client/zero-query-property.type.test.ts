import {expectTypeOf, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import type {SchemaQuery} from '../../../zql/src/query/schema-query.ts';
import type {Zero} from './zero.ts';

const schema = createSchema({
  tables: [
    table('user')
      .columns({
        id: string(),
        name: string(),
        age: number(),
      })
      .primaryKey('id'),
    table('post')
      .columns({
        id: string(),
        title: string(),
        authorId: string(),
      })
      .primaryKey('id'),
  ],
  enableLegacyQueries: true,
});

type Schema = typeof schema;

test('Zero.query type equals SchemaQuery<Schema>', () => {
  type ZeroInstance = Zero<Schema>;
  type QueryProp = ZeroInstance['query'];

  expectTypeOf<QueryProp>().toEqualTypeOf<SchemaQuery<Schema>>();
});

test('Zero.query.user equals Query<"user", Schema>', () => {
  type ZeroInstance = Zero<Schema>;
  type UserQuery = ZeroInstance['query']['user'];

  expectTypeOf<UserQuery>().toEqualTypeOf<Query<'user', Schema>>();
});

test('Zero.query.post equals Query<"post", Schema>', () => {
  type ZeroInstance = Zero<Schema>;
  type PostQuery = ZeroInstance['query']['post'];

  expectTypeOf<PostQuery>().toEqualTypeOf<Query<'post', Schema>>();
});
