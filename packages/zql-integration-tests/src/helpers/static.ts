import {ZPGQuery} from '../../../zero-server/src/zpg-query.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {QueryImpl} from '../../../zql/src/query/query-impl.ts';
import {queryWithContext} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery, Query} from '../../../zql/src/query/query.ts';
import {type bootstrap} from './runner.ts';

export function staticToRunnable<TSchema extends Schema>({
  query,
  schema,
  harness,
}: {
  query: AnyQuery;
  schema: TSchema;
  harness: Awaited<ReturnType<typeof bootstrap>>;
}): {
  // oxlint-disable-next-line no-explicit-any
  memory: Query<TSchema, any, any, any>;
  // oxlint-disable-next-line no-explicit-any
  pg: Query<TSchema, any, any, any>;
  // oxlint-disable-next-line no-explicit-any
  sqlite: Query<TSchema, any, any, any>;
} {
  const qi = queryWithContext(query, undefined);
  // reconstruct the generated query
  // for zql, zqlite and pg
  const zql = new QueryImpl(schema, qi.ast.table, qi.ast, qi.format);
  const zqlite = new QueryImpl(schema, qi.ast.table, qi.ast, qi.format);
  const pg = new ZPGQuery(
    schema,
    harness.delegates.pg.serverSchema,
    qi.ast.table,
    harness.delegates.pg.transaction,
    qi.ast,
    qi.format,
  );

  return {
    memory: zql,
    pg,
    sqlite: zqlite,
  };
}
