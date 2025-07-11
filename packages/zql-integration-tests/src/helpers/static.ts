import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {ast, QueryImpl} from '../../../zql/src/query/query-impl.ts';
import type {AnyQuery} from '../../../zql/src/query/test/util.ts';
import {type bootstrap} from './runner.ts';
import {ZPGQuery} from '../../../zero-server/src/query.ts';

export function staticToRunnable<TSchema extends Schema>({
  query,
  schema,
  harness,
}: {
  query: AnyQuery;
  schema: TSchema;
  harness: Awaited<ReturnType<typeof bootstrap>>;
}) {
  // reconstruct the generated query
  // for zql, zqlite and pg
  const zql = new QueryImpl(
    harness.delegates.memory,
    schema,
    ast(query).table,
    ast(query),
    query.format,
  );
  const zqlite = new QueryImpl(
    harness.delegates.sqlite,
    schema,
    ast(query).table,
    ast(query),
    query.format,
  );
  const pg = new ZPGQuery(
    schema,
    harness.delegates.pg.serverSchema,
    ast(query).table,
    harness.delegates.pg.transaction,
    ast(query),
    query.format,
  );

  return {
    memory: zql,
    pg,
    sqlite: zqlite,
  };
}
