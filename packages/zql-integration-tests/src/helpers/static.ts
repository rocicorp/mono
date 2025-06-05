import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {ast} from '../../../zql/src/query/query-impl.ts';
import type {AnyQuery} from '../../../zql/src/query/test/util.ts';
import {type bootstrap} from './runner.ts';
import {ZPGQuery} from '../../../zero-pg/src/query.ts';
import type {StaticQuery} from '../../../zql/src/query/static-query.ts';

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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const casted = query as StaticQuery<any, any, any>;
  const zql = casted.asRunnableQuery(harness.delegates.memory);
  const zqlite = casted.asRunnableQuery(harness.delegates.sqlite);
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
