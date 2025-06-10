import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {ast} from '../../../zql/src/query/query-impl.ts';
import type {AnyStaticQuery} from '../../../zql/src/query/test/util.ts';
import {type bootstrap} from './runner.ts';
import {ZPGQuery} from '../../../zero-pg/src/query.ts';

export function staticToRunnable<TSchema extends Schema>({
  query,
  schema,
  harness,
}: {
  query: AnyStaticQuery;
  schema: TSchema;
  harness: Awaited<ReturnType<typeof bootstrap>>;
}) {
  // reconstruct the generated query
  // for zql, zqlite and pg
  const zql = query.asRunnable(harness.delegates.memory);
  const zqlite = query.asRunnable(harness.delegates.sqlite);
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
