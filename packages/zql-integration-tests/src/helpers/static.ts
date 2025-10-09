import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {ZPGQuery} from '../../../zero-server/src/query.ts';
import {QueryImpl} from '../../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {type bootstrap} from './runner.ts';

export function staticToRunnable<TSchema extends Schema>({
  query,
  schema,
  harness,
}: {
  query: AnyQuery;
  schema: TSchema;
  harness: Awaited<ReturnType<typeof bootstrap>>;
}) {
  const qi = asQueryInternals(query);
  // reconstruct the generated query
  // for zql, zqlite and pg
  const zql = new QueryImpl(
    harness.delegates.memory,
    schema,
    qi.ast.table,
    qi.ast,
    qi.format,
  );
  const zqlite = new QueryImpl(
    harness.delegates.sqlite,
    schema,
    qi.ast.table,
    qi.ast,
    qi.format,
  );
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
