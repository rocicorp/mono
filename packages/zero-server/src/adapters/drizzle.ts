import type {
  PgDatabase,
  PgQueryResultHKT,
  PgTransaction,
} from 'drizzle-orm/pg-core';
import type {ExtractTablesWithRelations} from 'drizzle-orm/relations';

export type DrizzleDatabase<
  TQueryResult extends PgQueryResultHKT = PgQueryResultHKT,
  TClient = unknown,
> = PgDatabase<TQueryResult, Record<string, unknown>> & {
  $client: TClient;
};

/**
 * Helper type for a wrapped transaction in Drizzle.
 */
export type DrizzleBaseTransaction<
  TQueryResult extends PgQueryResultHKT,
  TClient,
  TDbOrSchema extends
    | DrizzleDatabase<TQueryResult, TClient>
    | Record<string, unknown>,
  TSchema extends Record<string, unknown> = TDbOrSchema extends PgDatabase<
    TQueryResult,
    infer TSchema
  >
    ? TSchema
    : TDbOrSchema,
> = PgTransaction<TQueryResult, TSchema, ExtractTablesWithRelations<TSchema>>;
