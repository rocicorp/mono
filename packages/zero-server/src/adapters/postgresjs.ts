import type {JSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {
  DBConnection,
  DBTransaction,
  Row,
} from '../../../zql/src/mutate/custom.ts';
import postgres from 'postgres';
import {ZQLDatabase} from '../zql-database.ts';

export class PostgresJSConnection<T extends Record<string, unknown>>
  implements DBConnection<postgres.TransactionSql<T>>
{
  readonly #pg: postgres.Sql<T>;
  constructor(pg: postgres.Sql<T>) {
    this.#pg = pg;
  }

  transaction<TRet>(
    fn: (tx: DBTransaction<postgres.TransactionSql<T>>) => Promise<TRet>,
  ): Promise<TRet> {
    return this.#pg.begin(pgTx => fn(new Transaction(pgTx))) as Promise<TRet>;
  }
}

class Transaction<T extends Record<string, unknown>>
  implements DBTransaction<postgres.TransactionSql<T>>
{
  readonly wrappedTransaction: postgres.TransactionSql<T>;
  constructor(pgTx: postgres.TransactionSql<T>) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}

/**
 * Example usage:
 * ```ts
 * import postgres from 'postgres';
 * const processor = new PushProcessor(
 *   zeroPostgresJS(schema, postgres(process.env.ZERO_UPSTREAM_DB as string)),
 * );
 *
 * // within your custom mutators you can do:
 * const tx = tx.wrappedTransaction();
 * // to get ahold of the underlying postgres.js transaction
 * // and then drop down to raw SQL if needed.
 * ```
 */
export function zeroPostgresJS<
  S extends Schema,
  T extends Record<string, unknown> = Record<string, unknown>,
>(schema: S, pg: postgres.Sql<T> | string) {
  if (typeof pg === 'string') {
    pg = postgres(pg) as postgres.Sql<T>;
  }
  return new ZQLDatabase(new PostgresJSConnection(pg), schema);
}
