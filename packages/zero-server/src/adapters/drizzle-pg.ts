import type {NodePgClient, NodePgDatabase} from 'drizzle-orm/node-postgres';
import type {
  DBConnection,
  DBTransaction,
} from '../../../zql/src/mutate/custom.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {ZQLDatabase} from '../zql-database.ts';

type DrizzleConnection<T extends Record<string, unknown>> =
  NodePgDatabase<T> & {
    $client: NodePgClient;
  };

export type DrizzleTransaction<T extends Record<string, unknown>> = Parameters<
  Parameters<DrizzleConnection<T>['transaction']>[0]
>[0];

class ZeroDrizzleConnection<
  T extends Record<string, unknown> = Record<string, unknown>,
> implements DBConnection<DrizzleTransaction<T>>
{
  readonly #client: DrizzleConnection<T>;
  constructor(client: DrizzleConnection<T>) {
    this.#client = client;
  }

  transaction<TRet>(
    cb: (tx: DBTransaction<DrizzleTransaction<T>>) => Promise<TRet>,
  ): Promise<TRet> {
    return this.#client.transaction(drizzleTx =>
      cb(new ZeroDrizzleTransaction(drizzleTx)),
    );
  }
}

class ZeroDrizzleTransaction<T extends Record<string, unknown>>
  implements DBTransaction<DrizzleTransaction<T>>
{
  readonly #tx: DrizzleTransaction<T>;
  constructor(tx: DrizzleTransaction<T>) {
    this.#tx = tx;
  }

  query(sql: string, params: unknown[]): Promise<Iterable<Row>> {
    const session = this.wrappedTransaction._.session as unknown as {
      client: DrizzleConnection<T>['$client'];
    };
    return session.client.query<Row>(sql, params).then(({rows}) => rows);
  }

  get wrappedTransaction() {
    return this.#tx;
  }
}

/**
 * Example usage:
 * ```ts
 * import {drizzle} from 'drizzle-orm/node-postgres';
 * const db = drizzle(process.env.PG_URL!);
 * const dbProvider = zeroNodePg(db);
 *
 * // within your custom mutators you can do:
 * const tx = tx.wrappedTransaction();
 * // to get ahold of the underlying drizzle client
 * ```
 */
export function zeroNodePg<
  S extends Schema,
  TDrizzleSchema extends Record<string, unknown> = Record<string, unknown>,
>(schema: S, client: DrizzleConnection<TDrizzleSchema>) {
  return new ZQLDatabase(new ZeroDrizzleConnection(client), schema);
}
