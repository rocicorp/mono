import type {DBConnection, DBTransaction} from '../../zql/src/mutate/custom.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import type {Row} from '../../zql/src/mutate/custom.ts';

/**
 * Subset of the postgres lib's `Transaction` interface that we use.
 */
export type PostgresTransaction = {
  unsafe(sql: string, params: JSONValue[]): Promise<Row[]>;
};

/**
 * Subset of the postgres lib's `SQL` interface that we use.
 */
export type PostgresSQL<Transaction extends PostgresTransaction> = {
  unsafe(sql: string, params: JSONValue[]): Promise<Row[]>;
  begin<T>(fn: (tx: Transaction) => Promise<T>): Promise<T>;
};

/**
 * Adapts the `postgres` library to Zero's `DBConnection` interface.
 * Note: we do this via Go-style structural interface dependencies rather than
 * directly depending on the `postgres` library to eliminate chance of version
 * conflicts with customer.
 */
export class Connection<
  WrappedTransaction extends PostgresTransaction,
  WrappedPostgres extends PostgresSQL<WrappedTransaction>,
> implements DBConnection<WrappedTransaction>
{
  readonly #pg: WrappedPostgres;
  constructor(pg: WrappedPostgres) {
    this.#pg = pg;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.#pg.unsafe(sql, params as JSONValue[]);
  }

  transaction<T>(
    fn: (tx: DBTransaction<WrappedTransaction>) => Promise<T>,
  ): Promise<T> {
    return this.#pg.begin(pgTx => fn(new Transaction(pgTx))) as Promise<T>;
  }
}

class Transaction<WrappedTransaction extends PostgresTransaction>
  implements DBTransaction<WrappedTransaction>
{
  readonly wrappedTransaction: WrappedTransaction;
  constructor(pgTx: WrappedTransaction) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}

export function connectionProvider<
  WrappedTransaction extends PostgresTransaction,
  WrappedPostgres extends PostgresSQL<WrappedTransaction>,
>(pg: WrappedPostgres): () => Connection<WrappedTransaction, WrappedPostgres> {
  return () => new Connection(pg);
}
